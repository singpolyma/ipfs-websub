import Prelude ()
import BasicPrelude
import Data.Typeable (Proxy(Proxy), typeOf, typeRep)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(LineBuffering))
import Data.Word (Word16)
import Control.Concurrent.STM (atomically, TVar, newTVarIO, modifyTVar', TQueue, newTQueueIO)
import UnexceptionalIO (syncIO)
import Network.URI (parseAbsoluteURI)
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified System.IO.Streams as Streams
import qualified System.IO.Streams.ByteString as Streams
import qualified Network.Http.Client as HTTP
import qualified Network.Http.Types as HTTP
import qualified Crypto.MAC.HMAC as HMAC
import qualified Crypto.Hash as HMAC
import qualified Database.Redis as Redis

import qualified LazyCBOR
import Common

concurrencyLimit :: Int
concurrencyLimit = 100

-- We can't do streaming when we need the HMAC
-- But need to protect against a giant payload taking our RAM
limitedStreamForce :: Streams.InputStream ByteString -> IO (Streams.InputStream ByteString)
limitedStreamForce stream = do
	stream' <- Streams.throwIfProducesMoreThan 100000 stream
	Streams.fromList =<< Streams.toList stream'

firePing :: TQueue Text -> HTTP.Response -> Streams.InputStream ByteString -> Text -> Text -> Maybe ByteString -> IO Bool
firePing logthese response instream ipfs callback msecret | Just uri <- parseAbsoluteURI (textToString callback) = do
	let mcontenttype = HTTP.lookupHeader (HTTP.getHeaders response) (encodeUtf8 $ s"Content-Type")
	(stream, iomsig) <- case msecret of
		Just secret -> do
			(stream, iomsig) <- second (fmap $ Just . HMAC.hmacGetDigest . HMAC.finalize) <$>
				Streams.inputFoldM (return .: HMAC.update) (HMAC.initialize secret) instream
			limitedStream <- limitedStreamForce stream
			return (limitedStream, iomsig)
		Nothing -> return (instream, return Nothing)
	msig <- iomsig

	HTTP.withConnection (HTTP.establishConnection $ encodeUtf8 callback) $ \conn -> do
		let req = HTTP.buildRequest1 $ do
			HTTP.http HTTP.POST (uriFullPath uri)
			HTTP.setHeader (encodeUtf8 $ s"Link")
				(encodeUtf8 $ s"<https://websub.ipfs.singpolyma.net/>; rel=\"hub\", <dweb:" ++ ipfs ++ s">; rel=\"self\"")
			forM_ mcontenttype HTTP.setContentType
			forM_ msig $ \sig ->
				HTTP.setHeader (encodeUtf8 $ s"X-Hub-Signature")
					(encodeUtf8 $ s"sha256=" ++ tshow (sig :: HMAC.Digest HMAC.SHA256))
		logPrint logthese "firePing:request" (req, ipfs, callback)
		HTTP.sendRequest conn req (HTTP.inputStreamBody stream)
		HTTP.receiveResponse conn $ \response _ -> do
			logPrint logthese "firePing:response" (response, ipfs, callback)
			case HTTP.getStatusCode response of
				410 -> return True -- Subscription deleted
				code | code >= 200 && code <= 299 -> return True
				_ -> return False
firePing logthese _ _ _ callback _ = logPrint logthese "firePing:nouri" callback >> return False

pingOne :: (MonadIO m) => TQueue Text -> Text -> Text -> Maybe ByteString -> m Bool
pingOne logthese ipfs callback secret = liftIO $ do
	logPrint logthese "pingOne" (ipfs, callback)
	result <- liftIO $ syncIO $ HTTP.get (encodeUtf8 $ s"http://127.0.0.1:8080" ++ ipfs) $ \response instream -> do
		logPrint logthese "pingOne:8080" (HTTP.getStatusCode response, ipfs, callback)
		case HTTP.getStatusCode response of
			404 -> HTTP.get (encodeUtf8 $ s"http://127.0.0.1:5001/api/v0/dag/get?arg=" ++ ipfs) $ \dagresponse daginstream -> do
				logPrint logthese "pingOne:5001" (HTTP.getStatusCode dagresponse, ipfs, callback)
				case HTTP.getStatusCode dagresponse of
					200 -> firePing logthese dagresponse daginstream ipfs callback secret
					_ -> return True
			200 -> firePing logthese response instream ipfs callback secret
			_ -> return False

	case result of
		Left e | typeOf e == typeRep (Proxy :: Proxy Streams.TooManyBytesReadException) ->
			logPrint logthese "pingOne:too big for HMAC" (ipfs, callback) >> return True
		Left _ -> logPrint logthese "pingOne:fatal" (result, ipfs, callback) >> return False
		Right False -> logPrint logthese "pingOne:failed" (result, ipfs, callback) >> return False
		Right True -> logPrint logthese "pingOne:success" (ipfs, callback) >> return True

recordFailure :: Text -> Text -> Text -> Word16 -> Redis.RedisTx (Redis.Queued ())
recordFailure ipfs callback ipns errorCount =
	if delay < 60*60*24 then do
		now <- realToFrac <$> liftIO getPOSIXTime
		void <$> Redis.zadd (encodeUtf8 $ s"pings_to_retry") [(now + delay, encoded)]
	else
		void <$> Redis.lpush (encodeUtf8 $ s"fatal") [encoded]
	where
	encoded = builderToStrict $ concat [
			LazyCBOR.text ipfs,
			LazyCBOR.text callback,
			LazyCBOR.text ipns,
			LazyCBOR.word16 errorCount
		]
	delay = (60 * 5) * (realToFrac errorCount ** 2)

whenTx :: Bool -> Redis.RedisTx (Redis.Queued ()) -> Redis.RedisTx (Redis.Queued ())
whenTx True tx = tx
whenTx False _ = return (return ())

getErrorCount :: (Integral i) => [LazyCBOR.Chunk] -> i
getErrorCount (LazyCBOR.Word16Chunk count : _) = fromIntegral count
getErrorCount _ = 0

startPing :: (MonadIO m) => Redis.Connection -> TQueue Text -> TVar Int -> Bool -> ByteString -> m ()
startPing redis logthese limit isretry rawping
	| (
	  LazyCBOR.TextChunk ipfs     :
	  LazyCBOR.TextChunk callback :
	  LazyCBOR.TextChunk ipns     :
	  merrorCount
	  ) <- LazyCBOR.parse rawping = liftIO $ do
		let errorCount = getErrorCount merrorCount
		logPrint logthese "startPing" (ipfs, callback)
		concurrencyUpOne concurrencyLimit limit
		logPrint logthese "startPing::forking" (ipfs, callback)
		linkFork $ Redis.runRedis redis $ do
			logPrint logthese "startPing::forked" (ipfs, callback)
			secret <- redisOrFail $ Redis.get $ builderToStrict $ concat $ map LazyCBOR.text [s"secret", callback, ipns]
			success <- pingOne logthese ipfs callback secret

			txresult <- Redis.multiExec $ do
				x <- whenTx isretry $ void <$> Redis.zrem (encodeUtf8 $ s"pings_to_retry") [rawping]
				y <- whenTx (not success) $ recordFailure ipfs callback ipns (errorCount+1)
				z <- whenTx (not isretry) $ void <$> Redis.lrem (encodeUtf8 $ s"pinging") 1 rawping
				return (x >> y >> z)

			case txresult of
				Redis.TxSuccess () -> return ()
				Redis.TxAborted -> fail "startPing transaction aborted"
				Redis.TxError e -> fail $ "startPing :: " ++ e

			liftIO $ atomically $ modifyTVar' limit (subtract 1)
	| otherwise =
		logPrint logthese "startPing::CBOR parse failed" rawping

main :: IO ()
main = do
	hSetBuffering stdout LineBuffering
	hSetBuffering stderr LineBuffering

	putStrLn $ s"Starting..."
	redis <- Redis.checkedConnect =<< redisFromEnvOrDefault
	limit <- newTVarIO 0
	logthese <- newTQueueIO
	linkFork $ logger logthese
	Redis.runRedis redis $ do
		leftovers <- redisOrFail $ Redis.lrange (encodeUtf8 $ s"pinging") 0 (-1)
		liftIO $ forM_ leftovers $ startPing redis logthese limit False
		waitForThreads limit

		forever $ do
			now <- realToFrac <$> liftIO getPOSIXTime
			pings <- redisOrFail $ Redis.zrangebyscoreLimit (encodeUtf8 $ s"pings_to_retry") (-inf) now 0 100
			when (not $ null pings) $ do
				forM_ pings $ startPing redis logthese limit True
				waitForThreads limit

			mping <- redisOrFail $
				Redis.brpoplpush (encodeUtf8 $ s"pings_to_send") (encodeUtf8 $ s"pinging") (60*5)
			forM_ mping $ startPing redis logthese limit False
