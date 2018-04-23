import Prelude ()
import BasicPrelude
import Data.Word (Word16)
import Control.Concurrent (forkIO)
import Control.Concurrent.STM (atomically, retry, TVar, newTVarIO, readTVar, modifyTVar', TQueue, newTQueueIO, readTQueue, writeTQueue)
import System.Environment (lookupEnv)
import UnexceptionalIO (syncIO)
import Control.Error (hush, justZ)
import Network.URI (URI(..), parseAbsoluteURI)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Network.URI (parseURI)
import qualified System.IO.Streams as Streams
import qualified Network.Http.Client as HTTP
import qualified Data.ByteString.Lazy as LZ
import qualified UnexceptionalIO as UIO
import qualified Crypto.MAC.HMAC as HMAC
import qualified Crypto.Hash as HMAC
import qualified Crypto.Hash.Algorithms as HMAC
import qualified Database.Redis as Redis

import qualified LazyCBOR
import Common

concurrencyLimit :: Int
concurrencyLimit = 100

firePing :: TQueue Text -> Streams.InputStream ByteString -> Text -> Text -> Maybe ByteString -> IO Bool
firePing logthese instream ipfs callback msecret | Just uri <- parseAbsoluteURI (textToString callback) = do
	(stream, iomsig) <- case msecret of
		Just secret -> second (fmap $ Just . HMAC.hmacGetDigest . HMAC.finalize) <$>
			Streams.inputFoldM (return .: HMAC.update) (HMAC.initialize secret) instream
		Nothing -> return (instream, return Nothing)
	msig <- iomsig

	HTTP.withConnection (HTTP.establishConnection $ encodeUtf8 callback) $ \conn -> do
		let req = HTTP.buildRequest1 $ do
			HTTP.http HTTP.POST (path uri)
			HTTP.setHeader (encodeUtf8 $ s"Link")
				(encodeUtf8 $ s"<https://websub.ipfs.singpolyma.net/>; rel=\"hub\", <dweb:" ++ ipfs ++ s">; rel=\"self\"")
			case msig of
				Just sig -> HTTP.setHeader (encodeUtf8 $ s"X-Hub-Signature")
					(encodeUtf8 $ s"sha256=" ++ (tshow (sig :: HMAC.Digest HMAC.SHA256)))
				Nothing -> return ()
		HTTP.sendRequest conn req (HTTP.inputStreamBody stream)
		HTTP.receiveResponse conn $ \response _ ->
			case HTTP.getStatusCode response of
				410 -> return True -- Subscription deleted
				code | code >= 200 && code <= 299 -> return True
				_ -> return False
firePing logthese _ _ callback _ = logPrint logthese "firePing:nouri" callback >> return False

pingOne :: (MonadIO m) => TQueue Text -> Text -> Text -> Maybe ByteString -> m Bool
pingOne logthese ipfs callback secret = liftIO $ do
	logPrint logthese "pingOne" (ipfs, callback)
	result <- liftIO $ syncIO $ HTTP.get (encodeUtf8 $ s"http://127.0.0.1:8080" ++ ipfs) $ \response instream -> do
		logPrint logthese "pingOne:8080" (HTTP.getStatusCode response, ipfs, callback)
		case HTTP.getStatusCode response of
			404 -> HTTP.get (encodeUtf8 $ s"http://127.0.0.1:5001/api/v0/dag/get?arg=" ++ ipfs) $ \dagresponse daginstream -> do
				logPrint logthese "pingOne:5001" (HTTP.getStatusCode dagresponse, ipfs, callback)
				case HTTP.getStatusCode dagresponse of
					200 -> firePing logthese daginstream ipfs callback secret
					_ -> return True
			200 -> firePing logthese instream ipfs callback secret
			_ -> return False

	case result of
		Left e -> logPrint logthese "pingOne:fatal" (result, ipfs, callback) >> return False
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
		void $ forkIO $ Redis.runRedis redis $ do
			logPrint logthese "startPing::forked" (ipfs, callback)
			secret <- redisOrFail $ Redis.get $ builderToStrict $ concat $ map LazyCBOR.text [(s"secret"), callback, ipns]
			success <- pingOne logthese ipfs callback secret

			txresult <- Redis.multiExec $ do
				whenTx isretry $ void <$> Redis.zrem (encodeUtf8 $ s"pings_to_retry") [rawping]
				whenTx (not success) $ recordFailure ipfs callback ipns (errorCount+1)
				whenTx (not isretry) $ void <$> Redis.lrem (encodeUtf8 $ s"pinging") 1 rawping

			case txresult of
				Redis.TxSuccess () -> return ()
				Redis.TxAborted -> fail "startPing transaction aborted"
				Redis.TxError e -> fail $ "startPing :: " ++ e

			liftIO $ atomically $ modifyTVar' limit (subtract 1)
	| otherwise = do
		logPrint logthese "startPing::CBOR parse failed" rawping

main :: IO ()
main = do
	putStrLn $ s"Starting..."
	redis <- Redis.checkedConnect =<< redisFromEnvOrDefault
	limit <- newTVarIO 0
	logthese <- newTQueueIO
	liftIO $ void $ forkIO $ logger logthese
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
