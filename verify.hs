import Prelude ()
import BasicPrelude
import System.IO (stdout, stderr, hSetBuffering, BufferMode(LineBuffering))
import Data.Word (Word16)
import Control.Concurrent.STM (atomically, TVar, newTVarIO, modifyTVar', TQueue, newTQueueIO)
import Control.Error (fmapL)
import Safe (toEnumMay)
import Network.URI (URI(..), parseAbsoluteURI)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Crypto.Random (getRandomBytes)
import qualified Data.Text as T
import qualified System.IO.Streams as Streams
import qualified Network.Http.Client as HTTP
import qualified Network.HTTP.Types as HTTP
import UnexceptionalIO (Unexceptional)
import qualified UnexceptionalIO as UIO
import qualified Database.Redis as Redis

import qualified LazyCBOR
import Common

concurrencyLimit :: Int
concurrencyLimit = 100

verifyWithSubscriber :: (Unexceptional m) => TQueue Text -> URI -> HubMode -> Text -> Word16 -> m Bool
verifyWithSubscriber logthese callbackUri mode topic lease = (either handleError return =<<) $ UIO.fromIO $ do
	logPrint logthese "verifyWithSubscriber" (topic, callbackUri)
	challenge <- getRandomBytes 50
	let callback = encodeUtf8 $ tshow $ callbackUri {
			uriQuery = textToString $ decodeUtf8 $
				HTTP.renderQuery True ((encodeUtf8 $ s"hub.challenge", Just challenge):query)
		}
	logPrint logthese "verifyWithSubscriber" (topic, callback)
	HTTP.get callback $ \response instream -> do
		challenge2 <- Streams.readExactly 50 instream
		logPrint logthese "verifyWithSubscriber" (challenge, challenge2, HTTP.getStatusCode response, topic, callbackUri)
		case HTTP.getStatusCode response of
			404 -> return False
			status | status >= 200 && status <= 299 -> return $ challenge == challenge2
			_ -> return False
	where
	handleError e = do
		printExceptions $ UIO.fromIO $ logPrint logthese "verifyWithSubscriber" e
		return False
	query = HTTP.parseQuery (encodeUtf8 $ fromString $ uriQuery callbackUri) ++ [
			(
				encodeUtf8 $ s"hub.mode",
				Just $ encodeUtf8 $ case mode of { ModeSubscribe -> s"subscribe"; ModeUnsubscribe -> s"unsubscribe"; }
			),
			(encodeUtf8 $ s"hub.topic", Just $ encodeUtf8 topic),
			(encodeUtf8 $ s"hub.lease_seconds", Just $ encodeUtf8 $ tshow lease)
		]

subscribeOne :: TQueue Text -> Text -> URI -> Text -> Text -> Word16 -> Maybe ByteString -> Redis.Redis ()
subscribeOne logthese callback callbackUri topic ipns lease msecret = do
	logPrint logthese "subscribeOne" (topic, callback, ipnsResolve)
	result <- liftIO $ fmap join $ (fmap.fmapL) show $ UIO.fromIO $ HTTP.get ipnsResolve jsonHandlerSafe
	logPrint logthese "subscribeOne::resolved" (result, topic, callback)
	mpath <- liftIO $ case result of
		Left _ -> (either (const $ return Nothing) (const $ return Nothing) =<<) $
			UIO.fromIO $ HTTP.get denyCallback (const $ const $ return ())
		Right (IPFSPath path) -> do
			verified <- verifyWithSubscriber logthese callbackUri ModeSubscribe topic lease
			if verified then return (Just path) else return Nothing
	logPrint logthese "subscribeOne::mpath" (mpath, topic, callback)
	now <- liftIO $ realToFrac <$> liftIO getPOSIXTime
	forM_ mpath $ \path -> do
		redisOrFail_ $ Redis.zadd (encodeUtf8 ipns)
			[(now + fromIntegral lease, encodeUtf8 callback)]
		forM_ msecret $ \secret -> redisOrFail_ $
			Redis.setOpts (builderToStrict $ concat $ map LazyCBOR.text [s"secret", callback, ipns]) secret
				(Redis.SetOpts (Just $ fromIntegral lease) Nothing Nothing)
		exists <- redisOrFail $ Redis.hexists (encodeUtf8 $ s"last_resolved_to") ipnsRoot
		when (not exists) $ redisOrFail_ $
			Redis.hset (encodeUtf8 $ s"last_resolved_to") ipnsRoot (encodeUtf8 path)
	where
	ipnsResolve =
		encodeUtf8 (s"http://127.0.0.1:5001/api/v0/name/resolve?r&arg=") ++
		HTTP.urlEncode False ipnsRoot
	ipnsRoot = encodeUtf8 $ case T.breakOnAll (s"/") ipns of
		(_:_:(x, _):_) -> x
		_ -> mempty
	denyCallback = encodeUtf8 $ tshow $ callbackUri {
			uriQuery = textToString $ decodeUtf8 $ HTTP.renderQuery True denyQuery
		}
	denyQuery = HTTP.parseQuery (encodeUtf8 $ fromString $ uriQuery callbackUri) ++ [
			(encodeUtf8 $ s"hub.mode", Just $ encodeUtf8 $ s"denied"),
			(encodeUtf8 $ s"hub.topic", Just $ encodeUtf8 topic),
			(encodeUtf8 $ s"hub.reason", Just $ encodeUtf8 $ s"Not a valid IPNS name.")
		]

unsubscribeOne :: TQueue Text -> Text -> URI -> Text -> Text -> Redis.Redis ()
unsubscribeOne logthese callback callbackUri topic ipns = do
	verified <- liftIO $ verifyWithSubscriber logthese callbackUri ModeUnsubscribe topic 0
	when verified $ redisOrFail_ $
		Redis.zrem (encodeUtf8 ipns) [encodeUtf8 callback]

maybeSecret :: [LazyCBOR.Chunk] -> Maybe ByteString
maybeSecret (LazyCBOR.ByteStringChunk secret : _) = Just secret
maybeSecret _ = Nothing

startVerify :: (MonadIO m) => Redis.Connection -> TQueue Text -> TVar Int -> ByteString -> m ()
startVerify redis logthese limit rawverify
	| (
	  LazyCBOR.Word16Chunk imode  :
	  LazyCBOR.TextChunk topic    :
	  LazyCBOR.TextChunk callback :
	  LazyCBOR.TextChunk ipns     :
	  LazyCBOR.Word16Chunk lease  :
	  secret
	  ) <- LazyCBOR.parse rawverify,
	  Just mode <- toEnumMay $ fromIntegral imode,
	  Just callbackUri <- parseAbsoluteURI $ textToString callback = liftIO $ do
		logPrint logthese "startverify" (mode, topic, callback)
		concurrencyUpOne concurrencyLimit limit
		logPrint logthese "startVerify::forking" (mode, topic, callback)
		void $ UIO.fork $ bailOnExceptions $ runRedis redis $ do
			logPrint logthese "startVerify::forked" (mode, topic, callback)

			case mode of
				ModeSubscribe -> subscribeOne logthese callback callbackUri topic ipns lease (maybeSecret secret)
				ModeUnsubscribe -> unsubscribeOne logthese callback callbackUri topic ipns

			logPrint logthese "startVerify::done" (mode, topic, callback)
			redisOrFail_ $ Redis.lrem (encodeUtf8 $ s"verifying") 1 rawverify
			liftIO $ atomically $ modifyTVar' limit (subtract 1)
	| otherwise =
		logPrint logthese "startVerify::CBOR parse failed" rawverify

main :: IO ()
main = do
	hSetBuffering stdout LineBuffering
	hSetBuffering stderr LineBuffering

	putStrLn $ s"Starting..."
	redis <- Redis.checkedConnect =<< redisFromEnvOrDefault
	limit <- newTVarIO 0
	logthese <- newTQueueIO
	void $ UIO.fork $ logger logthese
	Redis.runRedis redis $ do
		leftovers <- redisOrFail $ Redis.lrange (encodeUtf8 $ s"verifying") 0 (-1)
		liftIO $ forM_ leftovers $ startVerify redis logthese limit
		waitForThreads limit

		forever $ do
			mverify <- redisOrFail $
				Redis.brpoplpush (encodeUtf8 $ s"to_verify") (encodeUtf8 $ s"verifying") 0
			liftIO $ forM_ mverify $ startVerify redis logthese limit
