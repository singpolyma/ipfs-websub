import Prelude ()
import BasicPrelude
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
import qualified Data.Aeson as Aeson
import qualified UnexceptionalIO as UIO
import Database.Redis as Redis
import qualified RedisURL

import Common

concurrencyLimit :: Int
concurrencyLimit = 100

firePing :: TQueue Text -> Streams.InputStream ByteString -> Text -> Text -> IO Bool
firePing logthese instream ipfs callback | Just uri <- parseAbsoluteURI (textToString callback) =
	HTTP.withConnection (HTTP.establishConnection $ encodeUtf8 callback) $ \conn -> do
		let req = HTTP.buildRequest1 $ do
			HTTP.http HTTP.POST (path uri)
			HTTP.setHeader (encodeUtf8 $ s"Link")
				(encodeUtf8 $ s"<https://websub.ipfs.singpolyma.net/>; rel=\"hub\", <dweb:" ++ ipfs ++ s">; rel=\"self\"")
		HTTP.sendRequest conn req (HTTP.inputStreamBody instream)
		HTTP.receiveResponse conn $ \response _ ->
			case HTTP.getStatusCode response of
				410 -> return True -- Subscription deleted
				code | code >= 200 && code <= 299 -> return True
				_ -> return False
firePing logthese _ _ callback = logPrint logthese "firePing:nouri" callback >> return False

pingOne :: (MonadIO m) => TQueue Text -> Ping -> m Bool
pingOne logthese ping@(Ping ipfs callback _) = liftIO $ do
	logPrint logthese "pingOne" ping
	result <- liftIO $ syncIO $ HTTP.get (encodeUtf8 $ s"http://127.0.0.1:8080" ++ ipfs) $ \response instream -> do
		logPrint logthese "pingOne:8080" (HTTP.getStatusCode response, ping)
		case HTTP.getStatusCode response of
			404 -> HTTP.get (encodeUtf8 $ s"http://127.0.0.1:5001/api/v0/dag/get?arg=" ++ ipfs) $ \dagresponse daginstream -> do
				logPrint logthese "pingOne:5001" (HTTP.getStatusCode dagresponse, ping)
				case HTTP.getStatusCode dagresponse of
					200 -> firePing logthese daginstream ipfs callback
					_ -> return True
			200 -> firePing logthese instream ipfs callback
			_ -> return False

	case result of
		Left e -> logPrint logthese "pingOne:fatal" (result, ping) >> return False
		Right False -> logPrint logthese "pingOne:failed" (result, ping) >> return False
		Right True -> logPrint logthese "pingOne:success" ping >> return True

logger :: TQueue Text -> IO ()
logger logthese = forever $ do
	logthis <- atomically $ readTQueue logthese
	putStrLn logthis

concurrencyUpOne :: (MonadIO m) => TVar Int -> m ()
concurrencyUpOne limit =
	liftIO $ atomically $ do
		concurrency <- readTVar limit
		when (concurrency >= concurrencyLimit) retry
		modifyTVar' limit (+1)

waitForThreads :: (MonadIO m) => TVar Int -> m ()
waitForThreads limit =
	liftIO $ atomically $ do
		concurrency <- readTVar limit
		when (concurrency > 0) retry

recordFailure :: Ping -> Redis.RedisTx (Redis.Queued ())
recordFailure ping@(Ping _ _ errors) =
	if delay < 60*60*24 then do
		now <- realToFrac <$> liftIO getPOSIXTime
		void <$> Redis.zadd (encodeUtf8 $ s"pings_to_retry") [(now + delay, LZ.toStrict $ Aeson.encode ping)]
	else
		void <$> Redis.lpush (encodeUtf8 $ s"fatal") [LZ.toStrict $ Aeson.encode ping]
	where
	delay = (60 * 5) * (realToFrac errors ** 2)

whenTx :: Bool -> Redis.RedisTx (Redis.Queued ()) -> Redis.RedisTx (Redis.Queued ())
whenTx True tx = tx
whenTx False _ = return (return ())

startPing :: (MonadIO m) => Redis.Connection -> TQueue Text -> TVar Int -> Bool -> ByteString -> m ()
startPing redis logthese limit isretry rawping
	| Just (ping@(Ping ipfs callback errors)) <- Aeson.decodeStrict rawping = liftIO $ do
		logPrint logthese "startPing" ping
		concurrencyUpOne limit
		logPrint logthese "startPing::forking" ping
		void $ forkIO $ Redis.runRedis redis $ do
			logPrint logthese "startPing::forked" ping
			success <- pingOne logthese ping

			txresult <- Redis.multiExec $ do
				whenTx isretry $ void <$> Redis.zrem (encodeUtf8 $ s"pings_to_retry") [rawping]
				whenTx (not success) $ recordFailure (Ping ipfs callback (errors+1))
				whenTx (not isretry) $ void <$> Redis.lrem (encodeUtf8 $ s"pinging") 1 rawping

			case txresult of
				Redis.TxSuccess () -> return ()
				TxAborted -> fail "startPing transaction aborted"
				TxError e -> fail $ "startPing :: " ++ e

			liftIO $ atomically $ modifyTVar' limit (subtract 1)
	| otherwise = do
		logPrint logthese "startPing::json parse failed" rawping

main :: IO ()
main = do
	redis <- Redis.checkedConnect =<< redisFromEnvOrDefault
	limit <- newTVarIO 0
	logthese <- newTQueueIO
	liftIO $ void $ forkIO $ logger logthese
	Redis.runRedis redis $ do
		leftovers <- redisOrFail $ Redis.lrange (encodeUtf8 $ s"pinging") 0 (-1)
		liftIO $ forM_ leftovers $ startPing redis logthese limit False
		waitForThreads

		forever $ do
			now <- realToFrac <$> liftIO getPOSIXTime
			pings <- redisOrFail $ Redis.zrangebyscoreLimit (encodeUtf8 $ s"pings_to_retry") (-inf) now 0 100
			when (not $ null pings) $ do
				forM_ pings $ startPing redis logthese limit True
				waitForThreads

			mping <- redisOrFail $
				Redis.brpoplpush (encodeUtf8 $ s"pings_to_send") (encodeUtf8 $ s"pinging") (60*5)
			forM_ mping $ startPing redis logthese limit False
