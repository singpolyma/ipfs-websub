module Common where

import Prelude ()
import BasicPrelude
import Control.Concurrent (forkFinally, myThreadId, throwTo)
import System.Environment (lookupEnv)
import System.Exit (die)
import Data.Time (getCurrentTime)
import Control.Concurrent.STM (atomically, TQueue, TVar, readTVar, modifyTVar', retry, readTQueue, writeTQueue)
import Network.URI (URI(..))
import UnexceptionalIO (Unexceptional)
import qualified UnexceptionalIO as UIO
import qualified Data.Aeson as Aeson
import qualified System.IO.Streams as Streams
import qualified System.IO.Streams.Attoparsec as Streams
import qualified Network.Http.Client as HTTP
import qualified Data.ByteString.Lazy as LZ
import qualified Data.ByteString.Builder as Builder
import qualified Database.Redis as Redis
import qualified RedisURL

s :: (IsString a) => String -> a
s = fromString

inf :: Double
inf = 1/0

(.:) :: (c -> d) -> (a -> b -> c) -> (a -> b -> d)
(.:) = (.) . (.)

logPrint :: (Show a, MonadIO m) => TQueue Text -> String -> a -> m ()
logPrint logthese tag x = liftIO $ do
	time <- getCurrentTime
	atomically $ writeTQueue logthese $
		tshow time ++ s" :: " ++ fromString tag ++ s" :: " ++ tshow x

logger :: (Unexceptional m) => TQueue Text -> m ()
logger logthese = forever $ printExceptions $ UIO.fromIO $ do
	logthis <- atomically $ readTQueue logthese
	putStrLn logthis

bailOnExceptions :: (Show e, Unexceptional m) => m (Either e ()) -> m ()
bailOnExceptions = (either (ignoreExceptions . die . show) return =<<)

printExceptions :: (Show e, Unexceptional m) => m (Either e ()) -> m ()
printExceptions = (either (ignoreExceptions . print) return =<<)

ignoreExceptions :: (Unexceptional m) => IO () -> m ()
ignoreExceptions = UIO.lift . void . UIO.fromIO

uriFullPath :: URI -> ByteString
uriFullPath u = case url of
	""  -> encodeUtf8 $ s"/"
	_   -> encodeUtf8 $ fromString url
	where
	url = concat [uriPath u, uriQuery u]

redisOrFail :: Redis.Redis (Either Redis.Reply a) -> Redis.Redis a
redisOrFail x = join $ either (fail . show) return <$> x

redisOrFail_ :: Redis.Redis (Either Redis.Reply a) -> Redis.Redis ()
redisOrFail_ x = join $ either (fail . show) (const $ return ()) <$> x

runRedis :: (Unexceptional m) => Redis.Connection -> Redis.Redis () -> m (Either UIO.SomeNonPseudoException ())
runRedis conn action = UIO.fromIO (Redis.runRedis conn action)

concurrencyUpOne :: (MonadIO m) => Int -> TVar Int -> m ()
concurrencyUpOne concurrencyLimit limit =
	liftIO $ atomically $ do
		concurrency <- readTVar limit
		when (concurrency >= concurrencyLimit) retry
		modifyTVar' limit (+1)

waitForThreads :: (MonadIO m) => TVar Int -> m ()
waitForThreads limit =
	liftIO $ atomically $ do
		concurrency <- readTVar limit
		when (concurrency > 0) retry

jsonHandlerSafe :: (Aeson.FromJSON a) => HTTP.Response -> Streams.InputStream ByteString -> IO (Either String a)
jsonHandlerSafe _ i = do
	r <- Aeson.fromJSON <$> Streams.parseFromStream Aeson.json' i
	case r of
		(Aeson.Success a) ->  return (Right a)
		(Aeson.Error str) ->  return (Left str)

builderToStrict :: Builder.Builder -> ByteString
builderToStrict = LZ.toStrict . Builder.toLazyByteString

data HubMode = ModeSubscribe | ModeUnsubscribe deriving (Show, Enum, Bounded)

newtype IPFSPath = IPFSPath Text deriving (Show)

instance Aeson.FromJSON IPFSPath where
	parseJSON = Aeson.withObject "Path" $ \v -> IPFSPath <$> v Aeson..: s"Path"

redisFromEnvOrDefault :: IO Redis.ConnectInfo
redisFromEnvOrDefault =
	join $ either fail return <$>
	maybe (Right Redis.defaultConnectInfo) RedisURL.parseConnectInfo <$>
	lookupEnv "REDIS_URL"
