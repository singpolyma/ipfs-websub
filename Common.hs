module Common where

import Prelude ()
import BasicPrelude
import Control.Concurrent (forkIO)
import System.Environment (lookupEnv)
import Data.Time (getCurrentTime)
import Control.Concurrent.STM (atomically, TQueue, TVar, readTVar, modifyTVar', retry, readTQueue, writeTQueue)
import Network.URI (URI(..))
import Control.Error (syncIO, exceptT, ExceptT)
import UnexceptionalIO (Unexceptional, UIO, runUIO, liftUIO)
import qualified UnexceptionalIO as UIO
import qualified Data.Aeson as Aeson
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

logger :: (Unexceptional m, Monad m) => TQueue Text -> m ()
logger logthese = forever $ printExceptions $ syncIO $ do
	logthis <- atomically $ readTQueue logthese
	putStrLn logthis

printExceptions :: (Show e, Unexceptional m, Monad m) => ExceptT e m () -> m ()
printExceptions = exceptT (ignoreExceptions . print) return

ignoreExceptions :: (Unexceptional m) => IO () -> m ()
ignoreExceptions = liftUIO . void . UIO.fromIO

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

safeFork :: (MonadIO m) => UIO () -> m ()
safeFork = liftIO . void . forkIO . runUIO

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
