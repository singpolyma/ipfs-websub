module Main (main) where

import Prelude ()
import BasicPrelude
import Control.Concurrent (forkIO)
import Control.Concurrent.STM (atomically, retry, TVar, newTVarIO, readTVar, modifyTVar')
import System.Environment (lookupEnv)
import Control.Error (hush, justZ)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Network.URI (parseURI)
import Network.BufferType (BufferType)
import Network.HTTP (simpleHTTP, mkRequest, Request, RequestMethod(GET), getResponseBody, urlEncode)
import qualified Data.ByteString.Lazy as LZ
import qualified Data.Aeson as Aeson
import qualified UnexceptionalIO as UIO
import Database.Redis as Redis
import qualified RedisURL

import Common

concurrencyLimit :: Int
concurrencyLimit = 1

newtype IPFSPath = IPFSPath Text deriving (Show)

instance Aeson.FromJSON IPFSPath where
	parseJSON = Aeson.withObject "Path" $ \v -> IPFSPath <$> v Aeson..: (s"Path")

data IPFSDiff = IPFSDiff [(Text, Bool)] deriving (Show)

instance Aeson.FromJSON IPFSDiff where
	parseJSON v = do
		o <- Aeson.parseJSON v
		changes <- o Aeson..: (s"Changes")
		fmap IPFSDiff $ forM changes $ \item -> do
			path <- item Aeson..: (s"Path")
			after <- item Aeson..: (s"After")
			return (path, after == Aeson.Null)

getRequest :: (BufferType a) => String -> Request a
getRequest urlString =
	case parseURI urlString of
		Nothing -> error ("getRequest: Not a valid URL - " ++ urlString)
		Just u  -> mkRequest GET u

getJSON :: (MonadIO m, Aeson.FromJSON a) => String -> m (Maybe a)
getJSON url = liftIO $ fmap (join . hush) $ UIO.syncIO $ fmap Aeson.decode $
	getResponseBody =<< simpleHTTP (getRequest url)

redisFromEnvOrDefault :: IO Redis.ConnectInfo
redisFromEnvOrDefault =
	join $ either fail return <$>
	maybe (Right Redis.defaultConnectInfo) RedisURL.parseConnectInfo <$>
	lookupEnv "REDIS_URL"

resolveOne :: TVar Int -> ByteString -> ByteString -> Redis.Redis ()
resolveOne limit ipns lastPath = do
	now <- realToFrac <$> liftIO getPOSIXTime
	resolvedIPNS <- getJSON ("http://127.0.0.1:5001/api/v0/name/resolve?r&arg=" ++ urlEncode ipnsS)
	forM_ resolvedIPNS $ \(IPFSPath currentPath) ->
		let lastPathT = decodeUtf8 lastPath in
		if lastPathT == currentPath then return () else do
			diff <- (getJSON $ "http://127.0.0.1:5001/api/v0/object/diff?arg=" ++
				urlEncode (textToString lastPathT) ++
				"&arg=" ++ urlEncode (textToString currentPath)) :: Redis (Maybe IPFSDiff)
			forM_ diff $ \(IPFSDiff diff) -> forM_ diff $ \(path, deleted) -> when (not deleted) $ do
				let fullIPNS = ipns ++ (encodeUtf8 $ s"/" ++ path)
				let fullIPFS = currentPath ++ s"/" ++ path
				void $ Redis.zremrangebyscore fullIPNS (-inf) now
				mcallbacks <- Redis.zrangebyscore fullIPNS (-inf) inf
				forM_ mcallbacks $
					Redis.lpush (encodeUtf8 $ s"pings_to_send") . map
						(LZ.toStrict . Aeson.encode . Ping fullIPFS . decodeUtf8)
			void $ Redis.hset (encodeUtf8 $ s"last_resolved_to") ipns (encodeUtf8 currentPath)
	liftIO $ atomically $ modifyTVar' limit (subtract 1)
	where
	ipnsS = textToString $ decodeUtf8 ipns

scanLastResolvedTo :: Redis.Connection -> TVar Int -> Redis.Cursor -> Redis.Redis ()
scanLastResolvedTo redis limit cursor = do
	(next, items) <- join $ either (fail . show) return <$>
		Redis.hscan (encodeUtf8 $ s"last_resolved_to") cursor
	liftIO $ forM_ items $ \(ipns, lastValue) -> do
		atomically $ do
			concurrency <- readTVar limit
			when (concurrency >= concurrencyLimit) retry
			modifyTVar' limit (+1)
		void $ forkIO $ Redis.runRedis redis $ resolveOne limit ipns lastValue
	if next == Redis.cursor0 then return () else
		scanLastResolvedTo redis limit next

main :: IO ()
main = do
	redis <- Redis.checkedConnect =<< redisFromEnvOrDefault
	limit <- newTVarIO 0
	Redis.runRedis redis $ scanLastResolvedTo redis limit Redis.cursor0
	atomically $ do
		concurrency <- readTVar limit
		when (concurrency > 0) retry
