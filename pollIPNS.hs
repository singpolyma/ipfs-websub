module Main (main) where

import Prelude ()
import BasicPrelude
import Control.Concurrent (throwTo, myThreadId)
import Control.Concurrent.STM (atomically, retry, TVar, newTVarIO, readTVar, modifyTVar')
import Control.Error (hush, exceptT, syncIO)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Network.URI (parseURI)
import Network.BufferType (BufferType)
import Network.HTTP (simpleHTTP, mkRequest, Request, RequestMethod(GET), getResponseBody, urlEncode)
import qualified Data.Aeson as Aeson
import qualified UnexceptionalIO as UIO
import qualified Database.Redis as Redis

import qualified LazyCBOR
import Common

concurrencyLimit :: Int
concurrencyLimit = 1

data IPFSDiff = IPFSDiff [(Text, Bool)] deriving (Show)

instance Aeson.FromJSON IPFSDiff where
	parseJSON v = do
		o <- Aeson.parseJSON v
		changes <- o Aeson..: s"Changes"
		fmap IPFSDiff $ forM changes $ \item -> do
			path <- item Aeson..: s"Path"
			after <- item Aeson..: s"After"
			return (path, after == Aeson.Null)

getRequest :: (BufferType a) => String -> Request a
getRequest urlString =
	case parseURI urlString of
		Nothing -> error ("getRequest: Not a valid URL - " ++ urlString)
		Just u  -> mkRequest GET u

getJSON :: (MonadIO m, Aeson.FromJSON a) => String -> m (Maybe a)
getJSON url = liftIO $ fmap (join . hush) $ UIO.syncIO $ fmap Aeson.decode $
	getResponseBody =<< simpleHTTP (getRequest url)

resolveOne :: TVar Int -> ByteString -> ByteString -> Redis.Redis ()
resolveOne limit ipns lastPath = do
	now <- realToFrac <$> liftIO getPOSIXTime
	resolvedIPNS <- getJSON ("http://127.0.0.1:5001/api/v0/name/resolve?r&arg=" ++ urlEncode ipnsS)
	forM_ resolvedIPNS $ \(IPFSPath currentPath) ->
		let lastPathT = decodeUtf8 lastPath in
		if lastPathT == currentPath then return () else do
			diffr <- (getJSON $ "http://127.0.0.1:5001/api/v0/object/diff?arg=" ++
				urlEncode (textToString lastPathT) ++
				"&arg=" ++ urlEncode (textToString currentPath)) :: Redis.Redis (Maybe IPFSDiff)
			forM_ diffr $ \(IPFSDiff diff) -> forM_ diff $ \(path, deleted) -> when (not deleted) $ do
				let fullIPNS = ipns ++ encodeUtf8 (s"/" ++ path)
				let fullIPNScbor = LazyCBOR.text $ decodeUtf8 fullIPNS
				let fullIPFS = LazyCBOR.text $ currentPath ++ s"/" ++ path
				void $ Redis.zremrangebyscore fullIPNS (-inf) now
				mcallbacks <- Redis.zrangebyscore fullIPNS (-inf) inf
				forM_ mcallbacks $
					Redis.lpush (encodeUtf8 $ s"pings_to_send") . map (\callback ->
						builderToStrict $ concat [fullIPFS, LazyCBOR.text $ decodeUtf8 callback, fullIPNScbor]
					)
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
		mainThread <- myThreadId
		safeFork $ exceptT (ignoreExceptions . throwTo mainThread) return $ syncIO $ Redis.runRedis redis $
			resolveOne limit ipns lastValue
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
