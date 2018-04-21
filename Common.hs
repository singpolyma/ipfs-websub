module Common where

import Prelude ()
import BasicPrelude
import System.Environment (lookupEnv)
import Data.Time (getCurrentTime)
import Control.Concurrent.STM (atomically, TQueue, writeTQueue)
import Network.URI (URI(..), parseAbsoluteURI)
import Control.Error (justZ)
import qualified Data.Aeson as Aeson
import Database.Redis as Redis
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

path :: URI -> ByteString
path u = case url of
	""  -> encodeUtf8 $ s"/"
	_   -> encodeUtf8 $ fromString url
	where
	url = concat [uriPath u, uriQuery u, uriFragment u]

redisOrFail :: Redis.Redis (Either Redis.Reply a) -> Redis.Redis a
redisOrFail x = join $ either (fail . show) return <$> x

redisOrFail_ :: Redis.Redis (Either Redis.Reply a) -> Redis.Redis ()
redisOrFail_ x = join $ either (fail . show) (const $ return ()) <$> x

data Ping = Ping Int Text Text deriving (Show)

instance Aeson.FromJSON Ping where
	parseJSON = Aeson.withObject "Ping" $ \o ->
		Ping <$> o Aeson..: (s"ipfs") <*> o Aeson..: (s"callback") <*> o Aeson..: (s"errors")

instance Aeson.ToJSON Ping where
	toJSON (Ping ipfs callback errors) =
		Aeson.object [s"ipfs" Aeson..= ipfs, s"callback" Aeson..= callback, s"errors" Aeson..= errors]

	toEncoding (Ping ipfs callback errors) =
		Aeson.pairs (s"ipfs" Aeson..= ipfs ++ s"callback" Aeson..= callback ++ s"errors" Aeson..= errors)

redisFromEnvOrDefault :: IO Redis.ConnectInfo
redisFromEnvOrDefault =
	join $ either fail return <$>
	maybe (Right Redis.defaultConnectInfo) RedisURL.parseConnectInfo <$>
	lookupEnv "REDIS_URL"
