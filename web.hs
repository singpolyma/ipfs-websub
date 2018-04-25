import Prelude ()
import BasicPrelude
import Data.Word (Word16)
import Control.Error (runExceptT, exceptT, throwE, (??), readZ, syncIO)
import Network.URI (parseURIReference, URI(..), URIAuth(..))
import Data.Bool.HT (select)
import qualified Data.Text as T
import qualified Network.Wai.Handler.Warp as Warp
import qualified Network.Wai as Wai
import qualified Network.Wai.Parse as Wai
import qualified Network.Wai.Middleware.RequestLogger as Wai
import qualified Network.Wai.Util as Wai
import qualified Network.HTTP.Types as HTTP
import qualified Database.Redis as Redis

import qualified LazyCBOR
import Common

minLease :: Word16
minLease = 60*60*24

defaultLease :: Word16
defaultLease = 60*60*24*10

maxLease :: Word16
maxLease = 60*60*24*60

app :: Redis.Connection -> Wai.Application
app redis req = (>>=) $ exceptT (Wai.string HTTP.badRequest400 [] . (++"\n")) return $ do
	when (Wai.requestMethod req /= HTTP.methodPost) $
		throwE "Subscribe using POST with hub.callback, etc"
	(params, _) <- liftIO $ Wai.parseRequestBodyEx Wai.defaultParseRequestBodyOptions Wai.noStoreFileUploads req

	mode <- lookup (Wai.queryLookup "hub.mode" params)
		[(Just $ s"subscribe", ModeSubscribe), (Just $ s"unsubscribe", ModeUnsubscribe)] ??
		"Unknown hub.mode"
	topic <- (parseURIReference =<< textToString <$> Wai.queryLookup "hub.topic" params) ?? "hub.topic not a valid URI"
	callback <- (parseURIReference =<< textToString <$> Wai.queryLookup "hub.callback" params) ?? "hub.callback not a valid URI"

	when (uriScheme callback `notElem` ["http:", "https:"]) $ throwE "hub.callback must be HTTP(S)"
	when (uriQuery topic /= "") $ throwE "hub.topic with query string not supported"
	when (uriFragment topic /= "") $ throwE "hub.topic with fragment not supported"

	let topicHost = fromMaybe mempty $ T.toLower . fromString . uriRegName <$> uriAuthority topic
	ipns <- select (throwE "hub.topic must be /ipns/ or have hostname") [
			("/ipns/" `isPrefixOf` uriPath topic, return $ fromString $ uriPath topic),
			(topicHost /= mempty, return $ s"/ipns/" ++ topicHost ++ fromString (uriPath topic))
		]

	let lease = min maxLease $ max minLease $ fromMaybe defaultLease $
		readZ . textToString =<< Wai.queryLookup "hub.lease_seconds" params

	let secret = map LazyCBOR.byteString $ maybeToList $
		lookup (encodeUtf8 $ s"hub.secret") params

	redisResult <- liftIO $ runExceptT $ syncIO $ Redis.runRedis redis $ redisOrFail_ $
		Redis.lpush (encodeUtf8 $ s"to_verify") [
			builderToStrict $ concat $ [
				LazyCBOR.word16 $ fromIntegral $ fromEnum mode,
				LazyCBOR.text $ tshow topic,
				LazyCBOR.text $ tshow callback,
				LazyCBOR.text ipns,
				LazyCBOR.word16 lease
			] ++ secret
		]

	case redisResult of
		Left _ -> Wai.string HTTP.internalServerError500 [] "DB connection issue\n"
		Right () -> Wai.string HTTP.accepted202 [] "Request enqueued\n"

main :: IO ()
main = do
	redis <- Redis.checkedConnect =<< redisFromEnvOrDefault
	putStrLn $ s"Starting..."
	Warp.runEnv 3000 (Wai.logStdout $ app redis)
