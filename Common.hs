module Common where

import Prelude ()
import BasicPrelude
import qualified Data.Aeson as Aeson

s :: (IsString a) => String -> a
s = fromString

inf :: Double
inf = 1/0

data Ping = Ping Text Text deriving (Show)

instance Aeson.FromJSON Ping where
	parseJSON = Aeson.withObject "Ping" $ \o ->
		Ping <$> o Aeson..: (s"ipfs") <*> o Aeson..: (s"callback")

instance Aeson.ToJSON Ping where
	toJSON (Ping ipfs callback) =
		Aeson.object [s"ipfs" Aeson..= ipfs, s"callback" Aeson..= callback]

	toEncoding (Ping ipfs callback) =
		Aeson.pairs (s"ipfs" Aeson..= ipfs ++ s"callback" Aeson..= callback)
