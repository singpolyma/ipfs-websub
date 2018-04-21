module LazyCBOR (parse, Chunk(..), word16, text, byteString) where

-- Technically CBOR, but super lazy
-- Use a real library if you can get one easily
-- Some functions are partial because of laziness, but none have to be
-- Usage: parse $ LZ.toStrict $ Builder.toLazyByteString $ text (fromString "hello) ++ word16 1234

import Prelude ()
import BasicPrelude
import Data.Word (Word16)
import Data.Bits (shiftL, (.|.))
import Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString as B

data Chunk =
	Word16Chunk Word16 |
	TextChunk Text |
	ByteStringChunk ByteString
	deriving (Show)

parse :: ByteString -> [Chunk]
parse = unfoldr getCborChunk

getCborChunk :: ByteString -> Maybe (Chunk, ByteString)
getCborChunk bytes
	| mark == 0x19 = Just (Word16Chunk len, afterlen)
	| mark == 0x79 = Just (TextChunk $ decodeUtf8 str, afterstr)
	| mark == 0x59 = Just (ByteStringChunk str, afterstr)
	| otherwise = Nothing
	where
	(str, afterstr) = B.splitAt (fromIntegral len) afterlen
	len
		| B.length lenbytes == 2 =
			(fromIntegral (B.index lenbytes 0) `shiftL` 8) .|. (fromIntegral $ B.index lenbytes 1)
		| otherwise = 0
	(lenbytes, afterlen) = B.splitAt 2 aftermark
	(mark, aftermark) = fromMaybe (0, mempty) $ B.uncons bytes

word16 :: Word16 -> Builder
word16 word = Builder.word8 0x19 ++ Builder.word16BE word

text :: Text -> Builder
text = cborByteString' 0x79 . encodeUtf8

byteString :: ByteString -> Builder
byteString = cborByteString' 0x59

cborByteString' :: Word8 -> ByteString -> Builder
cborByteString' tag bytes
	| B.length bytes > 65535 =
		error $ "LazyCBOR cannot encode bytes of length " ++ show (B.length bytes)
	| otherwise = concat [
		Builder.word8 tag,
		Builder.word16BE (fromIntegral $ B.length bytes),
		Builder.byteString bytes
	]
