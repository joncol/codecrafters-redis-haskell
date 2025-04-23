{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE NegativeLiterals #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# OPTIONS_GHC -Wall #-}

module RdbParser where

import Control.Monad (void)
import Control.Monad.Combinators
import Data.Bits
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as LBS
import Data.List (foldl')
import Data.List.NonEmpty qualified as NE
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Void (Void)
import Data.Word
import Text.Megaparsec
import Text.Megaparsec.Byte

import DataStore

type Parser = Parsec Void LBS.ByteString

data Timestamp
  = TimestampInSeconds Word32
  | TimestampInMilliseconds Word64
  deriving (Show)

timestampToNominalDiffTime :: Timestamp -> NominalDiffTime
timestampToNominalDiffTime (TimestampInSeconds s) =
  secondsToNominalDiffTime $ fromIntegral s
timestampToNominalDiffTime (TimestampInMilliseconds ms) =
  secondsToNominalDiffTime $ fromIntegral ms / 1000

data Rdb = Rdb
  { version :: Text
  , metadata :: [(Text, Text)]
  , databases :: [[(Text, Value)]]
  }
  deriving (Show)

rdb :: Parser Rdb
rdb = do
  version <- header
  metadata <- many metadataSubsection
  databases <- many databaseSubsection
  endOfFile
  pure Rdb {version, metadata, databases}

header :: Parser Text
header = do
  void $ string "REDIS"
  version <- BS.pack <$> count 4 digitChar
  pure $ TE.decodeLatin1 version

metadataSubsection :: Parser (Text, Text)
metadataSubsection = do
  void $ char 0xfa
  name <- stringEncodedValue
  val <- stringEncodedValue
  pure (name, val)

databaseSubsection :: Parser [(Text, Value)]
databaseSubsection = do
  void $ char 0xfe
  _idx <- sizeEncodedValue
  void $ char 0xfb
  totalKeyValueCount <- sizeEncodedValue
  _expiresSize <- sizeEncodedValue
  count totalKeyValueCount keyValue
  where
    keyValue = do
      mTimestamp :: Maybe Timestamp <- optional timestamp
      void $ char 0x00
      name <- stringEncodedValue
      value <- stringEncodedValue
      pure
        ( name
        , Value
            { value
            , mExpirationTime =
                posixSecondsToUTCTime
                  . timestampToNominalDiffTime
                  <$> mTimestamp
            }
        )

timestamp :: Parser Timestamp
timestamp = do
  timestampInMilliseconds <|> timestampInSeconds
  where
    timestampInMilliseconds = do
      void $ char 0xfc
      bytes <- count 8 anySingle
      pure . TimestampInMilliseconds $ bytesToIntegralLE bytes

    timestampInSeconds = do
      void $ char 0xfd
      bytes <- count 4 anySingle
      pure . TimestampInSeconds $ bytesToIntegralLE bytes

endOfFile :: Parser ()
endOfFile = do
  void $ char 0xff
  _checksumBytes <- count 8 anySingle
  void $ optional newline

sizeEncodedValue :: Parser Int
sizeEncodedValue = do
  firstByte <- anySingle
  case firstByte .&. 0xc0 of
    0 -> pure $ fromIntegral firstByte
    0x40 -> do
      nextByte <- anySingle
      pure $ bytesToIntegralBE [firstByte .&. complement 0xc0, nextByte]
    0x80 -> do
      bytes <- count 4 anySingle
      pure $ bytesToIntegralBE bytes
    0xc0 -> do
      case firstByte of
        0xc0 -> do
          fromIntegral <$> anySingle
        0xc1 ->
          do
            bytes <- count 2 anySingle
            pure $ bytesToIntegralLE bytes
        0xc2 ->
          do
            bytes <- count 4 anySingle
            pure $ bytesToIntegralLE bytes
        _ ->
          failure
            (Just . Tokens $ NE.singleton firstByte)
            (Set.singleton . Tokens $ NE.fromList [0xc0, 0xc1, 0xc2])
    _ -> pure -1

stringEncodedValue :: Parser Text
stringEncodedValue = do
  firstByte <- lookAhead anySingle
  len <- sizeEncodedValue
  if (firstByte .&. 0xc0) /= 0
    then pure . T.pack $ show len
    else do
      bytes <- count len anySingle
      pure $ TE.decodeLatin1 . BS.pack $ bytes

bytesToIntegralLE :: Integral a => [Word8] -> a
bytesToIntegralLE bs =
  fst $ foldl' (\(acc, m) d -> (acc + fromIntegral d * m, m * 256)) (0, 1) bs

bytesToIntegralBE :: Integral a => [Word8] -> a
bytesToIntegralBE bs =
  fst . foldl' (\(acc, m) d -> (acc + fromIntegral d * m, m * 256)) (0, 1) $
    reverse bs
