{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NegativeLiterals #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module RespParser
  ( respType
  , array
  , psyncResponse
  , rdbData
  , replConfAck
  ) where

import Control.Applicative ((<|>))
import Control.Monad (guard, void)
import Data.Attoparsec.ByteString
import Data.Attoparsec.ByteString.Char8 (decimal, endOfLine, space)
import Data.ByteString qualified as BS
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Safe (headMay)
import Text.Read (readMaybe)

import RespType hiding (crlf)

respType :: Parser RespType
respType = choice [simpleString, simpleError, integer, bulkString, array]

simpleString :: Parser RespType
simpleString = do
  void $ string "+"
  bs <- BS.pack <$> manyTill' anyWord8 endOfLine
  pure . SimpleString $ TE.decodeASCII bs

simpleError :: Parser RespType
simpleError = do
  void $ string "-"
  bs <- BS.pack <$> manyTill' anyWord8 endOfLine
  pure . SimpleError $ TE.decodeLatin1 bs

integer :: Parser RespType
integer = do
  void $ string ":"
  sign <- option 1 (string "+" *> pure 1 <|> string "-" *> pure -1)
  n <- decimal
  endOfLine
  pure . Integer $ sign * n

bulkString :: Parser RespType
bulkString = do
  void $ string "$"
  len <- decimal
  endOfLine
  bs <- BS.pack <$> count len anyWord8
  endOfLine
  pure . BulkString $ TE.decodeLatin1 bs

array :: Parser RespType
array = do
  void $ string "*"
  len <- decimal
  endOfLine
  a <- count len respType
  pure $ Array a

-- | Parser for the response to the PSYNC call made during the handshake.
psyncResponse :: Parser RespType
psyncResponse = do
  void $ string "+FULLRESYNC "
  _replId <- BS.pack <$> manyTill' anyWord8 space
  _n :: Int <- decimal
  endOfLine
  pure PsyncResponse

-- | Parser for the data sent from the master node to the replica during the
-- handshake.
rdbData :: Parser RespType
rdbData = do
  void $ string "$"
  len <- decimal
  endOfLine
  bs <- BS.pack <$> count len anyWord8
  pure . RdbData $ TE.decodeLatin1 bs

replConfAck :: Parser RespType
replConfAck = do
  -- TODO: This should be possible to write in a cleaner way.
  Array l <- array
  guard $ length l == 3
  guard $ headMay l == Just (BulkString "REPLCONF")
  guard $ l !! 1 == BulkString "ACK"
  let mn :: Maybe Int = case l !! 2 of
        BulkString nStr -> readMaybe $ T.unpack nStr
        _ -> Nothing
  case mn of
    Just n -> pure $ ReplConfAck n
    Nothing -> fail "cannot parse integer in REPLCONF ACK"
