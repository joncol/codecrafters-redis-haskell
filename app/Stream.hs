{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wall #-}

module Stream
  ( Stream (..)
  , StreamKey
  , StreamIdRequest (..)
  , StreamId (..)
  , streamIdRequestParser
  , xRangeStreamIdBoundParser
  , xReadStreamIdBoundParser
  , streamToArray
  ) where

import Control.Applicative ((<|>))
import Control.Monad (void)
import Data.Attoparsec.ByteString
import Data.Attoparsec.ByteString.Char8 (decimal)
import Data.Functor (($>))
import Data.Text (Text)
import Data.Word (Word64)
import TextShow

import RespType

data Stream = Stream
  { streamId :: StreamId
  , entries :: [(Text, Text)]
  }
  deriving (Show)

type StreamKey = Text

data StreamIdRequest = Explicit StreamId | TimePart Int | Implicit
  deriving (Eq, Show)
  deriving (TextShow) via FromStringShow StreamIdRequest

data StreamId = StreamId
  { timePart :: Int
  , sequenceNumber :: Word64
  }
  deriving (Eq, Ord, Bounded)
  deriving (TextShow) via FromStringShow StreamId

instance Show StreamId where
  show StreamId {..} = show timePart <> "-" <> show sequenceNumber

streamIdRequestParser :: Parser StreamIdRequest
streamIdRequestParser =
  do
    string "*" $> Implicit <|> do
      timePart <- decimal
      void $ string "-*"
      pure $ TimePart timePart
    <|> do
      timePart <- decimal
      void $ string "-"
      sequenceNumber <- decimal
      pure $ Explicit StreamId {..}

xRangeStreamIdBoundParser :: Word64 -> Parser StreamId
xRangeStreamIdBoundParser def =
  string "-" $> minBound
    <|> string "+" $> maxBound
    <|> do
      timePart <- decimal
      sequenceNumber <- option def $ do
        void $ string "-"
        decimal
      pure $ StreamId {..}

xReadStreamIdBoundParser :: Parser StreamId
xReadStreamIdBoundParser = do
  timePart <- decimal
  sequenceNumber <- option 0 $ do
    void $ string "-"
    decimal
  pure $ StreamId {..}

streamToArray :: Stream -> RespType
streamToArray str =
  Array
    [ BulkString (showt str.streamId)
    , entriesToArray str.entries
    ]
  where
    entriesToArray :: [(Text, Text)] -> RespType
    entriesToArray entries =
      Array $
        concatMap
          (\(k, v) -> [BulkString k, BulkString v])
          entries
