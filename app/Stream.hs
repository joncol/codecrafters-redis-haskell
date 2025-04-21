{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wall #-}

module Stream
  ( Stream (..)
  , StreamKey
  , StreamIdRequest (..)
  , StreamId (..)
  , streamIdRequestParser
  ) where

import Control.Applicative ((<|>))
import Control.Monad (void)
import Data.Attoparsec.ByteString
import Data.Attoparsec.ByteString.Char8 (decimal)
import Data.Functor (($>))
import Data.Text (Text)
import TextShow

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
  , sequenceNumber :: Int
  }
  deriving (Eq, Ord)
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
