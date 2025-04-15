module Stream
  ( Stream (..)
  , StreamKey
  , StreamId
  ) where

import Data.Text (Text)

data Stream = Stream
  { streamId :: StreamId
  , entries :: [(Text, Text)]
  }
  deriving (Show)

type StreamKey = Text

type StreamId = Text
