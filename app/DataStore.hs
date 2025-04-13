module DataStore
  ( DataStore
  , Value (..)
  ) where

import Data.Map.Strict (Map)
import Data.Text (Text)
import Data.Time (UTCTime)

type DataStore = Map Text Value

data Value = Value
  { value :: Text
  , mExpirationTime :: Maybe UTCTime
  }
  deriving (Show)
