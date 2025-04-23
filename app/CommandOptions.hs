module CommandOptions
  ( SetOptions (..)
  , defaultSetOptions
  , setOptionsFromList
  , XReadOptions (..)
  , defaultXReadOptions
  , xReadOptionsFromList
  , isXReadUnblockedBy
  ) where

import Control.Arrow ((***))
import Control.Monad.Except (throwError)
import Data.Attoparsec.ByteString (parseOnly)
import Data.Either (rights)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE

import RespType
import Stream
import Util

newtype SetOptions = SetOptions
  { px :: Maybe Int
  }
  deriving (Show)

defaultSetOptions :: SetOptions
defaultSetOptions = SetOptions {px = Nothing}

setOptionsFromList :: [RespType] -> SetOptions -> SetOptions
setOptionsFromList [] options = options
setOptionsFromList (BulkString o : os) options
  | o ~= "px"
  , BulkString pxVal : os' <- os =
      setOptionsFromList os' options {px = Just . read $ T.unpack pxVal}
setOptionsFromList _ options = options

data XReadOptions = XReadOptions
  { streamKeys :: [StreamKey]
  , streamIdBounds :: [StreamIdBound]
  , blockTimeout :: Maybe Int
  }
  deriving (Show)

defaultXReadOptions :: XReadOptions
defaultXReadOptions =
  XReadOptions
    { streamKeys = []
    , streamIdBounds = []
    , blockTimeout = Nothing
    }

xReadOptionsFromList
  :: [RespType]
  -> XReadOptions
  -> Either RespType XReadOptions
xReadOptionsFromList [] options = pure options
xReadOptionsFromList (BulkString o : os) options
  | o ~= "block"
  , BulkString blockTimeout : os' <- os =
      xReadOptionsFromList os' $
        options {blockTimeout = Just . read $ T.unpack blockTimeout}
  | o ~= "streams"
  , even (length os) =
      let (h1, h2) =
            getBulkStrings *** getBulkStrings $ splitAt (length os `div` 2) os
      in  pure
            options
              { streamKeys = h1
              , streamIdBounds =
                  rights $
                    map (parseOnly xReadStreamIdBoundParser . TE.encodeUtf8) h2
              }
  | o ~= "streams" =
      throwError $
        SimpleError
          "ERR Unbalanced 'xread' list of streams: \
          \for each stream key an ID or '$' must be specified."
  | otherwise = throwError $ SimpleError "invalid 'xread' options"
xReadOptionsFromList _ _ = throwError $ SimpleError "invalid 'xread' options"

-- | Checks if a given stream key and stream ID (for an "XADD" call) "unblocks"
-- a "XREAD" call.
isXReadUnblockedBy :: XReadOptions -> StreamKey -> StreamId -> Bool
isXReadUnblockedBy options streamKey streamId =
  any
    ( \(streamKey', streamIdBound) ->
        case streamIdBound of
          AnyEntry streamIdBound' ->
            streamKey == streamKey' && streamIdBound' < streamId
          -- This function is only called when new entries are added, so we can
          -- safely return `True` here.
          OnlyNewEntries -> True
    )
    $ options.streamKeys `zip` options.streamIdBounds
