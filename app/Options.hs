{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Options
  ( Options (..)
  , optionValueByName
  , parseOptions
  , rdbFilename
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import Network.Socket (ServiceName)
import System.Environment (getArgs)

data Options = Options
  { mDir :: Maybe FilePath
  , mDbFilename :: Maybe FilePath
  , port :: ServiceName
  , mReplicaOf :: Maybe Text
  }
  deriving (Show)

defaultOptions :: Options
defaultOptions =
  Options
    { mDir = Nothing
    , mDbFilename = Nothing
    , port = "6379"
    , mReplicaOf = Nothing
    }

parseOptions :: IO Options
parseOptions = do
  getArgs >>= pure . go defaultOptions
  where
    go :: Options -> [String] -> Options
    go options [] = options
    go options ("--dir" : dir : os) = go (options {mDir = Just dir}) os
    go options ("--dbfilename" : dbFilename : os) =
      go (options {mDbFilename = Just dbFilename}) os
    go options ("--port" : port : os) = go (options {port}) os
    go options ("--replicaof" : master : os) =
      go (options {mReplicaOf = Just (T.pack master)}) os
    go _ _ = error "error: invalid options"

optionValueByName :: Text -> Options -> Maybe Text
optionValueByName name options =
  case name of
    "dir" -> T.pack <$> options.mDir
    "dbfilename" -> T.pack <$> options.mDbFilename
    _ -> Nothing

rdbFilename :: Options -> Maybe FilePath
rdbFilename Options {..} =
  let dir' = maybe "./" (<> "/") mDir
  in  (dir' <>) <$> mDbFilename
