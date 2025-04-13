{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wall #-}

module RedisEnv
  ( RedisEnv (..)
  , initialEnv
  , ReplicaInfo (..)
  , DataStore
  , Value (..)
  ) where

import Control.Concurrent.STM.TVar
import Data.ByteString.Lazy qualified as LBS
import Data.IORef
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (isNothing)
import Data.Text (Text)
import Network.Socket (SockAddr, Socket)
import System.IO.Error
import Text.Megaparsec (parseMaybe)

import DataStore
import Options
import RdbParser qualified

data RedisEnv = RedisEnv
  { dataStore :: IORef DataStore
  , options :: Options
  , isMasterNode :: Bool
  , mReplicationId :: Maybe Text
  -- ^ Replication ID for master nodes.
  , replicas :: TVar (Map SockAddr ReplicaInfo)
  -- ^ If we're the master node, this keeps track of all connected replicas.
  -- TODO: Should we use something more STM-friendly like
  -- http://hackage.haskell.org/package/tskiplist instead?
  , replicaOffset :: IORef Int
  -- ^ Number of command bytes sent from the master node. Only used by replicas.
  , masterOffset :: TVar Int
  -- ^ Number of command bytes sent from the master node. Only used by masters.
  }

data ReplicaInfo = ReplicaInfo
  { socket :: Socket
  , lastKnownReplicaOffset :: Int
  }
  deriving (Show)

initialEnv :: IO RedisEnv
initialEnv = do
  options <- parseOptions
  dataStore <- createDataStore options
  let isMasterNode = isNothing options.mReplicaOf
  replicas <- newTVarIO Map.empty
  replicaOffset <- newIORef 0
  masterOffset <- newTVarIO 0
  pure
    RedisEnv
      { dataStore
      , options
      , isMasterNode
      , -- Only set replication ID for master nodes.
        mReplicationId =
          if isMasterNode
            then Just "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
            else Nothing
      , replicas
      , replicaOffset
      , masterOffset
      }
  where
    createDataStore :: Options -> IO (IORef DataStore)
    createDataStore options = do
      ds <- case rdbFilename options of
        Nothing -> pure Map.empty
        Just fn ->
          flip
            catchIOError
            ( \e ->
                if isDoesNotExistError e
                  then do
                    putStrLn "warning: RDB file does not exist"
                    pure Map.empty
                  else ioError e
            )
            $ do
              rdbContents <- LBS.readFile fn
              case parseMaybe RdbParser.rdb rdbContents of
                Just rdb -> pure . Map.fromList $ concat rdb.databases
                Nothing -> error "error: invalid RDB file"
      newIORef ds
