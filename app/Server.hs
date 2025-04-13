{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wall #-}

module Server
  ( runServer
  ) where

import Control.Arrow ((>>>))
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Reader
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.Function ((&))
import Data.Map.Strict qualified as Map
import Data.Word (Word8)
import Network.Socket (SockAddr, Socket)
import Network.Socket.ByteString (sendAll)

import Command
import RedisEnv
import RedisM
import RespType (RespType, crlf)

runServer :: (MonadIO m) => (Socket, SockAddr) -> RedisM m ()
runServer (socket, addr) = do
  pure ()

sendRdbDataToReplica :: (MonadIO m) => Socket -> m ()
sendRdbDataToReplica socket = do
  liftIO $ putStrLn "sending RDB data to replica"
  let rdbData =
        BS8.pack ("$" <> show (length emptyRdb) <> crlf) <> BS.pack emptyRdb
  liftIO $ sendAll socket rdbData

saveReplicaConnection :: (MonadIO m) => (Socket, SockAddr) -> RedisEnv -> m ()
saveReplicaConnection (socket, addr) redisEnv = do
  liftIO $ do
    putStrLn $ "storing replica connection, socket: " <> show socket
    -- TODO: Handle lost replica connections.
    let replicaInfo =
          ReplicaInfo
            { socket
            , lastKnownReplicaOffset = 0
            }
    atomically $ do
      replicas <- readTVar redisEnv.replicas
      writeTVar redisEnv.replicas $
        Map.insert addr replicaInfo replicas

propagateCommandToReplicas :: (MonadIO m) => RedisEnv -> RespType -> m ()
propagateCommandToReplicas redisEnv cmdArray = do
  -- Increase `masterOffset`.
  liftIO . atomically $ do
    masterOffset <- readTVar redisEnv.masterOffset
    writeTVar redisEnv.masterOffset $
      masterOffset + length (show cmdArray)
  liftIO . putStrLn $ "increasing masterOffset by: " <> show (length $ show cmdArray)
  mo <- liftIO $ readTVarIO redisEnv.masterOffset
  liftIO . putStrLn $ "new masterOffset: " <> show mo

  replicas <- liftIO $ readTVarIO redisEnv.replicas
  forM_ replicas $ \replica -> liftIO $ do
    putStr "sending replicated command ("
    print $ show cmdArray
    putStrLn $ ") to socket: " <> show replica.socket
    sendAll replica.socket . BS8.pack $ show cmdArray

emptyRdb :: [Word8]
emptyRdb =
  [ 0x52
  , 0x45
  , 0x44
  , 0x49
  , 0x53
  , 0x30
  , 0x30
  , 0x31
  , 0x31
  , 0xfa
  , 0x09
  , 0x72
  , 0x65
  , 0x64
  , 0x69
  , 0x73
  , 0x2d
  , 0x76
  , 0x65
  , 0x72
  , 0x05
  , 0x37
  , 0x2e
  , 0x32
  , 0x2e
  , 0x30
  , 0xfa
  , 0x0a
  , 0x72
  , 0x65
  , 0x64
  , 0x69
  , 0x73
  , 0x2d
  , 0x62
  , 0x69
  , 0x74
  , 0x73
  , 0xc0
  , 0x40
  , 0xfa
  , 0x05
  , 0x63
  , 0x74
  , 0x69
  , 0x6d
  , 0x65
  , 0xc2
  , 0x6d
  , 0x08
  , 0xbc
  , 0x65
  , 0xfa
  , 0x08
  , 0x75
  , 0x73
  , 0x65
  , 0x64
  , 0x2d
  , 0x6d
  , 0x65
  , 0x6d
  , 0xc2
  , 0xb0
  , 0xc4
  , 0x10
  , 0x00
  , 0xfa
  , 0x08
  , 0x61
  , 0x6f
  , 0x66
  , 0x2d
  , 0x62
  , 0x61
  , 0x73
  , 0x65
  , 0xc0
  , 0x00
  , 0xff
  , 0xf0
  , 0x6e
  , 0x3b
  , 0xfe
  , 0xc0
  , 0xff
  , 0x5a
  , 0xa2
  ]
