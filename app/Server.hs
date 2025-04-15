{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# OPTIONS_GHC -Wall #-}

module Server
  ( runServer
  ) where

import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Reader
import Data.Attoparsec.ByteString qualified as A
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.Map.Strict qualified as Map
import Data.Text.Encoding qualified as TE
import Data.Word (Word8)
import Network.Socket.ByteString (sendAll)
import Pipes
import Pipes.Attoparsec qualified as A
import Pipes.Network.TCP
import Pipes.Prelude qualified as P
import TextShow

import Command
import RedisEnv
import RedisM
import RespParser
import RespType
import Network.Socket (getPeerName)

runServer :: MonadIO m => (Socket, SockAddr) -> Effect (RedisM m) ()
runServer (socket, addr) = do
  env <- ask
  void (A.parsed parseCommand (fromSocket socket bufferSize))
    >-> P.mapMaybe (\cmdArray -> (cmdArray,) <$> commandFromArray cmdArray)
    -- >-> P.map snd -- throw away the RespType array and only keep the commands
    >-> P.wither
      ( \(cmdArray, cmd) ->
          fmap ((cmdArray, cmd),) <$> runCommand (socket, addr) cmd
      )
    >-> P.tee
      ( P.map snd -- throw away the commands and only keep the results
          >-> P.map (TE.encodeUtf8 . showt)
          >-> toSocket socket
      )
    >-> P.tee
      ( P.filter (isPsyncCommand . snd . fst)
          >-> P.mapM_
            ( const $ do
                -- Note that this needs to happen after the response to the
                -- PSYNC command has been sent (in `toSocket` above).
                sendRdbDataToReplica socket
                saveReplicaConnection (socket, addr) env
            )
      )
    >-> P.filter (\((_cmdArray, cmd), _res) -> isReplicatedCommand cmd)
    >-> P.mapM_
      ( \((cmdArray, _cmd), _res) ->
          when env.isMasterNode $ propagateCommandToReplicas env cmdArray
      )
  where
    parseCommand = A.choice [psyncResponse, rdbData, array]

sendRdbDataToReplica :: MonadIO m => Socket -> m ()
sendRdbDataToReplica socket = do
  liftIO $ putStrLn "sending RDB data to replica"
  let rdb = BS8.pack ("$" <> show (length emptyRdb) <> crlf) <> BS.pack emptyRdb
  liftIO $ sendAll socket rdb

saveReplicaConnection :: MonadIO m => (Socket, SockAddr) -> RedisEnv -> m ()
saveReplicaConnection (socket, _addr) redisEnv = do
  liftIO $ do
    putStrLn $ "storing replica connection, socket: " <> show socket
    peerName <- getPeerName socket
    putStrLn $ "peerName: " <> show peerName
    -- TODO: Handle lost replica connections.
    let replicaInfo =
          ReplicaInfo
            { socket
            , lastKnownReplicaOffset = 0
            }
    atomically $ do
      replicas <- readTVar redisEnv.replicas
      writeTVar redisEnv.replicas $
        Map.insert peerName replicaInfo replicas

propagateCommandToReplicas :: MonadIO m => RedisEnv -> RespType -> m ()
propagateCommandToReplicas redisEnv cmdArray = do
  -- Increment `masterOffset`.
  liftIO . atomically $ do
    masterOffset <- readTVar redisEnv.masterOffset
    writeTVar redisEnv.masterOffset $ masterOffset + length (show cmdArray)

  liftIO . putStrLn $
    "incrementing masterOffset by: "
      <> show (length $ show cmdArray)
  masterOffset <- liftIO $ readTVarIO redisEnv.masterOffset
  liftIO . putStrLn $ "new masterOffset: " <> show masterOffset

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
