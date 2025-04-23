{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# OPTIONS_GHC -Wall #-}

module Replica (initReplica) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Reader
import Data.Attoparsec.ByteString
import Data.Attoparsec.ByteString qualified as A
import Data.ByteString.Char8 qualified as BS8
import Data.IORef
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Network.Socket.ByteString (recv, sendAll)
import Pipes
import Pipes.Attoparsec qualified as A
import Pipes.Network.TCP hiding (recv)
import Pipes.Prelude qualified as P
import TextShow

import Command
import Options
import RedisM
import RespParser
import RespType

-- | This connects to the master node and does the proper handshake process.
--
-- It should only be called if we're a replica.
--
-- This function creates a client that loops forever, so it needs to run in its
-- own thread.
--
-- The reason for running forever is that the master node needs to be able to
-- propagate write commands to the replica at an unknown later time, so we need
-- to keep the socket open.
initReplica :: RedisEnv -> IO ()
initReplica redisEnv
  | Just master <- redisEnv.options.mReplicaOf
  , [masterHostName, masterPort] <- T.words master = do
      -- Open a socket to communicate with the master node.
      handshakeComplete <- newEmptyMVar

      -- Using `forkIO` here, since the socket should be long lived.
      void . forkIO . connect (T.unpack masterHostName) (T.unpack masterPort) $
        \(s, addr) -> do
          handshake s redisEnv.options.port

          putMVar handshakeComplete ()

          -- Set up the replica streaming pipeline.
          void $
            flip runReaderT redisEnv . runRedisM . runEffect $
              runReplica (s, addr)

          putStrLn "closing replica socket"

      -- Wait until handshake is complete.
      void $ takeMVar handshakeComplete
  | otherwise = error "invalid `--replicaof` option value"

handshake :: Socket -> ServiceName -> IO ()
handshake s listeningPort = do
  do
    sendCommand pingCmd
    expectResponse (SimpleString "PONG")

    sendCommand replConfCmd1
    expectResponse (SimpleString "OK")

    sendCommand replConfCmd2
    expectResponse (SimpleString "OK")

    -- We handle the response to the PSYNC call in the replica `streaming`
    -- pipeline.
    sendCommand psyncCmd
  where
    sendCommand = sendAll s . BS8.pack . show

    expectResponse :: RespType -> IO ()
    expectResponse expected = do
      respMsg <- recv s bufferSize
      -- TODO: Better error handling?
      guard $ parseOnly respType respMsg == Right expected

    pingCmd = Array [BulkString "PING"]

    replConfCmd1 =
      Array
        [ BulkString "REPLCONF"
        , BulkString "listening-port"
        , BulkString $ T.pack listeningPort
        ]

    replConfCmd2 =
      Array
        [ BulkString "REPLCONF"
        , BulkString "capa"
        , BulkString "psync2"
        ]

    psyncCmd =
      Array
        [ BulkString "PSYNC"
        , BulkString "?"
        , BulkString "-1"
        ]

runReplica
  :: MonadIO m
  => (Socket, SockAddr)
  -> Effect (RedisM RedisEnv m) ()
runReplica (s, addr) = do
  void (A.parsed parseCommand (fromSocket s bufferSize))
    >-> P.mapMaybe (\cmdArray -> (cmdArray,) <$> commandFromArray cmdArray)
    >-> P.tee
      ( P.map snd
          >-> P.wither (\cmd -> fmap (cmd,) <$> runOrQueueCommand (s, addr) cmd)
          >-> P.filter (isReplConfGetAckCommand . fst)
          >-> P.map snd
          >-> P.map (TE.encodeUtf8 . showt)
          >-> toSocket s
      )
    >-> P.mapM_
      ( \(cmdArray, _cmd) -> do
          -- Post-increment replica offset.
          env <- ask
          liftIO $ modifyIORef' env.replicaOffset (+ length (show cmdArray))
      )
  where
    parseCommand = A.choice [psyncResponse, rdbData, array]
