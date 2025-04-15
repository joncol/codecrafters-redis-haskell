{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Command
  ( Command (..)
  , isPsyncCommand
  , isReplConfGetAckCommand
  , isReplicatedCommand
  , (~=)
  , commandFromArray
  , setOptionsFromArray
  , runCommand
  ) where

import Control.Applicative ((<|>))
import Control.Arrow ((>>>))
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Reader
import Data.ByteString.Char8 qualified as BS8
import Data.Function (on)
import Data.Functor ((<&>))
import Data.IORef
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromJust, fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock
import Network.Socket (SockAddr, Socket, getPeerName)
import Network.Socket.ByteString (sendAll)
import Text.Read (readMaybe)
import TextShow

import Options
import RedisEnv
import RedisM
import RespType
import Stream

data Command
  = Ping
  | Echo Text
  | Set Text Text SetOptions
  | Get Text
  | ConfigGet Text
  | Keys Text
  | Info Text
  | ReplConf Text Text
  | Psync Text Text
  | Wait Int Int
  | Type Text
  | XAdd StreamKey StreamId [(Text, Text)]
  deriving (Show)
  deriving (TextShow) via FromStringShow Command

isPsyncCommand :: Command -> Bool
isPsyncCommand (Psync _ _) = True
isPsyncCommand _ = False

isReplConfGetAckCommand :: Command -> Bool
isReplConfGetAckCommand (ReplConf key _) | key ~= "getack" = True
isReplConfGetAckCommand _ = False

isReplicatedCommand :: Command -> Bool
isReplicatedCommand Ping = False
isReplicatedCommand (Echo _) = False
isReplicatedCommand (Set {}) = True
isReplicatedCommand (Get _) = False
isReplicatedCommand (ConfigGet _) = False
isReplicatedCommand (Keys _) = False
isReplicatedCommand (Info _) = False
isReplicatedCommand (ReplConf _ _) = False
isReplicatedCommand (Psync _ _) = False
isReplicatedCommand (Wait _ _) = False
isReplicatedCommand (Type _) = False
isReplicatedCommand (XAdd {}) = True

newtype SetOptions = SetOptions
  { px :: Maybe Int
  }
  deriving (Show)

defaultSetOptions :: SetOptions
defaultSetOptions = SetOptions {px = Nothing}

(~=) :: Text -> Text -> Bool
(~=) = (==) `on` T.toCaseFold

commandFromArray :: RespType -> Maybe Command
commandFromArray (Array (BulkString cmd : args))
  | cmd ~= "ping" = Just Ping
  | cmd ~= "echo", [BulkString arg] <- args = Just $ Echo arg
  | cmd ~= "set"
  , BulkString key : BulkString val : options <- args =
      Just $ Set key val (setOptionsFromArray (Array options) defaultSetOptions)
  | cmd ~= "get", [BulkString key] <- args = Just $ Get key
  | cmd ~= "config"
  , [BulkString configCmd, BulkString configName] <- args
  , configCmd ~= "get" =
      Just $ ConfigGet configName
  | cmd ~= "keys", [BulkString pat] <- args = Just $ Keys pat
  | cmd ~= "info", [BulkString section] <- args = Just $ Info section
  | cmd ~= "replconf"
  , [BulkString key, BulkString val] <- args =
      Just $ ReplConf key val
  | cmd ~= "psync"
  , [BulkString replicationId, BulkString offset] <- args =
      Just $ Psync replicationId offset
  | cmd ~= "wait"
  , [BulkString numReplicas, BulkString timeout] <- args =
      Just $ Wait (read $ T.unpack numReplicas) (read $ T.unpack timeout)
  | cmd ~= "type", [BulkString key] <- args = Just $ Type key
  | cmd ~= "xadd"
  , BulkString key : BulkString streamId : keyVals <- args =
      Just $ XAdd key streamId (pairs $ getBulkStrings keyVals)
  | otherwise = Nothing
commandFromArray _ = Nothing

getBulkStrings :: [RespType] -> [Text]
getBulkStrings = foldMap getBulkString
  where
    getBulkString :: RespType -> [Text]
    getBulkString (BulkString s) = [s]
    getBulkString _ = []

pairs :: [a] -> [(a, a)]
pairs [] = []
pairs [_] = []
pairs (x : y : rest) = (x, y) : pairs rest

setOptionsFromArray :: RespType -> SetOptions -> SetOptions
setOptionsFromArray (Array []) setOptions = setOptions
setOptionsFromArray (Array (BulkString o : os)) setOptions
  | o ~= "px"
  , BulkString pxVal : os' <- os =
      setOptionsFromArray
        (Array os')
        setOptions {px = Just . read $ T.unpack pxVal}
setOptionsFromArray _ setOptions = setOptions

runCommand
  :: MonadIO m
  => (Socket, SockAddr)
  -> Command
  -> RedisM m (Maybe RespType)
runCommand (socket, addr) command = do
  liftIO . putStrLn $ "-> runCommand, command: " <> show command
  case command of
    Ping -> pure . Just $ SimpleString "PONG"
    Echo s -> pure . Just $ BulkString s
    Set key val options -> Just <$> runSetCommand key val options
    Get key -> do
      result <- Just <$> runGetCommand key
      liftIO $ putStrLn "<- runCommand"
      pure result
    ConfigGet configName -> Just <$> runConfigGetCommand configName
    Keys pat -> Just <$> runKeysCommand pat
    Info section -> Just <$> runInfoCommand section
    ReplConf key val -> runReplConfCommand (socket, addr) key val
    Psync replicationId offset ->
      Just <$> runPsyncCommand replicationId offset
    Wait numReplicas timeout ->
      Just <$> runWaitCommand numReplicas timeout
    Type key -> Just <$> runTypeCommand key
    XAdd key streamId keyVals -> Just <$> runXAddCommand key streamId keyVals

runSetCommand :: MonadIO m => Text -> Text -> SetOptions -> RedisM m RespType
runSetCommand key val options = do
  mExpirationTime <- case options.px of
    Just px -> do
      time <- liftIO getCurrentTime
      pure . Just $ addUTCTime (fromIntegral px / 1000.0) time
    Nothing -> pure Nothing
  dataStoreRef <- asks dataStore
  liftIO . atomicModifyIORef dataStoreRef $
    \dataStore ->
      ( Map.insert key (createValue val mExpirationTime) dataStore
      , ()
      )
  pure ok
  where
    createValue :: Text -> Maybe UTCTime -> Value
    createValue value mExpirationTime =
      Value
        { value
        , mExpirationTime
        }

runGetCommand :: MonadIO m => Text -> RedisM m RespType
runGetCommand key = do
  expired <- isKeyExpired key
  if expired
    then pure NullBulkString
    else getValue key

getValue :: MonadIO m => Text -> RedisM m RespType
getValue key = do
  dataStoreRef <- asks dataStore
  dataStore <- liftIO $ readIORef dataStoreRef
  case Map.lookup key dataStore of
    Just Value {value} -> do
      pure $ BulkString value
    Nothing -> pure NullBulkString

runConfigGetCommand :: MonadIO m => Text -> RedisM m RespType
runConfigGetCommand configName =
  do
    configVal <-
      asks
        ( maybe NullBulkString BulkString
            . (options >>> optionValueByName configName)
        )
    pure $ Array [BulkString configName, configVal]

runKeysCommand :: MonadIO m => Text -> RedisM m RespType
runKeysCommand _pat = do
  dataStoreRef <- asks dataStore
  dataStore <- liftIO $ readIORef dataStoreRef
  Array . catMaybes
    <$> mapM
      ( \key -> do
          expired <- isKeyExpired key
          if expired
            then pure Nothing
            else pure . Just $ BulkString key
      )
      (Map.keys dataStore)

isKeyExpired :: MonadIO m => Text -> RedisM m Bool
isKeyExpired key = do
  dataStoreRef <- asks dataStore
  dataStore <- liftIO $ readIORef dataStoreRef

  case Map.lookup key dataStore of
    Just Value {..} -> do
      case mExpirationTime of
        Just expirationTime -> do
          time <- liftIO getCurrentTime
          pure $ time >= expirationTime
        Nothing -> pure False
    Nothing -> pure False

runInfoCommand :: MonadIO m => Text -> RedisM m RespType
runInfoCommand section
  | section ~= "replication" = do
      options <- asks options
      replId <- asks $ fromMaybe "not available for replicas" . mReplicationId
      pure . BulkString $
        T.unlines
          [ "# Replication"
          , roleString options
          , "master_replid:" <> replId
          , "master_repl_offset:0"
          ]
  | otherwise = pure NullBulkString
  where
    roleString :: Options -> Text
    roleString options =
      case options.mReplicaOf of
        Nothing -> "role:master"
        _ -> "role:slave"

runReplConfCommand
  :: MonadIO m
  => (Socket, SockAddr)
  -> Text
  -> Text
  -> RedisM m (Maybe RespType)
runReplConfCommand (socket, addr) key val
  | key ~= "listening-port" = pure . Just $ ok
  | key ~= "capa" = pure . Just $ ok
  | key ~= "getack" && val == "*" = do
      env <- ask
      replOffset <- liftIO $ readIORef env.replicaOffset
      if env.options.sendAcks
        then
          pure . Just . Array $
            map
              BulkString
              [ "REPLCONF"
              , "ACK"
              , T.pack $ show replOffset
              ]
        else do
          liftIO $ putStrLn "Not sending ACK"
          pure Nothing
  | key ~= "ack" = do
      -- Update the last known replica offset.
      let replicaOffset :: Maybe Int = readMaybe $ T.unpack val
      liftIO . putStrLn $ "REPLCONF ACK received, val: " <> show replicaOffset
      env <- ask
      liftIO $ do
        peerName <- getPeerName socket
        putStrLn $ "peerName: " <> show peerName
        atomically $ do
          repls <- readTVar env.replicas
          writeTVar env.replicas $
            Map.adjust
              ( \replInfo ->
                  replInfo
                    { lastKnownReplicaOffset = fromJust replicaOffset
                    }
              )
              peerName
              repls

      -- Debug print.
      replicas <- liftIO $ readTVarIO env.replicas
      liftIO . putStrLn $
        "updated replica offsets: "
          <> show (map lastKnownReplicaOffset $ Map.elems replicas)

      liftIO . putStrLn $ "replicas: " <> show replicas

      pure Nothing
  | otherwise = error $ "unknown REPLCONF key: " <> show key

runPsyncCommand :: MonadIO m => Text -> Text -> RedisM m RespType
runPsyncCommand _replicationId _offset = do
  replId <- asks $ fromMaybe "not available for replicas" . mReplicationId
  pure . SimpleString $ T.unwords ["FULLRESYNC", replId, "0"]

runWaitCommand :: MonadIO m => Int -> Int -> RedisM m RespType
runWaitCommand numReplicas timeout = do
  liftIO $ putStrLn "-> runWaitCommand"

  env <- ask
  masterOffset <- liftIO $ readTVarIO env.masterOffset
  liftIO . putStrLn $ "masterOffset: " <> show masterOffset
  initialUpToDateCount <-
    liftIO . atomically $ caughtUpReplicaCount env masterOffset
  liftIO . putStrLn $ "initialUpToDateCount: " <> show initialUpToDateCount

  replicas <- liftIO $ readTVarIO env.replicas
  liftIO . putStrLn $
    "replica offsets: "
      <> show (map lastKnownReplicaOffset $ Map.elems replicas)

  if
    | masterOffset == 0 -> pure . Integer $ length replicas
    | numReplicas <= initialUpToDateCount -> do
        liftIO . putStrLn $
          "simple case, returning early, up-to-date-count: "
            <> show initialUpToDateCount
        pure $ Integer initialUpToDateCount
    | otherwise -> liftIO $ do
        let replConfGetAck = Array $ map BulkString ["REPLCONF", "GETACK", "*"]

        forM_ replicas $ \replica -> do
          putStr "sending REPLCONF GETACK ("
          print $ show replConfGetAck
          putStrLn $ ") to socket: " <> show replica.socket

          -- Send a REPLCONF GETACK to each replica. The response is handled
          -- in "Command.runReplConfCommand".
          sendAll replica.socket . BS8.pack $ show replConfGetAck

        -- See: https://gist.github.com/vdorr/cfc97e298d34d0a586012cdea0972e37.
        result <-
          registerDelay (timeout * 1000) >>= \timeouted -> do
            atomically $
              ( do
                  n <- caughtUpReplicaCount env masterOffset
                  check $ numReplicas <= n
                  pure $ Integer n
              )
                <|> Integer
                  <$> caughtUpReplicaCount env masterOffset
                  <* (readTVar timeouted >>= check)

        -- Increment `masterOffset` due to the REPLCONF GETACK just sent.
        liftIO . atomically $ do
          mo <- readTVar env.masterOffset
          writeTVar env.masterOffset $ mo + length (show replConfGetAck)
        liftIO . putStr $
          "incrementing masterOffset by: "
            <> show (length $ show replConfGetAck)
            <> ", due to command: "
        print $ show replConfGetAck
        mo <- liftIO $ readTVarIO env.masterOffset
        liftIO . putStrLn $ "new masterOffset: " <> show mo

        pure result
  where
    caughtUpReplicaCount :: RedisEnv -> Int -> STM Int
    caughtUpReplicaCount env masterOffset = do
      replicas <- readTVar env.replicas
      pure . Map.size $
        Map.filter
          (\r -> masterOffset <= r.lastKnownReplicaOffset)
          replicas

runTypeCommand :: MonadIO m => Text -> RedisM m RespType
runTypeCommand key = do
  env <- ask
  dataStore <- liftIO $ readIORef env.dataStore
  streams <- liftIO $ readIORef env.streams
  if
    | key `Map.member` dataStore -> pure $ SimpleString "string"
    | key `Map.member` streams -> pure $ SimpleString "stream"
    | otherwise -> pure $ SimpleString "none"

runXAddCommand
  :: MonadIO m
  => StreamKey
  -> StreamId
  -> [(Text, Text)]
  -> RedisM m RespType
runXAddCommand key streamId entries = do
  env <- ask
  let newStream = [Stream {streamId, entries}]
  liftIO . modifyIORef' env.streams $ Map.insertWith ins key newStream
  pure $ BulkString streamId
  where
    ins :: [Stream] -> [Stream] -> [Stream]
    ins newStream oldStream = oldStream <> newStream
