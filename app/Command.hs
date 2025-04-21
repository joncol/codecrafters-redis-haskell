{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wall #-}

module Command
  ( Command (..)
  , isPsyncCommand
  , isReplConfGetAckCommand
  , isReplicatedCommand
  , (~=)
  , commandFromArray
  , setOptionsFromList
  , runCommand
  ) where

import Control.Applicative ((<|>))
import Control.Arrow ((***), (>>>))
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Except (throwError)
import Control.Monad.Reader
import Data.Attoparsec.ByteString (parseOnly)
import Data.ByteString.Char8 qualified as BS8
import Data.Either (rights)
import Data.Foldable (toList)
import Data.Function (on)
import Data.IORef
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromJust, fromMaybe)
import Data.Sequence (Seq ((:|>)))
import Data.Sequence qualified as Seq
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time.Clock
import Data.Time.Clock.POSIX
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
  | XAdd StreamKey StreamIdRequest [(Text, Text)]
  | XRange StreamKey (StreamId, StreamId)
  | XRead (Either RespType XReadOptions)
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
isReplicatedCommand (XRange {}) = False
isReplicatedCommand (XRead _) = False

(~=) :: Text -> Text -> Bool
(~=) = (==) `on` T.toCaseFold

newtype SetOptions = SetOptions
  { px :: Maybe Int
  }
  deriving (Show)

defaultSetOptions :: SetOptions
defaultSetOptions = SetOptions {px = Nothing}

data XReadOptions = XReadOptions
  { streamKeys :: [StreamKey]
  , streamIdBounds :: [StreamId]
  }
  deriving (Show)

defaultXReadOptions :: XReadOptions
defaultXReadOptions =
  XReadOptions
    { streamKeys = []
    , streamIdBounds = []
    }

commandFromArray :: RespType -> Maybe Command
commandFromArray (Array (BulkString cmd : args))
  | cmd ~= "ping" = Just Ping
  | cmd ~= "echo", [BulkString arg] <- args = Just $ Echo arg
  | cmd ~= "set"
  , BulkString key : BulkString val : options <- args =
      Just $ Set key val (setOptionsFromList options defaultSetOptions)
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
  , BulkString key : BulkString streamIdStr : keyVals <- args =
      case parseOnly streamIdRequestParser $ TE.encodeUtf8 streamIdStr of
        Left err -> error $ "could not parse request stream ID: " <> err
        Right streamIdReq ->
          Just $ XAdd key streamIdReq (pairs $ getBulkStrings keyVals)
  | cmd ~= "xrange"
  , [BulkString key, BulkString start, BulkString end] <- args =
      case ( parseOnly (xRangeStreamIdBoundParser 0) $ TE.encodeUtf8 start
           , parseOnly (xRangeStreamIdBoundParser maxBound) $ TE.encodeUtf8 end
           ) of
        (Right start', Right end') -> Just $ XRange key (start', end')
        _ -> error "could not parse stream ID bounds"
  | cmd ~= "xread" =
      Just $ XRead (xReadOptionsFromList args defaultXReadOptions)
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

setOptionsFromList :: [RespType] -> SetOptions -> SetOptions
setOptionsFromList [] options = options
setOptionsFromList (BulkString o : os) options
  | o ~= "px"
  , BulkString pxVal : os' <- os =
      setOptionsFromList os' options {px = Just . read $ T.unpack pxVal}
setOptionsFromList _ options = options

xReadOptionsFromList
  :: [RespType]
  -> XReadOptions
  -> Either RespType XReadOptions
xReadOptionsFromList [] options = pure options
xReadOptionsFromList (BulkString o : os) _options
  | o ~= "streams"
  , even (length os) =
      let (h1, h2) =
            getBulkStrings *** getBulkStrings $ splitAt (length os `div` 2) os
      in  pure
            XReadOptions
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
    XAdd key streamIdReq keyVals ->
      Just <$> runXAddCommand key streamIdReq keyVals
    XRange key (start, end) ->
      Just <$> runXRangeCommand key (start, end)
    XRead mStreamParams -> case mStreamParams of
      Left err -> pure $ Just err
      Right streamParams -> Just <$> runXReadCommand streamParams

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
runReplConfCommand (socket, _addr) key val
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
  allStreams <- liftIO $ readIORef env.streams
  if
    | key `Map.member` dataStore -> pure $ SimpleString "string"
    | key `Map.member` allStreams -> pure $ SimpleString "stream"
    | otherwise -> pure $ SimpleString "none"

runXAddCommand
  :: MonadIO m
  => StreamKey
  -> StreamIdRequest
  -> [(Text, Text)]
  -> RedisM m RespType
runXAddCommand key streamIdReq entries = do
  env <- ask
  allStreams <- liftIO $ readIORef env.streams
  let mOldStream = Map.lookup key allStreams
  case streamIdReq of
    Explicit streamId@(StreamId {timePart, sequenceNumber}) ->
      if timePart == 0 && sequenceNumber == 0
        then pure idMustBeGreaterThanZeroError
        else case mOldStream of
          Just (_ :|> lastStream)
            | streamId <= lastStream.streamId -> pure idTooSmallError
          _ -> do
            let newStream =
                  Seq.singleton $
                    Stream
                      { streamId = streamId
                      , entries
                      }
            liftIO . modifyIORef' env.streams $ Map.insertWith ins key newStream
            pure . BulkString $ showt streamId
    TimePart timePart -> do
      addEntry env.streams mOldStream timePart
    Implicit -> do
      t <- liftIO getPOSIXTime
      let timePart = floor $ nominalDiffTimeToSeconds t * 1000
      addEntry env.streams mOldStream timePart
  where
    idMustBeGreaterThanZeroError =
      SimpleError "ERR The ID specified in XADD must be greater than 0-0"

    idTooSmallError =
      SimpleError
        "ERR The ID specified in XADD is equal or smaller than the \
        \target stream top item"

    addEntry
      :: MonadIO m
      => IORef (Map StreamKey (Seq Stream))
      -> Maybe (Seq Stream)
      -> Int
      -> RedisM m RespType
    addEntry streams mOldStream timePart = do
      let mOldStream' =
            Seq.filter (\str -> str.streamId.timePart == timePart)
              <$> mOldStream
      case mOldStream' of
        Just (_ :|> lastStream) -> do
          let streamId =
                StreamId
                  { timePart
                  , sequenceNumber = lastStream.streamId.sequenceNumber + 1
                  }
          let newStream =
                Seq.singleton $
                  Stream
                    { streamId = streamId
                    , entries
                    }
          liftIO . modifyIORef' streams $ Map.insertWith ins key newStream
          pure . BulkString $ showt streamId
        _ -> do
          let streamId =
                StreamId
                  { timePart
                  , sequenceNumber = if timePart == 0 then 1 else 0
                  }
          let newStream =
                Seq.singleton $
                  Stream
                    { streamId = streamId
                    , entries
                    }
          liftIO . modifyIORef' streams $ Map.insertWith ins key newStream
          pure . BulkString $ showt streamId

    ins :: Seq Stream -> Seq Stream -> Seq Stream
    ins _ Seq.Empty = error "should be unreachable"
    ins newStream oldStream = oldStream <> newStream

runXRangeCommand
  :: MonadIO m
  => StreamKey
  -> (StreamId, StreamId)
  -> RedisM m RespType
runXRangeCommand key (start, end) = do
  env <- ask
  allStreams <- liftIO $ readIORef env.streams
  case Map.lookup key allStreams of
    Just strs ->
      pure
        . Array
        . map streamToArray
        . toList
        . Seq.dropWhileL (\s -> s.streamId < start)
        $ Seq.dropWhileR (\s -> end < s.streamId) strs
    Nothing -> pure $ Array []

runXReadCommand :: MonadIO m => XReadOptions -> RedisM m RespType
runXReadCommand options = do
  env <- ask
  allStreams <- liftIO $ readIORef env.streams
  let result = Array
        . flip map (options.streamKeys `zip` options.streamIdBounds)
        $ \(streamKey, start) -> do
          case Map.lookup streamKey allStreams of
            Just strs -> do
              let streamArrays =
                    [ Array . map streamToArray . toList $
                        Seq.dropWhileL (\s -> s.streamId <= start) strs
                    ]
              Array $ BulkString streamKey : streamArrays
            Nothing -> Array []
  pure result
