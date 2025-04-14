{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# OPTIONS_GHC -Wall -Wno-unused-top-binds #-}

module Main
  ( main
  ) where

import Control.Monad.Reader
import Data.Text.Encoding qualified as TE
import Pipes
import Pipes.Attoparsec qualified as A
import Pipes.Network.TCP
import Pipes.Prelude qualified as P
import System.IO
import TextShow

import Command
import Options
import RedisEnv
import RedisM
import RespParser

main :: IO ()
main = do
  -- Disable output buffering
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering

  redisEnv <- initialEnv

  let port = redisEnv.options.port
  putStrLn $ "Redis server listening on port " ++ port

  serve HostAny port $ \(socket, addr) -> do
    putStrLn $ "successfully connected client: " ++ show addr
    flip runReaderT redisEnv . runRedisM . runEffect $ runServer (socket, addr)

runServer :: MonadIO m => (Socket, SockAddr) -> Effect (RedisM m) ()
runServer (socket, addr) = do
  void (A.parsed array (fromSocket socket bufferSize))
    >-> P.mapMaybe commandFromArray
    >-> P.wither (runCommand (socket, addr))
    >-> P.map (TE.encodeUtf8 . showt)
    >-> toSocket socket
