module Main
  ( main
  ) where

import Control.Monad (unless)
import Control.Monad.Reader
import Pipes
import Pipes.Network.TCP
import System.IO

import Options
import RedisM
import Replica
import Server
import Command

main :: IO ()
main = do
  -- Disable output buffering
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering

  redisEnv <- initialEnv

  unless redisEnv.isMasterNode $ do
    initReplica redisEnv

  let port' = redisEnv.options.port
  putStrLn $ "Redis server listening on port " ++ port'

  serve HostAny port' $ \(s, addr) -> do
    putStrLn $ "successfully connected client: " ++ show addr
    flip runReaderT redisEnv . runRedisM . runEffect $ runServer s
