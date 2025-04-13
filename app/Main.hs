{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wall -Wno-unused-top-binds #-}

module Main (main) where

import Network.Simple.TCP (HostPreference (HostAny), closeSock, serve)
import Pipes
import Pipes.Network.TCP (fromSocket, toSocket)
import qualified Pipes.Prelude as P
import System.IO

import RedisM (bufferSize)

main :: IO ()
main = do
  -- Disable output buffering
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering

  let port = "6379"
  putStrLn $ "Redis server listening on port " ++ port

  serve HostAny port $ \(socket, address) -> do
    putStrLn $ "successfully connected client: " ++ show address
    runEffect $
      fromSocket socket bufferSize
        >-> P.map (const "+PONG\r\n")
        >-> toSocket socket
    closeSock socket
