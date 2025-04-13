{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wall -Wno-unused-top-binds #-}

module Main (main) where

import Network.Simple.TCP (HostPreference (HostAny), closeSock, serve)
import System.IO

main :: IO ()
main = do
  -- Disable output buffering
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering

  let port = "6379"
  putStrLn $ "Redis server listening on port " ++ port
  serve HostAny port $ \(socket, address) -> do
    putStrLn $ "successfully connected client: " ++ show address
    closeSock socket
