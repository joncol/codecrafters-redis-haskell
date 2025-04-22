{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -Wall #-}

module RedisM
  ( RedisM (..)
  , bufferSize
  )
where

import Control.Monad.Reader

import RedisEnv

newtype RedisM m a = RedisM
  { runRedisM :: ReaderT RedisEnv m a
  }
  deriving
    ( Functor
    , Applicative
    , Monad
    , MonadReader RedisEnv
    , MonadIO
    )

bufferSize :: Integral a => a
bufferSize = 4096
