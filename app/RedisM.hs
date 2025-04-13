{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# OPTIONS_GHC -Wall #-} 

module RedisM where

import Control.Monad.Reader

import RedisEnv

newtype RedisM m a = RedisM
  -- { runRedisM :: ReaderT RedisEnv (S.Stream (S.Of (Maybe RespType)) m) a
  { runRedisM :: ReaderT RedisEnv m a
  }
  deriving
    ( Functor
    , Applicative
    , Monad
    , MonadReader RedisEnv
    , MonadIO
    )

bufferSize :: (Integral a) => a
bufferSize = 4096
