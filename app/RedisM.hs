module RedisM
  ( RedisM (..)
  , bufferSize
  )
where

import Control.Monad.Reader

newtype RedisM r m a = RedisM
  { runRedisM :: ReaderT r m a
  }
  deriving
    ( Functor
    , Applicative
    , Monad
    , MonadReader r
    , MonadIO
    )

bufferSize :: Integral a => a
bufferSize = 4096
