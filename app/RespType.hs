{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE NegativeLiterals #-}
{-# LANGUAGE OverloadedStrings #-}

module RespType
  ( RespType (..)
  , ok
  , crlf
  )
where

import Data.Text (Text)
import Data.Text qualified as T
import TextShow

data RespType
  = SimpleString Text
  | SimpleError Text
  | Integer Int
  | BulkString Text
  | NullBulkString
  | Array [RespType]
  | -- | TODO: Maybe this shouldn't be here?
    PsyncResponse
  | RdbData Text
  | ReplConfAck Int
  deriving (Eq)
  deriving (TextShow) via FromStringShow RespType

instance Show RespType where
  show (SimpleString s) = "+" <> T.unpack s <> crlf
  show (SimpleError e) = "-" <> T.unpack e <> crlf
  show (Integer n) =
    ":"
      <> (if signum n == -1 then "-" else "")
      <> show (abs n)
      <> crlf
  show (BulkString s) = "$" <> show len <> crlf <> T.unpack s <> crlf
    where
      len = T.length s
  show NullBulkString = "$-1" <> crlf
  show (Array a) = "*" <> show len <> crlf <> concatMap show a
    where
      len = length a
  show PsyncResponse = "PSYNC response"
  show (RdbData s) = "RDB data (length: " <> show len <> ")"
    where
      len = T.length s
  show (ReplConfAck n) =
    show . Array $
      map
        BulkString
        [ "REPLCONF"
        , "ACK"
        , T.pack $ show n
        ]

ok :: RespType
ok = SimpleString "OK"

crlf :: String
crlf = "\r\n"
