{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wall #-}

module Util
  ( (~=)
  ) where

import Data.Function (on)
import Data.Text (Text)
import Data.Text qualified as T

(~=) :: Text -> Text -> Bool
(~=) = (==) `on` T.toCaseFold
