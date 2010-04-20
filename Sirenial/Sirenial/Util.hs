module Sirenial.Util ((<>)) where

import Data.Monoid

(<>) :: Monoid m => m -> m -> m
(<>) = mappend
