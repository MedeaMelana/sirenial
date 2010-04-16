{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Sirenial.QueryString
 ( QueryString, qss, qsv, prepareQs, renderQs, listQs ) where

import Prelude hiding (concatMap)
import Data.Foldable (foldMap, concatMap)
import Data.List (intersperse)
import Data.FMList
import Data.Monoid
import Database.HDBC

-- | A query string consists of string chunks interspersed with SQL literal values.
newtype QueryString = QueryString { unQS :: FMList (Either String SqlValue) }
  deriving (Show, Monoid)

instance Eq QueryString where
  QueryString x == QueryString y = toList x == toList y

-- | Lift a string literal into a query string.
qss :: String -> QueryString
qss = QueryString . singleton . Left

-- | Lift an SQL literal into a query string.
qsv :: SqlValue -> QueryString
qsv = QueryString . singleton . Right

-- | Convert a query string to a string with ?\'s as value placeholders,
-- coupled with the actual values. The string is passed to 'prepare'; then the
-- resulting 'Statement' and values are passed to 'execute'.
prepareQs :: QueryString -> (String, [SqlValue])
prepareQs = foldMap f . unQS
  where
    f p = case p of
      Left s   -> (s,    [])
      Right v  -> ("?",  [v])

-- | Convert a query string to a human-readable format.
renderQs :: QueryString -> String
renderQs = concatMap f . unQS
  where
    f (Left s)   = s
    f (Right v)  = "{" ++ show v ++ "}"

-- | Wrap a list of query strings in list (tuple) syntax. E.g. @listQs
-- [a,b,c]@ becomes @"(a,b,c)"@.
listQs :: [QueryString] -> QueryString
listQs qs = qss "(" <> mconcat (intersperse (qss ",") qs) <> qss ")"

(<>) :: Monoid m => m -> m -> m
(<>) = mappend
