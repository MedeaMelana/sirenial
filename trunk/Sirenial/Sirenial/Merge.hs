{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE Rank2Types #-}

-- | Transparently merge SELECT queries.
module Sirenial.Merge (Merge, select, concatSelect, execMerge) where

import Sirenial.Query

import Data.Traversable
import Control.Applicative

-- | SELECT statements can be lifted into the @Merge@ functor, allowing them
-- to be combined with other queries in applicative style, possibly resulting
-- in increased performance.
data Merge a where
  MePure    :: a -> Merge a
  MeApply   :: Merge (a -> b) -> Merge a -> Merge b
  
  MeSelect  :: Select (SelectStmt a) -> Merge [a]

instance Functor Merge where
  fmap   = liftA

instance Applicative Merge where
  pure   = MePure
  (<*>)  = MeApply

-- Bring a query into the 'Merge' functor.
select :: Select (SelectStmt a) -> Merge [a]
select = MeSelect

-- | Merge a list of queries and flatten the resulting record lists.
concatSelect :: [Select (SelectStmt a)] -> Merge [a]
concatSelect ss = concat <$> sequenceA (map select ss)

-- Execute a merged query. The 'Merge' functor tries its best to combine
-- similar queries, hopefully resulting in less calls to 'ExecSelect' than the
-- number of SELECT queries that were lifted.
execMerge :: ExecSelect -> Merge b -> IO b
execMerge execSelect = go
  where
    -- Todo: insert magical merge optimizations.
    go :: Merge a -> IO a
    go merge = case merge of
      MePure x       -> return x
      MeApply mf mx  -> go mf <*> go mx
      MeSelect s     -> execSelect s
