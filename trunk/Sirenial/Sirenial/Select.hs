{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Sirenial.Select (
    -- * Building SELECT queries
    Select, from, restrict,
    SelectStmt(..), toStmt,
    
    -- * Executing SELECT queries
    Suspend(..), execSelect, for,
  ) where

import Sirenial.Tables
import Sirenial.Expr

import Control.Concurrent.MVar
import qualified Data.Traversable as T
import qualified Data.Sequence as Seq
import Control.Applicative
import Control.Monad.State
import Control.Arrow


-- | The Select monad handles the supply of new table aliases arising from FROMs and JOINs.
newtype Select a = Select (State ([String], [Expr Bool]) a)
  deriving (Functor, Monad, MonadState ([String], [Expr Bool]))

instance Applicative Select where
  pure    = return
  (<*>)   = ap

-- | Select from a table. The resulting alias can be used in expressions to
-- retrieve fields using 'ExGet'.
from :: Table t -> Select (TableAlias t)
from t = do
  (fs, ws) <- get
  put (fs ++ [tableName t], ws)
  return (TableAlias (length fs))

-- | Add a WHERE-clause.
restrict :: Expr Bool -> Select ()
restrict p = modify (second (++ [p]))

-- | A reified select statement in which aliases have been supplied.
data SelectStmt a = SelectStmt
  { ssFroms   :: [String]
  , ssCrit  :: Expr Bool
  , ssResult  :: Expr a
  }

-- | Run the 'Select' computation, supplying aliases. The aliases are indices
-- into the 'ssFroms' list.
toStmt :: Select (Expr a) -> SelectStmt a
toStmt (Select s) = SelectStmt froms (exprAnd wheres) result
  where
    (result, (froms, wheres)) = runState s ([], [])


-- Executing SELECT queries

data Suspend a where
  SuPure    :: a -> Suspend a
  SuApply   :: Suspend (a -> b) -> Suspend a -> Suspend b
  SuBind    :: Suspend a -> (a -> Suspend b) -> Suspend b
  SuSelect  :: SelectStmt a -> Maybe (MVar (Seq.Seq a)) -> Suspend [a]

instance Functor Suspend where
  fmap    = liftA

instance Applicative Suspend where
  pure    = SuPure
  (<*>)   = SuApply

instance Monad Suspend where
  return  = SuPure
  (>>=)   = SuBind

-- | An alias for 'EsExec'.
execSelect :: Select (Expr a) -> Suspend [a]
execSelect s = SuSelect (toStmt s) Nothing

-- | Run queries for each element in a container (e.g. a list). The queries
-- may be merged into optimized queries.
for :: T.Traversable f => f a -> (a -> Suspend b) -> Suspend (f b)
for = T.for
