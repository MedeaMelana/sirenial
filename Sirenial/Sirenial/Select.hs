{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Sirenial.Select (
    -- * Building SELECT queries
    Select, from, restrict,
    SelectStmt(..), toStmt,
    
    -- * Executing SELECT queries
    ExecSelect(..), Merge(..), execSelect, for,
  ) where

import Sirenial.Tables
import Sirenial.Expr

import qualified Data.Traversable as T
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
  , ssWheres  :: [Expr Bool]
  , ssResult  :: Expr a
  }

-- | Run the 'Select' computation, supplying aliases. The aliases are indices
-- into the 'ssFroms' list.
toStmt :: Select (Expr a) -> SelectStmt a
toStmt (Select s) = SelectStmt froms wheres result
  where
    (result, (froms, wheres)) = runState s ([], [])


-- Executing SELECT queries

-- | Monadic context in which SELECT queries may be executed.
data ExecSelect a where
  EsReturn    :: a -> ExecSelect a
  EsBind      :: ExecSelect a -> (a -> ExecSelect b) -> ExecSelect b
  EsExec      :: Select (Expr a) -> ExecSelect [a]
  EsExecMany  :: Merge a -> ExecSelect a

instance Functor ExecSelect where
  fmap    = liftM

instance Applicative ExecSelect where
  pure    = return
  (<*>)   = ap

instance Monad ExecSelect where
  return  = EsReturn
  (>>=)   = EsBind

-- | Applicative context in which 'ExecSelect's may be merged into optimized queries.
data Merge a where
  MePure    :: a -> Merge a
  MeApply   :: Merge (a -> b) -> Merge a -> Merge b
  MeSelect  :: ExecSelect a -> Merge a

instance Functor Merge where
  fmap   = liftA

instance Applicative Merge where
  pure   = MePure
  (<*>)  = MeApply

-- | An alias for 'EsExec'.
execSelect :: Select (Expr a) -> ExecSelect [a]
execSelect = EsExec

-- | Run queries for each element in a list. The queries may be merged into optimized queries.
for :: [a] -> (a -> ExecSelect b) -> ExecSelect [b]
for xs f = EsExecMany (T.for xs (MeSelect . f))
