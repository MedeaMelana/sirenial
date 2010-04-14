{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Sirenial.Select (
    -- * Building SELECT queries
    Select, from, restrict,
    SelectStmt(..), toStmt, stmtToQs,
    
    -- * Executing SELECT queries
    Query(..), select, for,
  ) where

import Sirenial.Tables
import Sirenial.Expr
import Sirenial.QueryString

import Data.Monoid
import Data.List (intercalate)
import qualified Data.Traversable as T
import Control.Concurrent.MVar
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

(<>) :: Monoid m => m -> m -> m
(<>) = mappend

-- | Render a SELECT statement as query string.
stmtToQs :: SelectStmt a -> QueryString
stmtToQs (SelectStmt froms crit result) =
    qss "select " <> fieldsQs <> qss "\nfrom " <> fromQs <> whereQs
  where
    fields = selectedFields result
    fieldsQs
      | null fields  = qss "0"
      | otherwise    = qss $ intercalate ", " $ map fn fields
    fn (alias, fieldName) = "t" <> show alias <> "." <> fieldName
    tn (t, i) = t <> " t" <> show (i :: Integer)
    fromQs = qss $ intercalate ", "  $ map tn $ zip froms [0..]
    whereQs :: QueryString
    whereQs =
      case crit of
        ExAnd []  -> mempty
        _         -> qss "\nwhere " <> exprToQs crit


-- Executing SELECT queries

-- | The query monad allows the execution of SELECT queries. Parallel
-- composition of queries using 'QuApply' allows them to be merged
-- efficiently, while sequential composition using 'QuBind' allows inspection
-- of results before deciding on subsequent queries.
data Query a where
  QuPure    :: a -> Query a
  QuSelect  :: SelectStmt a -> Maybe (MVar [a]) -> Query [a]
  QuApply   :: Query (a -> b) -> Query a -> Query b
  QuBind    :: Query a -> (a -> Query b) -> Query b

instance Functor Query where
  fmap    = liftA

instance Applicative Query where
  pure    = QuPure
  (<*>)   = QuApply

instance Monad Query where
  return  = QuPure
  (>>=)   = QuBind

-- | Execute a SELECT query.
select :: Select (Expr a) -> Query [a]
select s = QuSelect (toStmt s) Nothing

-- | Run queries for each element in a container (e.g. a list). The queries
-- may be merged into optimized queries.
for :: T.Traversable f => f a -> (a -> Query b) -> Query (f b)
for = T.for
