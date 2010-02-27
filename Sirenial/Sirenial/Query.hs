{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE Rank2Types #-}

module Sirenial.Query (
    -- * Describing tables
    Table(..), Ref(..), Field(..), FieldType(..),
    primKey, foreignKey,

    -- * SQL expressions
    Expr(..), TableAlias(..),
    (#),

    -- * Building SELECT queries
    Select(..), JoinType(..), SelectStmt(..),
    from, leftJoin, returnAll,

    -- * Executing SELECT queries
    ExecSelect(..), Merge(..),
    for,
  ) where

import Data.Traversable (sequenceA)
import Data.Time.Calendar
import Control.Applicative
import Control.Monad


-- Describing tables and their fields.

-- | A table description, indexed by its own phantom type.
data Table t = Table
  { tableName  :: String
  , tableKey   :: Field t (Ref t)
  }

-- | @Ref t@ is the type of the primary key to table @t@.
newtype Ref t = Ref { getRef :: Int }

-- | A typed field in a table.
data Field t a = Field
  { fieldTable  :: Table t
  , fieldName   :: String
  , fieldType   :: FieldType a
  }

-- | Possible types of fields, indexed.
data FieldType :: * -> * where
  TyInt     :: FieldType Int
  TyString  :: FieldType String
  TyDay     :: FieldType Day
  TyRef     :: FieldType (Ref t)
  TyMaybe   :: FieldType a -> FieldType (Maybe a)

-- | Describe a primary key field named @\"id\"@.
primKey :: Table t -> Field t (Ref t)
primKey t = Field t "id" TyRef

-- | Describe a foreign key from table @t@ to table @t'@.
foreignKey :: Table t -> String -> Table t' -> Field t (Ref t')
foreignKey t name _ = Field t name TyRef


-- | SQL expressions indexed by their type.
data Expr a where
  ExPure   :: a -> Expr a
  ExApply  :: Expr (a -> b) -> Expr a -> Expr b
  ExGet    :: TableAlias t -> Field t a -> Expr a
  ExEq     :: Expr a -> Expr a -> Expr Bool
  ExBool   :: Bool -> Expr Bool
  ExRef    :: Int -> Expr (Ref a)
  ExAlias  :: Int -> Expr (TableAlias t)

instance Functor Expr where
  fmap   = liftA

instance Applicative Expr where
  pure   = ExPure
  (<*>)  = ExApply

-- | SQL constructs such as FROM and JOIN introduce new table aliases.
data TableAlias t = TableAlias { getAlias :: Int }

-- | Retrieve a field from a table. (An alias for 'ExGet'.)
(#) :: TableAlias t -> Field t a -> Expr a
(#) = ExGet


-- Building SELECT queries

-- | The Select monad handles the supply of new table aliases arising from FROMs and JOINs.
data Select a where
  SeReturn  :: a -> Select a
  SeBind    :: Select a -> (a -> Select b) -> Select b
  SeFrom    :: Table t -> Select (TableAlias t)
  SeJoin    :: JoinType -> Field t a -> Expr a -> Select (TableAlias t)

instance Functor Select where
  fmap    = liftM

instance Monad Select where
  return  = SeReturn
  (>>=)   = SeBind

data JoinType = InnerJoin | LeftJoin | RightJoin
  deriving (Eq, Read, Show, Enum)

data SelectStmt a = SelectStmt
  { ssResult :: Expr a
  , ssWhere  :: Expr Bool
  }

-- | Select from a table. (An alias for 'SeFrom'.)
from :: Table t -> Select (TableAlias t)
from = SeFrom

-- | Short for @'SeJoin' 'LeftJoin'@.
leftJoin :: Field t a -> Expr a -> Select (TableAlias t)
leftJoin = SeJoin LeftJoin

-- | Return an expression with no WHERE clause.
returnAll :: Expr a -> Select (SelectStmt a)
returnAll expr = return (SelectStmt expr (ExBool True))


-- Executing SELECT queries

-- | Monadic context in which SELECT queries may be executed.
data ExecSelect a where
  EsReturn    :: a -> ExecSelect a
  EsBind      :: ExecSelect a -> (a -> ExecSelect b) -> ExecSelect b
  EsExec      :: Select (SelectStmt a) -> ExecSelect [a]
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

-- | Run queries for each element in a list. The queries may be merged into optimized queries.
for :: [a] -> (a -> ExecSelect b) -> ExecSelect [b]
for xs f = (EsExecMany . sequenceA . map (MeSelect . f)) xs
