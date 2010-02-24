{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE Rank2Types #-}

module Sirenial.Query (
    -- * Describing tables
    Table(..), Ref(..), Field(..), FieldType(..),
    TableAlias(..), Expr(..),
    
    -- * SELECT queries
    Select(..), JoinType(..), SelectStmt(..), ExecSelect,
    primKey, foreignKey, (#), from, leftJoin, returnAll,
  ) where

import Control.Applicative
import Control.Monad
import Data.Time.Calendar


-- Types

-- | A table description, indexed by its own (phantom) index.
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
  TyRef     :: FieldType (Ref a)
  TyMaybe   :: FieldType a -> FieldType (Maybe a)


-- | SQL expressions indexed by their type.
data Expr a where
  ExPure   :: a -> Expr a
  ExApply  :: Expr (a -> b) -> Expr a -> Expr b
  ExGet    :: TableAlias t -> Field t a -> Expr a
  ExEq     :: Expr a -> Expr a -> Expr Bool
  ExTrue   :: Expr Bool
  ExRef    :: Int -> Expr (Ref a)

instance Functor Expr where
  fmap   = liftA

instance Applicative Expr where
  pure   = ExPure
  (<*>)  = ExApply


-- | SQL constructs such as FROM and JOIN introduce new table aliases.
data TableAlias t = TableAlias { getAlias :: Int }


-- SELECT

-- | The Select monad handles the supply of new table aliases arising from FROMs and JOINs.
data Select a where
  SeReturn  :: a -> Select a
  SeBind    :: Select a -> (a -> Select b) -> Select b
  SeFrom    :: Table t -> Select (TableAlias t)
  SeJoin    :: JoinType -> Field t a -> Expr a -> Select (TableAlias t)

data SelectStmt a = SelectStmt
  { ssResult :: Expr a
  , ssWhere  :: Expr Bool
  }

instance Functor Select where
  fmap    = liftM

instance Monad Select where
  return  = SeReturn
  (>>=)   = SeBind

data JoinType = InnerJoin | LeftJoin | RightJoin
  deriving (Eq, Read, Show, Enum)

-- | Execute a SELECT statement.
type ExecSelect = forall a. Select (SelectStmt a) -> IO [a]


-- SELECT aliases

-- | Select from a table. (An alias for 'SeFrom'.)
from :: Table t -> Select (TableAlias t)
from = SeFrom

-- | Retrieve a field from a table. (An alias for 'ExGet'.)
(#) :: TableAlias t -> Field t a -> Expr a
(#) = ExGet

-- | Short for @'SeJoin' 'LeftJoin'@.
leftJoin :: Field t a -> Expr a -> Select (TableAlias t)
leftJoin = SeJoin LeftJoin

-- | Describe a primary key field named @"id"@.
primKey :: Table t -> Field t (Ref t)
primKey t = Field t "id" TyRef

-- | Describe a foreign key from table @t@ to table @t'@.
foreignKey :: Table t -> String -> Table t' -> Field t (Ref t')
foreignKey t name _ = Field t name TyRef

-- | Return an expression with no WHERE clause.
returnAll :: Expr a -> Select (SelectStmt a)
returnAll expr = return (SelectStmt expr ExTrue)
