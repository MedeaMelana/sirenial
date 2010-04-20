{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeOperators #-}

module Sirenial.Tables (
    -- * Describing tables
    Table(..), Ref(..), Field(..),
    primKey, foreignKey,
  ) where

import Data.Convertible
import Database.HDBC
import Control.Applicative
import Text.Read (readPrec)

-- | A table description, indexed by its own phantom type.
data Table t  = Table { tableName  :: String }

-- | @Ref t@ is the type of the primary key to table @t@.
newtype Ref t = Ref   { getRef     :: Integer }
  deriving (Enum, Eq, Integral, Num, Ord, Real)

instance Show (Ref t) where show (Ref r) = show r
instance Read (Ref t) where readPrec = Ref <$> readPrec

instance Convertible SqlValue (Ref t) where
  safeConvert sql =
    case safeConvert sql of
      Left e   -> Left e
      Right n  -> Right (Ref n)

instance Convertible (Ref t) SqlValue where
  safeConvert = safeConvert . getRef

-- | A typed field in a table.
data Field t a = Field
  { fieldTable  :: Table t
  , fieldName   :: String
  }

-- | Describe a primary key field named @\"id\"@.
primKey :: Table t -> Field t (Ref t)
primKey t = Field t "id"

-- | Describe a foreign key from table @t@ to table @t'@.
foreignKey :: Table t -> String -> Table t' -> Field t (Ref t')
foreignKey t name _ = Field t name
