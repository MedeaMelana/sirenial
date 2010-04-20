{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}

module Sirenial.Expr (
    -- * SQL expressions
    Expr(..), TableAlias(..), ToExpr(..),
    (#), (.==.), (.<.), (.&&.), (.||.), exprAnd, exprOr,
    isPure, isTrue, isFalse, selectedFields
  ) where

import Sirenial.Tables
import Sirenial.Util

import Control.Applicative
import Control.Monad.State
import Database.HDBC
import Data.Monoid
import Data.Convertible
import Data.List


-- | SQL expressions indexed by their type.
data Expr a where
  ExPure    :: a -> Expr a
  ExApply   :: Expr (a -> b) -> Expr a -> Expr b
  ExGet     :: Convertible SqlValue a => TableAlias t -> Field t a -> Expr a
  ExEq      :: Eq a => Expr a -> Expr a -> Expr Bool
  ExLT      :: Ord a => Expr a -> Expr a -> Expr Bool
  ExAnd     :: [Expr Bool] -> Expr Bool
  ExOr      :: [Expr Bool] -> Expr Bool
  ExLit     :: Convertible a SqlValue => a -> Expr a

instance Functor Expr where
  fmap   = liftA

instance Applicative Expr where
  pure   = ExPure
  (<*>)  = ExApply

-- | In SELECT queries, constructs such as FROM and JOIN introduce new table
-- aliases. The 'Select' monad takes care of supplying these aliases,
-- containing 'Just's with unique numbers. In INSERT, DELETE and UPDATE
-- queries, there is always exactly one table being modified. In those cases,
-- a 'TableAlias' containing a 'Nothing' is used. In both cases, the aliases
-- can be used as argument to @#@ to retrieve fields.
data TableAlias t = TableAlias { getAlias :: Maybe Int }

-- | Lift a value into the 'Expr' functor.
class     ToExpr a        where expr :: a -> Expr a
instance  ToExpr [Char]   where expr = ExLit
instance  ToExpr Bool     where expr b = if b then ExAnd [] else ExOr []
instance  ToExpr (Ref t)  where expr = ExLit

-- | Retrieve a field from a table.
(#) :: Convertible SqlValue a => TableAlias t -> Field t a -> Expr a
(#) = ExGet

-- | Compare two values for equality.
(.==.) :: Eq a => Expr a -> Expr a -> Expr Bool
x@(ExGet _ _) .==. y = ExEq x y
x .==. y@(ExGet _ _) = ExEq y x
x .==. y = ExEq x y
infixl 4 .==.

-- | Less than.
(.<.) :: Ord a => Expr a -> Expr a -> Expr Bool
(.<.) = ExLT
infix 4 .<.

-- | Binary conjunction.
(.&&.) :: Expr Bool -> Expr Bool -> Expr Bool
ExAnd ps .&&. ExAnd qs = ExAnd (ps ++ qs)
ExAnd ps .&&. q = ExAnd (ps ++ [q])
p .&&. ExAnd qs = ExAnd ([p] ++ qs)
p .&&. q = ExAnd [p, q]
infixr 3 .&&.

-- | Binary disjunction.
(.||.) :: Expr Bool -> Expr Bool -> Expr Bool
ExOr ps .||. ExOr qs = ExOr (ps ++ qs)
ExOr ps .||. q = ExOr (ps ++ [q])
p .||. ExOr qs = ExOr ([p] ++ qs)
p .||. q = ExOr [p, q]
infixr 2 .||.

-- | N-ary conjunction.
exprAnd :: [Expr Bool] -> Expr Bool
exprAnd [p] = p
exprAnd ps = ExAnd ps

-- | N-ary disjunction.
exprOr :: [Expr Bool] -> Expr Bool
exprOr [p] = p
exprOr ps = ExOr ps

-- | If an expression does not depend on data from the database (e.g. contains
-- no 'ExGet' constructors), it can be evaluated without connecting to the
-- database. In that case, 'isPure' returns @'Just' x@.
isPure :: Expr a -> Maybe a
isPure expr =
  case expr of
    ExPure x       -> pure x
    ExApply ef ex  -> isPure ef <*> isPure ex
    ExGet _ _      -> Nothing
    ExEq x y       -> (==) <$> isPure x <*> isPure y
    ExLT x y       -> (<)  <$> isPure x <*> isPure y
    ExAnd ps       -> and  <$> mapM isPure ps
    ExOr ps        -> or   <$> mapM isPure ps
    ExLit x        -> pure x

isTrue :: Expr Bool -> Bool
isTrue expr =
  case expr of
    ExAnd ps  -> all isTrue ps
    ExOr ps   -> any isTrue ps
    p ->
      case isPure p of
        Just b   -> b
        Nothing  -> False

isFalse :: Expr Bool -> Bool
isFalse expr =
  case expr of
    ExAnd ps  -> any isTrue ps
    ExOr ps   -> all isTrue ps
    p ->
      case isPure p of
        Just b   -> not b
        Nothing  -> False

selectedFields :: Expr a -> [(Int, String)]
selectedFields = nub . sort . go
  where
    go :: Expr a -> [(Int, String)]
    go expr =
      case expr of
        ExGet (TableAlias (Just a)) f ->
          [(a, fieldName f)]
        ExApply f x  -> go f <> go x
        ExEq x y     -> go x <> go y
        ExLT x y     -> go x <> go y
        ExAnd ps     -> concatMap go ps
        ExOr ps      -> concatMap go ps
        _            -> []
