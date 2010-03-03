{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}

module Sirenial.Expr (
    -- * SQL expressions
    Expr(..), TableAlias(..), ToExpr(..),
    (#), (.==.), (.<.), (.&&.), (.||.), exprAnd, exprOr,
    lits,
  ) where

import Sirenial.Tables

import Control.Applicative
import Control.Monad.State
import Database.HDBC
import Data.Convertible


-- | SQL expressions indexed by their type.
data Expr a where
  ExPure    :: a -> Expr a
  ExApply   :: Expr (a -> b) -> Expr a -> Expr b
  ExGet     :: Convertible SqlValue a => TableAlias t -> Field t a -> Expr a
  ExEq      :: Eq a => Expr a -> Expr a -> Expr Bool
  ExLT      :: Ord a => Expr a -> Expr a -> Expr Bool
  ExAnd     :: Expr Bool -> Expr Bool -> Expr Bool
  ExOr      :: Expr Bool -> Expr Bool -> Expr Bool
  ExBool    :: Bool -> Expr Bool
  ExString  :: String -> Expr String
  ExRef     :: Ref t -> Expr (Ref t)

instance Functor Expr where
  fmap   = liftA

instance Applicative Expr where
  pure   = ExPure
  (<*>)  = ExApply

-- | SQL constructs such as FROM and JOIN introduce new table aliases.
data TableAlias t = TableAlias { getAlias :: Int }

-- | Lift a value into the 'Expr' functor.
class     ToExpr a        where expr :: a -> Expr a
instance  ToExpr [Char]   where expr = ExString
instance  ToExpr Bool     where expr = ExBool
instance  ToExpr (Ref t)  where expr = ExRef

lits :: Expr a -> [String]
lits e =
  case e of
    ExPure _ -> []
    ExApply f x -> lits f ++ lits x
    ExGet _ _ -> []
    ExEq x y -> lits x ++ lits y
    ExLT x y -> lits x ++ lits y
    ExAnd x y -> lits x ++ lits y
    ExOr x y -> lits x ++ lits y
    ExBool b -> [show b]
    ExString s -> [show s]
    ExRef r -> [show r]

-- | Retrieve a field from a table. (An alias for 'ExGet'.)
(#) :: Convertible SqlValue a => TableAlias t -> Field t a -> Expr a
(#) = ExGet

-- | Compare two values for equality.
(.==.) :: Eq a => Expr a -> Expr a -> Expr Bool
(.==.) = ExEq
infixl 4 .==.

(.<.) :: Ord a => Expr a -> Expr a -> Expr Bool
(.<.) = ExLT
infix 4 .<.

(.&&.) :: Expr Bool -> Expr Bool -> Expr Bool
(.&&.) = ExAnd
infixr 3 .&&.

(.||.) :: Expr Bool -> Expr Bool -> Expr Bool
(.||.) = ExOr
infixr 2 .||.

exprAnd :: [Expr Bool] -> Expr Bool
exprAnd [] = expr True
exprAnd bs = foldr1 (.&&.) bs

exprOr :: [Expr Bool] -> Expr Bool
exprOr [] = expr False
exprOr bs = foldr1 (.||.) bs
