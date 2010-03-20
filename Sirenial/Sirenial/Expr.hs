{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}

module Sirenial.Expr (
    -- * SQL expressions
    Expr(..), TableAlias(..), ToExpr(..),
    (#), (.==.), (.<.), (.&&.), (.||.), exprAnd, exprOr,
    isPure,
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
  -- ExInList  :: Eq a => Expr a -> [Expr a] -> Expr Bool
  ExEq      :: Eq a => Expr a -> Expr a -> Expr Bool
  ExLT      :: Ord a => Expr a -> Expr a -> Expr Bool
  ExAnd     :: [Expr Bool] -> Expr Bool
  ExOr      :: [Expr Bool] -> Expr Bool
  -- ExString  :: String -> Expr String
  -- ExRef     :: Ref t -> Expr (Ref t)
  ExLit     :: Convertible a SqlValue => a -> Expr a

instance Functor Expr where
  fmap   = liftA

instance Applicative Expr where
  pure   = ExPure
  (<*>)  = ExApply

-- | SQL constructs such as FROM and JOIN introduce new table aliases.
data TableAlias t = TableAlias { getAlias :: Int }

-- | Lift a value into the 'Expr' functor.
class     ToExpr a        where expr :: a -> Expr a
instance  ToExpr [Char]   where expr = ExLit
instance  ToExpr Bool     where expr b = if b then ExAnd [] else ExOr []
instance  ToExpr (Ref t)  where expr = ExLit

-- | Retrieve a field from a table. (An alias for 'ExGet'.)
(#) :: Convertible SqlValue a => TableAlias t -> Field t a -> Expr a
(#) = ExGet

-- | Compare two values for equality.
(.==.) :: Eq a => Expr a -> Expr a -> Expr Bool
x@(ExGet _ _) .==. y = ExEq x y
x .==. y@(ExGet _ _) = ExEq y x
x .==. y = ExEq x y
infixl 4 .==.

(.<.) :: Ord a => Expr a -> Expr a -> Expr Bool
(.<.) = ExLT
infix 4 .<.

(.&&.) :: Expr Bool -> Expr Bool -> Expr Bool
ExAnd ps .&&. ExAnd qs = ExAnd (ps ++ qs)
ExAnd ps .&&. q = ExAnd (ps ++ [q])
p .&&. ExAnd qs = ExAnd ([p] ++ qs)
p .&&. q = ExAnd [p, q]
infixr 3 .&&.

(.||.) :: Expr Bool -> Expr Bool -> Expr Bool
ExOr ps .||. ExOr qs = ExOr (ps ++ qs)
ExOr ps .||. q = ExOr (ps ++ [q])
p .||. ExOr qs = ExOr ([p] ++ qs)
p .||. q = ExOr [p, q]
infixr 2 .||.

exprAnd :: [Expr Bool] -> Expr Bool
exprAnd [p] = p
exprAnd ps = ExAnd ps

exprOr :: [Expr Bool] -> Expr Bool
exprOr [p] = p
exprOr ps = ExOr ps

isPure :: Expr a -> Maybe a
isPure expr =
  case expr of
    ExPure x       -> pure x
    ExApply ef ex  -> isPure ef <*> isPure ex
    ExGet _ _      -> Nothing
    -- ExInList x ys  -> elem <$> isPure x <*> mapM isPure ys
    ExEq x y       -> (==) <$> isPure x <*> isPure y
    ExLT x y       -> (<)  <$> isPure x <*> isPure y
    ExAnd ps       -> and  <$> mapM isPure ps
    ExOr ps        -> or   <$> mapM isPure ps
    ExLit x        -> pure x
    -- ExString s     -> pure s
    -- ExRef r        -> pure r

isTrue :: Expr Bool -> Bool
isTrue expr =
  case expr of
    ExAnd ps  -> all isTrue ps
    ExOr ps   -> any isTrue ps
    p ->
      case isPure p of
        Just b -> b
        Nothing -> False

-- optimize :: Expr a -> Expr a
-- optimize expr =
--   case expr of
--     ExOr (ExEq (ExGet a f) x : ps) ->
--       case matchGets a f ps of
--         (yes, no) -> exInList (ExGet a f) (x : yes) .||. exprOr no
--   where
--     matchGets a f [] = ([], [])
--     matchGets a f ()
--       case e of
