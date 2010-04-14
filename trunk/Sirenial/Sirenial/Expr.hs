{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}

module Sirenial.Expr (
    -- * SQL expressions
    Expr(..), TableAlias(..), ToExpr(..),
    (#), (.==.), (.<.), (.&&.), (.||.), exprAnd, exprOr,
    isPure, isTrue, isFalse, exprToQs, selectedFields
  ) where

import Sirenial.Tables
import Sirenial.QueryString

import Control.Applicative
import Control.Monad.State
import Database.HDBC
import Data.Either
import Data.Monoid
import Data.Convertible
import Data.List
import qualified Data.Map as M
import Control.Arrow (first, second)


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

(<>) :: Monoid m => m -> m -> m
(<>) = mappend

selectedFields :: Expr a -> [(Int, String)]
selectedFields = nub . sort . go
  where
    go :: Expr a -> [(Int, String)]
    go expr =
      case expr of
        ExGet a f    -> [(getAlias a, fieldName f)]
        ExApply f x  -> go f <> go x
        ExEq x y     -> go x <> go y
        ExLT x y     -> go x <> go y
        ExAnd ps     -> concatMap go ps
        ExOr ps      -> concatMap go ps
        _            -> []

-- | Render an SQL expression as query string.
exprToQs :: Expr a -> QueryString
exprToQs expr = case expr of
    ExPure _     -> error "Pure values cannot be converted to SQL."
    ExApply _ _  -> error "Pure values cannot be converted to SQL."
    ExGet a f    -> qss $ "t" <> show (getAlias a) <> "." <> fieldName f
    ExEq x y     -> parens $ exprToQs x <> qss " = " <> exprToQs y
    ExLT x y     -> parens $ exprToQs x <> qss " < " <> exprToQs y
    ExAnd ps     -> reduce " and " "1" (map exprToQs ps)
    ExOr  ps     -> disjToQs ps
    ExLit x      -> qsv (toSql x)

reduce :: String -> String -> [QueryString] -> QueryString
reduce sep zero exprs =
  case exprs of
    []      -> qss zero
    [expr]  -> expr
    _       -> parens $ mconcat $ intersperse (qss sep) exprs

parens :: QueryString -> QueryString
parens x = qss "(" <> x <> qss ")"

-- It'd be nice if we could perform this rewrite on Exprs directly, but we
-- don't have enough type information for that.
disjToQs :: [Expr Bool] -> QueryString
disjToQs = allToQs . first group . partitionEithers . map part
  where
    part :: Expr a -> Either ((Int, String), QueryString) QueryString
    part expr =
      case expr of
        ExEq (ExGet alias field) y  -> Left ((getAlias alias, fieldName field), exprToQs y)
        _                           -> Right (exprToQs expr)
    group :: Ord k => [(k, v)] -> [(k, [v])]
    group = M.toList . M.fromListWith (<>) . reverse . map (second (:[]))
    allToQs :: ([((Int, String), [QueryString])], [QueryString]) -> QueryString
    allToQs (gs, ss) = disjToQs' (map singleToQs gs <> ss)
    singleToQs :: ((Int, String), [QueryString]) -> QueryString
    singleToQs ((a, f), es) = qss ("t" <> show a <> "." <> f) <>
      case nub es of
        [e]    -> qss " = " <> e
        nubEs  -> qss " in (" <> mconcat (intersperse (qss ",") nubEs) <> qss ")"
    disjToQs' :: [QueryString] -> QueryString
    disjToQs' = reduce " or " "0"
