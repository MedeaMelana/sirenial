{-# LANGUAGE RankNTypes #-}

module Sirenial.SqlGen where

import Sirenial.QueryString
import Sirenial.Expr
import Sirenial.Modify

-- | Captures the conversion of expressions and statements to query strings.
-- The actual implementation will depend on the underlying database engine.
data SqlGen = SqlGen
  { -- | Convert an expression to SQL.
    exprToQs   :: forall a. Expr a -> QueryString,
    -- | Convert a SELECT statement to SQL. The respective arguments are: the
    -- tables that are selected from, the fields that are selected from these
    -- tables (table index, field name) and an expression modelling the WHERE
    -- clause.
    selectToQs :: [String] -> [(Int, String)] -> Expr Bool -> QueryString,
    -- | Convert a modification statement to SQL.
    modifyToQs :: ModifyStmt -> QueryString
  }
