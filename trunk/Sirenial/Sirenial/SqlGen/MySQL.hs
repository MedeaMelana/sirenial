{-# LANGUAGE GADTs #-}

module Sirenial.SqlGen.MySQL (mysqlGen) where

import Sirenial.SqlGen
import Sirenial.Tables
import Sirenial.Expr
-- import Sirenial.Select
import Sirenial.Modify
import Sirenial.QueryString
import Sirenial.Util

import Data.Monoid
import Data.List
import qualified Data.Map as M
import Control.Arrow (first, second)
import Data.Either (partitionEithers)
import Database.HDBC (toSql)
import qualified Data.List as L


mysqlGen :: SqlGen
mysqlGen = SqlGen mysqlExprToQs mysqlSelectToQs mysqlModifyToQs

-- | Render a SELECT statement as query string.
mysqlSelectToQs :: [String] -> [(Int, String)] -> Expr Bool -> QueryString
mysqlSelectToQs froms fields crit =
    qss "select " <> fieldsQs <> qss "\nfrom " <> fromQs <> whereQs
  where
    fieldsQs
      | null fields  = qss "0"
      | otherwise    = qss $ L.intercalate ", " $ map fn fields
    fn (alias, fieldName) = "t" <> show alias <> "." <> fieldName
    tn (t, i) = t <> " t" <> show (i :: Integer)
    fromQs = qss $ L.intercalate ", "  $ map tn $ zip froms [0..]
    whereQs :: QueryString
    whereQs =
      if isTrue crit
        then mempty
        else qss "\nwhere " <> mysqlExprToQs crit

-- -- | Render a SELECT statement as query string.
-- mysqlStmtToQs :: SelectStmt a -> QueryString
-- mysqlStmtToQs (SelectStmt froms crit result) =
--     qss "select " <> fieldsQs <> qss "\nfrom " <> fromQs <> whereQs
--   where
--     fields = selectedFields result
--     fieldsQs
--       | null fields  = qss "0"
--       | otherwise    = qss $ intercalate ", " $ map fn fields
--     fn (alias, fieldName) = "t" <> show alias <> "." <> fieldName
--     tn (t, i) = t <> " t" <> show (i :: Integer)
--     fromQs = qss $ intercalate ", "  $ map tn $ zip froms [0..]
--     whereQs :: QueryString
--     whereQs =
--       case crit of
--         ExAnd []  -> mempty
--         _         -> qss "\nwhere " <> mysqlExprToQs crit

-- | Render an SQL expression as query string.
mysqlExprToQs :: Expr a -> QueryString
mysqlExprToQs expr = case expr of
    ExPure _     -> error "Pure values cannot be converted to SQL."
    ExApply _ _  -> error "Pure values cannot be converted to SQL."
    ExGet a f    -> qss $ "t" <> maybe mempty (\a' -> show a' <> ".") (getAlias a) <> fieldName f
    ExEq x y     -> parens $ mysqlExprToQs x <> qss " = " <> mysqlExprToQs y
    ExLT x y     -> parens $ mysqlExprToQs x <> qss " < " <> mysqlExprToQs y
    ExAnd ps     -> reduce " and " "1" (map mysqlExprToQs ps)
    ExOr  ps     -> disjToQs ps
    ExLit x      -> qsv (toSql x)

reduce :: String -> String -> [QueryString] -> QueryString
reduce sep zero exprs =
  case exprs of
    []      -> qss zero
    [expr]  -> expr
    _       -> parens $ mconcat $ intersperse (qss sep) exprs

-- It'd be nice if we could perform this rewrite on Exprs directly, but we
-- don't have enough type information for that.
disjToQs :: [Expr Bool] -> QueryString
disjToQs = allToQs . first group . partitionEithers . map part
  where
    part :: Expr a -> Either ((Maybe Int, String), QueryString) QueryString
    part expr =
      case expr of
        ExEq (ExGet alias field) y  -> Left ((getAlias alias, fieldName field), mysqlExprToQs y)
        _                           -> Right (mysqlExprToQs expr)
    group :: Ord k => [(k, v)] -> [(k, [v])]
    group = M.toList . M.fromListWith (<>) . reverse . map (second (:[]))
    allToQs :: ([((Maybe Int, String), [QueryString])], [QueryString]) -> QueryString
    allToQs (gs, ss) = disjToQs' (map singleToQs gs <> ss)
    singleToQs :: ((Maybe Int, String), [QueryString]) -> QueryString
    singleToQs ((a, f), es) = qss (maybe mempty (\a' -> "t" <> show a' <> ".") a <> f) <>
      case nub es of
        [e]    -> qss " = " <> e
        nubEs  -> qss " in (" <> mconcat (intersperse (qss ",") nubEs) <> qss ")"
    disjToQs' :: [QueryString] -> QueryString
    disjToQs' = reduce " or " "0"

mysqlModifyToQs :: ModifyStmt -> QueryString
mysqlModifyToQs stmt =
  case stmt of
    ExecUpdate table sets mkCond ->
        qss ("update " <> tableName table <> "\nset ") <>
        mconcat (intersperse (qss ", ") (map (\(f := v) -> qss (fieldName f <> " = ") <>
        mysqlExprToQs v) sets)) <> condQs
      where
        cond = mkCond (TableAlias Nothing)
        condQs
          | isTrue cond  = mempty
          | otherwise    = qss "\nwhere " <> mysqlExprToQs cond
    ExecInsert table sets ->
        qss ("insert into " <> tableName table <> " ") <>
        listQs (map qss fns) <> qss " values " <> listQs exprs
      where
        (fns, exprs) = unzip (map (\(f := v) -> (fieldName f, mysqlExprToQs v)) sets)
    ExecDelete table mkCond ->
        qss ("delete from " <> tableName table) <> condQs
      where
        cond = mkCond (TableAlias Nothing)
        condQs
          | isTrue cond  = mempty
          | otherwise    = qss "\nwhere " <> mysqlExprToQs cond
