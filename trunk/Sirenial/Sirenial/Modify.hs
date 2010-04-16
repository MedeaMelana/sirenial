{-# LANGUAGE GADTs #-}

module Sirenial.Modify where

import Sirenial.Tables
import Sirenial.Expr
import Sirenial.QueryString

import Data.Monoid

import Database.HDBC
import Data.List (intersperse)


-- | An SQL modification statement.
data ModifyStmt where
  ExecUpdate  :: Table t -> [SetField t] -> (TableAlias t -> Expr Bool) -> ModifyStmt
  ExecInsert  :: Table t -> [SetField t] ->                                ModifyStmt
  ExecDelete  :: Table t ->                 (TableAlias t -> Expr Bool) -> ModifyStmt

-- | Set a table field to a particular value.
data SetField t where
  (:=) :: Field t a -> Expr a -> SetField t

execModifyStmt :: IConnection conn => conn -> ModifyStmt -> IO ()
execModifyStmt conn stmt = do
  let qs = stmtToQs stmt
  let (sql, vs) = prepareQs qs
  stmt <- prepare conn sql
  execute stmt vs
  finish stmt

(<>) :: Monoid m => m -> m -> m
(<>) = mappend

stmtToQs :: ModifyStmt -> QueryString
stmtToQs stmt =
  case stmt of
    ExecUpdate table sets mkCond ->
        qss ("update " <> tableName table <> "\nset ") <>
        mconcat (intersperse (qss ", ") (map (\(f := v) -> qss (fieldName f <> " = ") <>
        exprToQs v) sets)) <> condQs
      where
        cond = mkCond (TableAlias Nothing)
        condQs
          | isTrue cond  = mempty
          | otherwise    = qss "\nwhere " <> exprToQs cond
    ExecInsert table sets ->
        qss ("insert into " <> tableName table <> " ") <>
        listQs (map qss fns) <> qss " values " <> listQs exprs
      where
        (fns, exprs) = unzip (map (\(f := v) -> (fieldName f, exprToQs v)) sets)
    ExecDelete table mkCond ->
        qss ("delete from " <> tableName table) <> condQs
      where
        cond = mkCond (TableAlias Nothing)
        condQs
          | isTrue cond  = mempty
          | otherwise    = qss "\nwhere " <> exprToQs cond
