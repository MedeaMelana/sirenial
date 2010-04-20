{-# LANGUAGE GADTs #-}

module Sirenial.Modify (ModifyStmt(..), SetField(..)) where

import Sirenial.Tables
import Sirenial.Expr


-- | An SQL modification statement.
data ModifyStmt where
  ExecUpdate  :: Table t -> [SetField t] -> (TableAlias t -> Expr Bool) -> ModifyStmt
  ExecInsert  :: Table t -> [SetField t] ->                                ModifyStmt
  ExecDelete  :: Table t ->                 (TableAlias t -> Expr Bool) -> ModifyStmt

-- | Set a table field to a particular value.
data SetField t where
  (:=) :: Field t a -> Expr a -> SetField t
