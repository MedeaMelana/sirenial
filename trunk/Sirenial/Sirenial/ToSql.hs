{-# LANGUAGE GADTs #-}

module Sirenial.ToSql where

import Sirenial.Query

import Data.List
import Control.Applicative
import Control.Monad.State
import Control.Monad.Writer
import Control.Arrow

import Database.HDBC


selectedFields :: Expr a -> [(Int, String)]
selectedFields = nub . sort . go
  where
    go :: Expr a -> [(Int, String)]
    go expr =
      case expr of
        ExGet a f    -> [(getAlias a, fieldName f)]
        ExApply f x  -> go f ++ go x
        ExEq x y     -> go x ++ go y
        ExLT x y     -> go x ++ go y
        ExAnd x y    -> go x ++ go y
        _            -> []

stmtToSql :: SelectStmt a -> String
stmtToSql (SelectStmt froms wheres result) =
  execWriter (tellSelect (selectedFields result) froms wheres)

tellSelect :: [(Int, String)] -> [String] -> [Expr Bool] -> Writer String ()
tellSelect fields tables wheres = do
  tell "select "
  let fn (alias, fieldName) = "t" ++ show alias ++ "." ++ fieldName
  tell $ intercalate ", " $ map fn fields
  
  when (not (null tables)) $ do
    tell "\nfrom "
    let tn (t, i) = t ++ " t" ++ show (i :: Integer)
    tell $ intercalate ", "  $ map tn $ zip tables [0..]
  
  when (not (null wheres)) $ do
    tell $ "\nwhere "
    tellExpr (foldr1 (.&&.) wheres)

tellExpr :: Expr a -> Writer String ()
tellExpr = go
  where
    go :: Expr a -> Writer String ()
    go expr = case expr of
      ExPure _     -> error "Pure values cannot be converted to SQL."
      ExApply _ _  -> error "Pure values cannot be converted to SQL."
      ExGet a f    -> tell $ "t" ++ show (getAlias a) ++ "." ++ fieldName f
      ExEq x y     -> parens $ go x >> tell " = " >> go y
      ExLT x y     -> parens $ go x >> tell " < " >> go y
      ExAnd x y    -> parens $ go x >> tell " and " >> go y
      ExBool b     -> tell $ show $ (if b then 1 else 0 :: Int)
      ExString s   -> tell $ show s
      ExRef r      -> tell $ show r

parens :: Writer String () -> Writer String ()
parens x = tell "(" >> x >> tell ")"

printExec :: ExecSelect a -> IO a
printExec es =
  case es of
    EsReturn x -> return x
    EsBind mx f -> do
      x <- printExec mx
      printExec (f x)
    EsExec s -> do
      putStrLn (stmtToSql (toStmt s))
      return []
    EsExecMany m -> do
      printMerge m

printMerge :: Merge a -> IO a
printMerge m =
  case m of
    MePure x -> pure x
    MeApply f x -> printMerge f <*> printMerge x
    MeSelect s -> printExec s

go :: IConnection conn => conn -> SelectStmt a -> IO [a]
go c s = do
  let result = ssResult s
  let cols = selectedFields result
  stmt <- prepare c (stmtToSql s)
  execute stmt []
  rows <- fetchAllRows' stmt
  return (map (reify cols result) rows)

reify :: [(Int, String)] -> Expr a -> [SqlValue] -> a
reify cols expr row = go expr
  where
    go :: Expr a -> a
    go expr =
      case expr of
        ExPure x     -> x
        ExApply f x  -> go f (go x)
        ExGet a f    ->
          case elemIndex (getAlias a, fieldName f) cols of
            Just index  -> fromSql (row !! index)
            Nothing     -> error "oops"
        ExEq x y     -> go x == go y
        ExLT x y     -> go x < go y
        ExAnd x y    -> go x && go y
        ExBool b     -> b
        ExString s   -> s
        ExRef r      -> r
