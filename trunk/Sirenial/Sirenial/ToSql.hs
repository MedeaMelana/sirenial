{-# LANGUAGE GADTs #-}

module Sirenial.ToSql where

import Sirenial.Tables
import Sirenial.Expr
import Sirenial.Select

import Data.List
import Control.Applicative
import Control.Monad.Writer

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
stmtToSql (SelectStmt froms crit result) =
  execWriter (tellSelect (selectedFields result) froms crit)

tellSelect :: [(Int, String)] -> [String] -> Expr Bool -> Writer String ()
tellSelect fields tables crit = do
  tell "select "
  if null fields
    then tell "0"
    else do
      let fn (alias, fieldName) = "t" ++ show alias ++ "." ++ fieldName
      tell $ intercalate ", " $ map fn fields
  
  when (not (null tables)) $ do
    tell "\nfrom "
    let tn (t, i) = t ++ " t" ++ show (i :: Integer)
    tell $ intercalate ", "  $ map tn $ zip tables [0..]
  
  tell "\nwhere "
  tellExpr crit

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
      ExOr  x y    -> parens $ go x >> tell " or " >> go y
      ExBool b     -> tell $ show $ (if b then 1 else 0 :: Int)
      ExString s   -> tell $ show s
      ExRef r      -> tell $ show r

parens :: Writer String () -> Writer String ()
parens x = tell "(" >> x >> tell ")"

execExec :: IConnection conn => conn -> ExecSelect a -> IO a
execExec c es =
  case es of
    EsReturn x -> return x
    EsBind mx f -> do
      x <- execExec c mx
      execExec c (f x)
    EsExec s -> execSingle c s
    EsExecMany m -> execMerge c m

execMerge :: IConnection conn => conn -> Merge a -> IO a
execMerge c m =
  case m of
    MePure x -> pure x
    MeApply f x -> execMerge c f <*> execMerge c x
    MeSelect s -> execExec c s

execSingle :: IConnection conn => conn -> SelectStmt a -> IO [a]
execSingle c s = do
  let result = ssResult s
  let cols = selectedFields result
  let sql = stmtToSql s
  putStrLn $ "*** Executing query:\n" ++ sql
  stmt <- prepare c sql
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
        ExOr x y     -> go x || go y
        ExBool b     -> b
        ExString s   -> s
        ExRef r      -> r

combine :: SelectStmt a -> SelectStmt b -> Maybe (ExecSelect ([a], [b]))
combine (SelectStmt ts1 c1 r1) (SelectStmt ts2 c2 r2)
  -- We can combine two queries if they select from the same tables.
  | ts1 == ts2  = Just $ do
      -- To be able to distinguish between the results afterwards, we return for
      -- each result whether to include it in either query's result.
      rows <- EsExec $ SelectStmt
        { ssFroms   = ts1
        , ssCrit    = c1 .||. c2
        , ssResult  = (,,,) <$> r1 <*> c1 <*> r2 <*> c2
        }
      return ( [x | (x, True, _, _   ) <- rows]
             , [y | (_, _   , y, True) <- rows]
             )
  | otherwise   = Nothing
