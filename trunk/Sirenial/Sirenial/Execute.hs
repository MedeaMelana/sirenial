{-# LANGUAGE GADTs #-}

module Sirenial.Execute (execModifyStmt, runSelectStmt, runQuery) where

import Sirenial.SqlGen
import Sirenial.Expr
import Sirenial.Select
import Sirenial.Modify
import Sirenial.QueryString
import Sirenial.Util
import Sirenial.Tables

import Data.List
import Data.Traversable
import Control.Applicative
import Control.Monad
import Control.Concurrent.MVar

import Database.HDBC


-- | Run a modification statement.
execModifyStmt :: IConnection conn => conn -> SqlGen -> ModifyStmt -> IO ()
execModifyStmt conn gen stmt = do
  let qs = modifyToQs gen stmt
  let (sql, vs) = prepareQs qs
  stmt <- prepare conn sql
  execute stmt vs
  finish stmt

-- | Run a single SELECT statement.
runSelectStmt :: IConnection conn => conn -> SqlGen -> SelectStmt a -> IO [a]
runSelectStmt c gen (SelectStmt froms crit res) 
  | null froms  = return [reify [] res []]
  | otherwise   = do
      let cols = selectedFields res
      let qs = selectToQs gen froms (selectedFields res) crit
      let (sql, vs) = prepareQs qs
      putStrLn $ "*** Executing query:\n" <> renderQs qs
      stmt <- prepare c sql
      execute stmt vs
      rows <- fetchAllRows' stmt
      return (map (reify cols res) rows)

-- | Given a list of selected columns and an expression, evaluate it using the
-- supplied SQL values.
reify :: [(Int, String)] -> Expr a -> [SqlValue] -> a
reify cols expr row = go expr
  where
    go :: Expr a -> a
    go expr =
      case expr of
        ExPure x     -> x
        ExApply f x  -> go f (go x)
        ExGet (TableAlias (Just a)) f    ->
          case elemIndex (a, fieldName f) cols of
            Just index  -> fromSql (row !! index)
            Nothing     -> error "oops"
        ExEq x y     -> go x == go y
        ExLT x y     -> go x < go y
        ExAnd ps     -> and (map go ps)
        ExOr  ps     -> or (map go ps)
        ExLit x      -> x

-- | Run a 'Query' computation, merging queries whenever possible.
runQuery :: IConnection conn => conn -> SqlGen -> Query a -> IO a
runQuery conn gen qss = do
  ei <- collect qss
  case ei of
    Left v ->
      return v
    Right (qss', ps) -> do
      progress conn gen ps
      runQuery conn gen qss'

-- | Either a Query turns out to be a pure value, or it still has some queries
-- to be executed.
collect :: Query a -> IO (Either a (Query a, [Pendulum]))
collect qss =
  case qss of
    QuPure x -> return (Left x)
    QuApply sf sx -> do
      cf <- collect sf
      cx <- collect sx
      return $ case (cf, cx) of
        (Left f, Left x) ->
          Left (f x)
        (Left f, Right (sx', xps)) ->
          Right (f <$> sx', xps)
        (Right (sf', fps), Left x) ->
          Right (($ x) <$> sf', fps)
        (Right (sf', fps), Right (sx', xps)) ->
          Right (sf' <*> sx', fps <> xps)
    QuBind sx f -> do
      cx <- collect sx
      case cx of
        Left x -> collect (f x)
        Right (sx', xps)  -> return $ Right (sx' >>= f, xps)
    QuSelect qss mv -> do
      v <- case mv of
        Nothing  -> newEmptyMVar
        Just v   -> return v
      mrs <- tryTakeMVar v
      case mrs of
        Nothing -> return $ Right (QuSelect qss (Just v), [Pendulum qss v])
        Just rs -> return $ Left rs

-- | Send exactly one statement to the database, executing at least one
-- pending query (more if any could be merged.)
progress :: IConnection conn => conn -> SqlGen -> [Pendulum] -> IO ()
progress conn gen (p:ps') =
    case ps of
      [Pendulum q v] -> do
        res <- runSelectStmt conn gen q
        putMVar v res
      _ -> runManyStmts conn gen ps      
  where
    ps = p : filter ok ps'
    ok p' = froms p == froms p'  -- two queries can be merged if
                                 -- their FROMs are equal
    froms (Pendulum qss _) = ssFroms qss

-- | A SELECT statement waiting to be executed. The results are to be put in
-- the MVar.
data Pendulum where
  Pendulum :: SelectStmt a -> MVar [a] -> Pendulum

-- | Execute all the pending queries. They are guaranteed to be selecting from
-- the same table(s), so they can be merged and only one statement is sent to
-- the database.
runManyStmts :: IConnection conn => conn -> SqlGen -> [Pendulum] -> IO ()
runManyStmts conn gen ps = do
    putStrLn $ "*** Executing query:\n" <> renderQs qs
    stmt <- prepare conn sql
    execute stmt vs
    rows <- fetchAllRows' stmt
    forM_ ps $ \(Pendulum st v) ->
      putMVar v [ reify fields (ssResult st) row
                | row <- rows
                , reify fields (ssCrit st) row
                ]
  where
    froms (Pendulum qss _) = ssFroms qss
    crit  (Pendulum qss _) = ssCrit qss
    f (Pendulum st _) = () <$ ssCrit st <* ssResult st
    fields = selectedFields (sequenceA $ map f ps)
    qs = selectToQs gen (froms (head ps)) fields (exprOr (map crit ps))
    (sql, vs) = prepareQs qs
