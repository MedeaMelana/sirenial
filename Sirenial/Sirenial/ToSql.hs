{-# LANGUAGE GADTs #-}

module Sirenial.ToSql (
    -- * Executing queries
    runQuery, runSelectStmt,
    -- * Converting to query strings
    stmtToQs
  ) where

import Sirenial.Tables
import Sirenial.Expr
import Sirenial.Select
import Sirenial.QueryString

import Prelude hiding (concatMap, and, or, sequence_, concat)
import Data.Function
import Data.Either
import Data.Monoid
import qualified Data.List as L
import Data.Foldable
import qualified Data.Traversable as T
import Control.Applicative
import Control.Concurrent.MVar

import Database.HDBC


(<>) :: Monoid m => m -> m -> m
(<>) = mappend

-- | Run a single SELECT statement against the given connection.
runSelectStmt :: IConnection conn => conn -> SelectStmt a -> IO [a]
runSelectStmt c qss 
  | null (ssFroms qss)  = return [reify [] (ssResult qss) []]
  | otherwise         = do
      let result = ssResult qss
      let cols = selectedFields result
      let qs = stmtToQs qss
      let (sql, vs) = prepareQs qs
      putStrLn $ "*** Executing query:\n" <> renderQs qs
      stmt <- prepare c sql
      execute stmt vs
      rows <- fetchAllRows' stmt
      return (map (reify cols result) rows)

stmtsToQs :: [String] -> [(Maybe Int, String)] -> Expr Bool -> QueryString
stmtsToQs froms fields crit =
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
        else qss "\nwhere " <> exprToQs crit

reify :: [(Maybe Int, String)] -> Expr a -> [SqlValue] -> a
reify cols expr row = go expr
  where
    go :: Expr a -> a
    go expr =
      case expr of
        ExPure x     -> x
        ExApply f x  -> go f (go x)
        ExGet a f    ->
          case L.elemIndex (getAlias a, fieldName f) cols of
            Just index  -> fromSql (row !! index)
            Nothing     -> error "oops"
        ExEq x y     -> go x == go y
        ExLT x y     -> go x < go y
        ExAnd ps     -> and (map go ps)
        ExOr  ps     -> or (map go ps)
        ExLit x      -> x

data Pendulum where
  Pendulum :: SelectStmt a -> MVar [a] -> Pendulum

-- | Either a Query turns out to be a pure value, or it still has some queries to be executed.
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
        Just rs -> return $ Left (toList rs)

runManyStmts :: IConnection conn => conn -> [Pendulum] -> IO ()
runManyStmts conn ps = do
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
    fields = selectedFields (T.sequenceA $ map f ps)
    qs = stmtsToQs (froms (head ps)) fields (exprOr (map crit ps))
    (sql, vs) = prepareQs qs

progress :: IConnection conn => conn -> [Pendulum] -> IO ()
progress conn (p:ps') =
    case ps of
      [Pendulum q v] -> do
        res <- runSelectStmt conn q
        putMVar v res
      _ -> runManyStmts conn ps      
  where
    ps = p : filter ok ps'
    ok p' = froms p == froms p'
    froms (Pendulum qss _) = ssFroms qss

-- | Run a 'Query' computation against the specified connection.
runQuery :: IConnection conn => conn -> Query a -> IO a
runQuery conn qss = do
  ei <- collect qss
  case ei of
    Left v ->
      return v
    Right (qss', ps) -> do
      progress conn ps
      runQuery conn qss'
