{-# LANGUAGE GADTs #-}

module Sirenial.ToSql where

import Sirenial.Tables
import Sirenial.Expr
import Sirenial.Select

import Data.Function
import Data.List
import qualified Data.Sequence as Seq
import qualified Data.Traversable as T
import qualified Data.Foldable as F
import Control.Applicative
import Control.Monad.Writer
import Control.Concurrent.MVar

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

  -- Guaranteed to be selecting from at least one table.
  tell "\nfrom "
  let tn (t, i) = t ++ " t" ++ show (i :: Integer)
  tell $ intercalate ", "  $ map tn $ zip tables [0..]
  
  case crit of
    ExBool True -> return ()
    _ -> do
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

execSingle :: IConnection conn => conn -> SelectStmt a -> IO [a]
execSingle c s 
  | null (ssFroms s)  = return [reify [] (ssResult s) []]
  | otherwise         = do
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


data Pendulum where
  Pendulum :: SelectStmt a -> MVar (Seq.Seq a) -> Pendulum

-- | Either a Suspend turns out to be a pure value, or it still has some queries to be executed.
collect :: Suspend a -> IO (Either a (Suspend a, [Pendulum]))
collect s =
  case s of
    SuPure x -> return (Left x)
    SuApply sf sx -> do
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
          Right (sf' <*> sx', fps ++ xps)
    SuBind sx f -> do
      cx <- collect sx
      case cx of
        Left x -> collect (f x)
        Right (sx', xps)  -> return $ Right (sx' >>= f, xps)
    SuSelect s mv -> do
      v <- case mv of
        Nothing  -> newEmptyMVar
        Just v   -> return v
      mrs <- tryTakeMVar v
      case mrs of
        Nothing -> return $ Right (SuSelect s (Just v), [Pendulum s v])
        Just rs -> return $ Left (F.toList rs)

progress :: IConnection conn => conn -> [Pendulum] -> IO ()
progress conn (p:ps') = do
    T.for ps $ \(Pendulum _ v) -> putMVar v Seq.empty
    let stmt = SelectStmt
          { ssFroms   = froms p
          , ssCrit    = exprOr (map crit ps)
          , ssResult  = T.for ps $ \(Pendulum s v) -> update v <$> ssCrit s <*> ssResult s
          }
    actions <- execSingle conn stmt
    sequence_ (concat actions)
  where
    ps = p : filter ok ps'
    ok p' = froms p == froms p'
    froms (Pendulum s _) = ssFroms s
    crit (Pendulum s _) = ssCrit s
    update v b r =
      when b $ modifyMVar_ v (\rs -> return (rs Seq.|> r))

runSuspend :: IConnection conn => conn -> Suspend a -> IO a
runSuspend conn s = do
  ei <- collect s
  case ei of
    Left v ->
      return v
    Right (s', ps) -> do
      progress conn ps
      runSuspend conn s'
