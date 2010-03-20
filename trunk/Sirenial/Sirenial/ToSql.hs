{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}

module Sirenial.ToSql where

import Sirenial.Tables
import Sirenial.Expr
import Sirenial.Select

import Data.Function
import Data.List
import Data.Either
import Control.Arrow (first, second)
import qualified Data.Map as M
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
        ExAnd ps     -> concatMap go ps
        ExOr ps      -> concatMap go ps
        _            -> []

stmtToSql :: SelectStmt a -> QueryString
stmtToSql (SelectStmt froms crit result) =
  execWriter (tellSelect (selectedFields result) froms crit)

tellS :: String -> Writer QueryString ()
tellS s = tell [Left s]

tellSelect :: [(Int, String)] -> [String] -> Expr Bool -> Writer QueryString ()
tellSelect fields tables crit = do
  tellS "select "
  if null fields
    then tellS "0"
    else do
      let fn (alias, fieldName) = "t" ++ show alias ++ "." ++ fieldName
      tellS $ intercalate ", " $ map fn fields

  -- Guaranteed to be selecting from at least one table.
  tellS "\nfrom "
  let tn (t, i) = t ++ " t" ++ show (i :: Integer)
  tellS $ intercalate ", "  $ map tn $ zip tables [0..]
  
  case crit of
    ExAnd [] -> return ()
    _ -> do
      tellS "\nwhere "
      tellExpr crit

tellExpr :: Expr a -> Writer QueryString ()
tellExpr expr = case expr of
    ExPure _     -> error "Pure values cannot be converted to SQL."
    ExApply _ _  -> error "Pure values cannot be converted to SQL."
    ExGet a f    -> tell [Left $ "t" ++ show (getAlias a) ++ "." ++ fieldName f]
    -- ExInList x ys  -> parens $ tellIn x ys
    ExEq x y     -> parens $ tellExpr x >> tellS " = " >> tellExpr y
    ExLT x y     -> parens $ tellExpr x >> tellS " < " >> tellExpr y
    ExAnd ps     -> reduce " and " "1" ps
    ExOr  ps     -> tell (disjToQs ps)
    ExLit x      -> tell [Right (toSql x)]
    -- ExString s   -> tell $ show s
    -- ExRef r      -> tell $ show r

reduce :: String -> String -> [Expr a] -> Writer QueryString ()
reduce sep zero exprs =
  case exprs of
    []      -> tellS zero
    [expr]  -> tellExpr expr
    _       -> parens $ sequence_ $ intersperse (tellS sep) (map tellExpr exprs)

reduce' :: String -> String -> [QueryString] -> Writer QueryString ()
reduce' sep zero exprs =
  case exprs of
    []      -> tellS zero
    [expr]  -> tell expr
    _       -> parens $ sequence_ $ intersperse (tellS sep) (map tell exprs)

parens :: Writer QueryString () -> Writer QueryString ()
parens x = tellS "(" >> x >> tellS ")"

type QueryString = [Either String SqlValue]

prepareQs :: QueryString -> (String, [SqlValue])
prepareQs = F.foldMap f
  where
    f p = case p of
      Left s   -> (s,    [])
      Right v  -> ("?",  [v])

renderQs :: QueryString -> String
renderQs = concatMap $ \p ->
  case p of
    Left s -> s
    Right v -> "{" ++ show v ++ "}"

-- It'd be nice if we could perform this rewrite on Exprs directly, but we
-- don't have enough type information for that.
disjToQs :: [Expr Bool] -> QueryString
disjToQs exprs = allToQs $ first group $ partitionEithers $ map part exprs
  where
    part :: Expr a -> Either ((Int, String), QueryString) QueryString
    part expr =
      case expr of
        ExEq (ExGet alias field) y  -> Left ((getAlias alias, fieldName field), exprToQs y)
        _                           -> Right (exprToQs expr)
    group :: [((Int, String), QueryString)] -> [((Int, String), [QueryString])]
    group = M.toList . M.fromListWith (++) . map (second (:[]))
    allToQs :: ([((Int, String), [QueryString])], [QueryString]) -> QueryString
    allToQs (gs, ss) = disjToQs' (map singleToQs gs ++ ss)
    singleToQs :: ((Int, String), [QueryString]) -> QueryString
    singleToQs ((a, f), es) = [Left $ "t" ++ show a ++ "." ++ f] ++
      case nub es of
        [e]    -> [Left " = "] ++ e
        nubEs  -> [Left $ " in ("] ++ intercalate [Left ","] nubEs ++ [Left ")"]
    disjToQs' :: [QueryString] -> QueryString
    disjToQs' = execWriter . reduce' " or " "0"

exprToQs :: Expr a -> QueryString
exprToQs = execWriter . tellExpr

execSingle :: IConnection conn => conn -> SelectStmt a -> IO [a]
execSingle c s 
  | null (ssFroms s)  = return [reify [] (ssResult s) []]
  | otherwise         = do
      let result = ssResult s
      let cols = selectedFields result
      let qs = stmtToSql s
      let (sql, vs) = prepareQs qs
      putStrLn $ "*** Executing query:\n" ++ renderQs qs
      stmt <- prepare c sql
      execute stmt vs
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
        ExAnd ps     -> and (map go ps)
        ExOr  ps     -> or (map go ps)
        ExLit x      -> x

data Pendulum where
  Pendulum :: SelectStmt a -> MVar (Seq.Seq a) -> Pendulum

-- | Either a Query turns out to be a pure value, or it still has some queries to be executed.
collect :: Query a -> IO (Either a (Query a, [Pendulum]))
collect s =
  case s of
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
          Right (sf' <*> sx', fps ++ xps)
    QuBind sx f -> do
      cx <- collect sx
      case cx of
        Left x -> collect (f x)
        Right (sx', xps)  -> return $ Right (sx' >>= f, xps)
    QuSelect s mv -> do
      v <- case mv of
        Nothing  -> newEmptyMVar
        Just v   -> return v
      mrs <- tryTakeMVar v
      case mrs of
        Nothing -> return $ Right (QuSelect s (Just v), [Pendulum s v])
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

runQuery :: IConnection conn => conn -> Query a -> IO a
runQuery conn s = do
  ei <- collect s
  case ei of
    Left v ->
      return v
    Right (s', ps) -> do
      progress conn ps
      runQuery conn s'
