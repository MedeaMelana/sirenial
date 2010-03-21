{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}

module Sirenial.ToSql where

import Sirenial.Tables
import Sirenial.Expr
import Sirenial.Select

import Prelude hiding (concatMap, and, or, sequence_, concat)
import Data.Function
import Data.Either
import Data.Monoid
import Data.Foldable
import qualified Data.FMList as FM
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Sequence as Seq
import qualified Data.Traversable as T
import Control.Applicative
import Control.Concurrent.MVar
import Control.Arrow (first, second)
import Control.Monad (when)

import Database.HDBC


(<>) :: Monoid m => m -> m -> m
(<>) = mappend

selectedFields :: Expr a -> [(Int, String)]
selectedFields = L.nub . L.sort . go
  where
    go :: Expr a -> [(Int, String)]
    go expr =
      case expr of
        ExGet a f    -> [(getAlias a, fieldName f)]
        ExApply f x  -> go f <> go x
        ExEq x y     -> go x <> go y
        ExLT x y     -> go x <> go y
        ExAnd ps     -> concatMap go ps
        ExOr ps      -> concatMap go ps
        _            -> []

stmtToQs :: SelectStmt a -> QueryString
stmtToQs (SelectStmt froms crit result) =
    s "select " <> fieldsQs <> s "\nfrom " <> fromQs <> whereQs
  where
    fields = selectedFields result
    fieldsQs
      | null fields  = s "0"
      | otherwise    = s $ L.intercalate ", " $ map fn fields
    fn (alias, fieldName) = "t" <> show alias <> "." <> fieldName
    tn (t, i) = t <> " t" <> show (i :: Integer)
    fromQs = s $ L.intercalate ", "  $ map tn $ zip froms [0..]
    whereQs :: QueryString
    whereQs =
      case crit of
        ExAnd [] -> FM.empty
        _ -> do
          s "\nwhere " <> exprToQs crit

s :: String -> QueryString
s = FM.singleton . Left

exprToQs :: Expr a -> QueryString
exprToQs expr = case expr of
    ExPure _     -> error "Pure values cannot be converted to SQL."
    ExApply _ _  -> error "Pure values cannot be converted to SQL."
    ExGet a f    -> s $ "t" <> show (getAlias a) <> "." <> fieldName f
    ExEq x y     -> parens $ exprToQs x >> s " = " >> exprToQs y
    ExLT x y     -> parens $ exprToQs x >> s " < " >> exprToQs y
    ExAnd ps     -> reduce " and " "1" (map exprToQs ps)
    ExOr  ps     -> disjToQs ps
    ExLit x      -> FM.singleton $ Right (toSql x)

reduce :: String -> String -> [QueryString] -> QueryString
reduce sep zero exprs =
  case exprs of
    []      -> s zero
    [expr]  -> expr
    _       -> parens $ mconcat $ L.intersperse (s sep) exprs

parens :: QueryString -> QueryString
parens x = s "(" <> x <> s ")"

type QueryString = FM.FMList (Either String SqlValue)

eqFM :: Eq a => FM.FMList a -> FM.FMList a -> Bool
eqFM = (==) `on` FM.toList

prepareQs :: QueryString -> (String, [SqlValue])
prepareQs = foldMap f
  where
    f p = case p of
      Left s   -> (s,    [])
      Right v  -> ("?",  [v])

renderQs :: QueryString -> String
renderQs = concatMap $ \p ->
  case p of
    Left s -> s
    Right v -> "{" <> show v <> "}"

-- It'd be nice if we could perform this rewrite on Exprs directly, but we
-- don't have enough type information for that.
disjToQs :: [Expr Bool] -> QueryString
disjToQs = allToQs . first group . partitionEithers . map part
  where
    part :: Expr a -> Either ((Int, String), QueryString) QueryString
    part expr =
      case expr of
        ExEq (ExGet alias field) y  -> Left ((getAlias alias, fieldName field), exprToQs y)
        _                           -> Right (exprToQs expr)
    group :: Ord k => [(k, v)] -> [(k, [v])]
    group = M.toList . M.fromListWith (<>) . reverse . map (second (:[]))
    allToQs :: ([((Int, String), [QueryString])], [QueryString]) -> QueryString
    allToQs (gs, ss) = disjToQs' (map singleToQs gs <> ss)
    singleToQs :: ((Int, String), [QueryString]) -> QueryString
    singleToQs ((a, f), es) = s ("t" <> show a <> "." <> f) <>
      case L.nubBy eqFM es of
        [e]    -> s " = " <> e
        nubEs  -> s " in (" <> mconcat (L.intersperse (s ",") nubEs) <> s ")"
    disjToQs' :: [QueryString] -> QueryString
    disjToQs' = reduce " or " "0"

execSingle :: IConnection conn => conn -> SelectStmt a -> IO [a]
execSingle c s 
  | null (ssFroms s)  = return [reify [] (ssResult s) []]
  | otherwise         = do
      let result = ssResult s
      let cols = selectedFields result
      let qs = stmtToQs s
      let (sql, vs) = prepareQs qs
      putStrLn $ "*** Executing query:\n" <> renderQs qs
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
          case L.elemIndex (getAlias a, fieldName f) cols of
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
          Right (sf' <*> sx', fps <> xps)
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
        Just rs -> return $ Left (toList rs)

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
