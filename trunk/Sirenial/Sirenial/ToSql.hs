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
import Debug.Trace


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

data Suspend a where
  SuPure    :: a -> Suspend a
  SuApply   :: Suspend (a -> b) -> Suspend a -> Suspend b
  SuSelect  :: SelectStmt a -> Maybe (MVar (Seq.Seq a)) -> ([a] -> Suspend b) -> Suspend b

instance Functor Suspend where
  fmap     = liftA

instance Applicative Suspend where
  pure     = SuPure
  (<*>)    = SuApply

instance Monad Suspend where
  return   = SuPure
  s >>= f  =
    case s of
      SuPure x -> f x
      SuApply sg sx -> do
        g <- sg
        x <- sx
        f (g x)
      SuSelect s v g -> SuSelect s v (\xs -> g xs >>= f)

suspend :: Merge a -> Suspend a
suspend m =
  case m of
    MePure x -> SuPure x
    MeApply f x -> SuApply (suspend f) (suspend x)
    MeSelect es ->
      let s = suspendES es
        -- in trace ("Suspending ES. Resulting Suspend size: " ++ show (size s)) s
        in s

suspendES :: ExecSelect a -> Suspend a
suspendES es =
  case es of
    EsReturn x -> SuPure x
    EsExecMany m -> suspend m -- trace ("Suspending Merge with size " ++ show (sizeMe m)) $ suspend m
    EsExec s -> SuSelect s Nothing SuPure
    EsBind (EsReturn x) f -> suspendES (f x)
    EsBind (EsExec s) f -> SuSelect s Nothing (suspendES . f)
    EsBind (EsExecMany m) f -> suspend m >>= suspendES . f
    EsBind es' f -> suspendES es' >>= suspendES . f

data Pendulum where
  Pendulum :: SelectStmt a -> MVar (Seq.Seq a) -> Pendulum

newtype CollectResult a = CollectResult (Either a (Suspend a, [Pendulum]))

instance Functor CollectResult where
  fmap f (CollectResult res) =
    CollectResult $ case res of
      Left x -> Left (f x)
      Right (s, ps) -> Right (fmap f s, ps)

collect :: Suspend a -> IO (CollectResult a)
collect s =
  case s of
    SuPure x -> return (CollectResult (Left x))
    SuApply sf sx -> do
      CollectResult cf <- collect sf
      CollectResult cx <- collect sx
      case (cf, cx) of
        (Left f, _) ->
          return (f <$> CollectResult cx)
        (Right _, Left x) ->
          return (($ x) <$> CollectResult cf)
        (Right (sf', fps), Right (sx', xps)) ->
          return $ CollectResult $ Right (sf' <*> sx', fps ++ xps)
    SuSelect s mv f -> do
      v <- case mv of
        Nothing  -> newEmptyMVar
        Just v   -> return v
      mrs <- tryTakeMVar v
      case mrs of
        Nothing ->
          return $ CollectResult $ Right $ (SuSelect s (Just v) f, [Pendulum s v])
        Just rs ->
          collect (f (F.toList rs))

progress :: IConnection conn => conn -> [Pendulum] -> IO ()
progress conn (p:ps') = do
    putStrLn $ "*** Merging " ++ show (length ps) ++ " queries. "
      ++ show (1 + length ps' - length ps) ++ " queries still pending."
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
  putStrLn $ "*** Running Suspend of size " ++ show (size s)
  CollectResult res <- collect s
  case res of
    Left x -> return x
    Right (s', ps) -> do
      progress conn ps
      runSuspend conn s'

size :: Suspend a -> Int
size s =
  case s of
    SuPure _ -> 0
    SuApply f x -> size f + size x
    SuSelect _ _ _ -> 1

sizeMe :: Merge a -> Int
sizeMe m =
  case m of
    MePure _ -> 0
    MeApply f x -> sizeMe f + sizeMe x
    MeSelect _ -> 1
