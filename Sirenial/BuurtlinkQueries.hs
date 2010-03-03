{-# LANGUAGE RankNTypes #-}

module BuurtlinkQueries where

import qualified BuurtlinkTables as Db

import Sirenial.Tables
import Sirenial.Expr
import Sirenial.Select
import Sirenial.Modify
import Sirenial.ToSql

import Data.Maybe
import Data.Time.Calendar
import Control.Applicative

import Database.HDBC
import Database.HDBC.MySQL


data Ad = Ad
    { adId      :: Ref Db.Ad
    , adStatus  :: String
    , adWeeks   :: [AdWeek]
    }
  deriving Show

data AdWeek = AdWeek
    { adWeekStartsOn  :: Day
    , adWeekTownName  :: String
    }
  deriving Show

selectAds :: (TableAlias Db.Ad -> Expr Bool) -> Suspend [Ad]
selectAds p = do
  -- Query Db.tableAd
  ads <- execSelect $ do
    a <- from Db.tableAd
    restrict (p a)
    return $ Ad <$> a # Db.adId <*> a # Db.adStatus <*> pure []

  -- For each matching ad ...
  for ads $ \ad -> do
    -- ... if the ad has been paid for ...
    if adStatus ad == "confirmed"
      then do
        -- ... fetch the accompanying weeks.
        weeks <- getAdWeeks (adId ad)
        return (ad { adWeeks = weeks })
      else do
        return ad

-- | Retrieve the weeks accompanying an ad.
getAdWeeks :: Ref Db.Ad -> Suspend [AdWeek]
getAdWeeks adId = do
  weeks <- execSelect $ do
    aw <- from Db.tableAdWeek
    restrict $ aw # Db.adWeekAdId .==. expr adId
    return $ (,) <$> aw # Db.adWeekStartsOn <*> aw # Db.adWeekTownId
  
  -- Fetch town names for weeks
  for weeks $ \(startsOn, townId) ->
    AdWeek startsOn <$> getTownName townId

-- | Retrieve the town name, given a town ID.
getTownName :: Ref Db.Town -> Suspend String
getTownName townId = do
  [townName] <- execSelect $ do
    t <- from Db.tableTown
    restrict (t # Db.townId .==. expr townId)
    return (t # Db.townName)
  return townName

getAdWeekPrice :: Ref Db.Town -> Suspend Int
getAdWeekPrice townId = do
  querySingle $ do
    t <- from Db.tableTown
    restrict (t # Db.townId .==. expr townId)
    return (t # Db.townAdWeekPrice)

querySingle :: Select (Expr a) -> Suspend a
querySingle stmt = do
  [v] <- execSelect stmt
  return v

selectAllAds :: Suspend [Ad]
selectAllAds = selectAds (\_ -> expr True)

selectAdById :: Ref Db.Ad -> Suspend (Maybe Ad)
selectAdById adId = listToMaybe <$> selectAds (\a -> a # Db.adId .==. expr adId)

setAdStatus :: Ref Db.Ad -> String -> ModifyStmt
setAdStatus adId newStatus =
  ExecUpdate Db.tableAd $ \a ->
    ( [Db.adStatus := expr newStatus]
    , a # Db.adId .==. expr adId )

qAdIds :: (TableAlias Db.Ad -> Expr Bool) -> Select (Expr (Ref Db.Ad, String))
qAdIds f = do
  a <- from Db.tableAd
  restrict (f a)
  return $ (,) <$> a # Db.adId <*> a # Db.adStatus

qAdStatus :: Ref Db.Ad -> SelectStmt String
qAdStatus adId = toStmt $ do
  a <- from Db.tableAd
  restrict (a # Db.adId .==. expr adId)
  return (a # Db.adStatus)

-- mergeTest :: Suspend ([String], [String])
-- mergeTest = (,) <$> MeSelect (EsExec (qAdStatus 1)) <*> MeSelect (EsExec (qAdStatus 2))

withConn :: (forall conn. IConnection conn => conn -> IO a) -> IO a
-- withConn = undefined
withConn f = do
  conn <- connectMySQL defaultMySQLConnectInfo
    { mysqlUnixSocket = "/var/run/mysqld/mysqld.sock"
    , mysqlUser = "root"
    , mysqlDatabase = "buurtlink"
    }
  f conn

test :: IO ([Maybe Ad])
test = withConn $ \c -> runSuspend c $ for [996,997] selectAdById

test2 = withConn $ \c -> runSuspend c $ execSelect $ qAdIds (\_ -> expr True)

bla :: Suspend ()
bla = ((() <$ execSelect q) <* execSelect q)
    >>= \_ -> return ()
  where
    q = return (pure ())

