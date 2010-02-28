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

import Database.HDBC.MySQL


data Ad = Ad
  { adId      :: Ref Db.Ad
  , adStatus  :: String
  , adWeeks   :: [AdWeek]
  }

data AdWeek = AdWeek
  { adWeekStartsOn :: Day
  , adWeekTownId   :: Ref Db.Town
  }

selectAds :: (TableAlias Db.Ad -> Expr Bool) -> ExecSelect [Ad]
selectAds p = do
  -- Query Db.tableAd
  ads <- execSelect $ do
    a <- from Db.tableAd
    restrict (p a)
    return $ Ad <$> a # Db.adId <*> a # Db.adStatus <*> pure []

  -- Per ad, query Db.tableAdWeek to fill the adWeeks field
  for ads $ \ad -> do
    if adStatus ad == "confirmed"
      then do
        weeks <- execSelect $ do
          aw <- from Db.tableAdWeek
          restrict $ aw # Db.adWeekAdId .==. expr (adId ad)
          return $ AdWeek <$> aw # Db.adWeekStartsOn <*> aw # Db.adWeekTownId
        return (ad { adWeeks = weeks })
      else do
        return ad

selectAllAds :: ExecSelect [Ad]
selectAllAds = selectAds (\_ -> expr True)

selectAdById :: Ref Db.Ad -> ExecSelect (Maybe Ad)
selectAdById adId = listToMaybe <$> selectAds (\a -> a # Db.adId .==. expr adId)

setAdStatus :: Ref Db.Ad -> String -> ModifyStmt
setAdStatus adId newStatus =
  ExecUpdate Db.tableAd $ \a ->
    ( [Db.adStatus := expr newStatus]
    , a # Db.adId .==. expr adId )

qAdIds :: Select (Expr (Ref Db.Ad, String))
qAdIds = do
  a <- from Db.tableAd
  restrict (a # Db.adId .<. expr 1000)
  -- restrict (a # Db.adStatus .==. expr "reserved")
  return $ (,) <$> a # Db.adId <*> a # Db.adStatus

test :: IO ()
test = do
  conn <- connectMySQL defaultMySQLConnectInfo
    { mysqlUnixSocket = "/var/run/mysqld/mysqld.sock"
    , mysqlUser = "root"
    , mysqlDatabase = "buurtlink"
    }
  let stmt = toStmt qAdIds
  print (stmtToSql stmt)
  ids <- go conn stmt
  print ids
