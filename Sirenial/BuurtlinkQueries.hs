module BuurtlinkQueries where

import qualified BuurtlinkTables as Db

import Sirenial.Query

import Control.Applicative
import Data.Time.Calendar


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

selectAdById :: Ref Db.Ad -> ExecSelect Ad
selectAdById adId = do
  [ad] <- selectAds (\a -> a # Db.adId .==. expr adId)
  return ad

setAdStatus :: Ref Db.Ad -> String -> ModifyStmt
setAdStatus adId newStatus =
  ExecUpdate Db.tableAd $ \a ->
    ( [Db.adStatus := expr newStatus]
    , a # Db.adId .==. expr adId )
