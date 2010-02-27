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
selectAds f = do
  -- Query Db.tableAd
  ads <- execSelect $ do
    a <- from Db.tableAd
    restrict (f a)
    return $ Ad <$> a # Db.adId <*> a # Db.adStatus <*> pure []

  -- Per ad, query Db.tableAdWeek
  for ads $ \ad -> do
    weeks <- execSelect $ do
      aw <- from Db.tableAdWeek
      restrict $ aw # Db.adWeekAdId `ExEq` ExRef (adId ad)
      return $ AdWeek <$> aw # Db.adWeekStartsOn <*> aw # Db.adWeekTownId
    return (ad { adWeeks = weeks })

selectAllAds :: ExecSelect [Ad]
selectAllAds = selectAds (\_ -> ExBool True)

selectAdById :: Ref Db.Ad -> ExecSelect Ad
selectAdById adId = do
  [ad] <- selectAds (\a -> a # Db.adId `ExEq` ExRef adId)
  return ad
