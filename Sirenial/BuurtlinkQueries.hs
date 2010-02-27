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
  ads <- execSelect $ do
    a <- from Db.tableAd
    return $ SelectStmt
      { ssResult = Ad <$> a # Db.adId <*> a # Db.adStatus <*> pure []
      , ssWhere  = f a
      }
  for ads $ \ad -> do
    weeks <- execSelect $ do
      aw <- from Db.tableAdWeek
      return $ SelectStmt
        { ssResult  = AdWeek <$> aw # Db.adWeekStartsOn <*> aw # Db.adWeekTownId
        , ssWhere   = aw # Db.adWeekAdId `ExEq` ExRef (adId ad)
        }
    return (ad { adWeeks = weeks })

selectAllAds :: ExecSelect [Ad]
selectAllAds = selectAds (\_ -> ExBool True)

selectAdById :: Ref Db.Ad -> ExecSelect Ad
selectAdById adId = do
  [ad] <- selectAds (\a -> a # Db.adId `ExEq` ExRef adId)
  return ad
