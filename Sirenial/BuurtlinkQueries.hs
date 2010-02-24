module BuurtlinkQueries where

import qualified BuurtlinkTables as Db

import Sirenial.Query
import Sirenial.Merge

import Control.Applicative
import Data.Traversable
import Data.Time.Calendar


data RenderAd = RenderAd
  { rAdId     :: Int
  , rAdWeeks  :: [RenderAdWeek]
  }

data RenderAdWeek = RenderAdWeek
  { rAdWeekStartsOn :: Day
  , rAdWeekTownId   :: Int
  }

type S a = Select (SelectStmt a)

selectAllAds :: S (Ref Db.Ad, Day)
selectAllAds = do
  a   <- from Db.tableAd
  aw  <- leftJoin Db.adWeekAdId (a # Db.adId)
  returnAll ((,) <$> (a # Db.adId) <*> aw # Db.adWeekStartsOn)

selectAdById :: Int -> S (Ref Db.Ad, Day)
selectAdById adId = do
  a   <- from Db.tableAd
  aw  <- leftJoin Db.adWeekAdId (a # Db.adId)
  return $ SelectStmt
    { ssResult = (,) <$> (a # Db.adId) <*> aw # Db.adWeekStartsOn
    , ssWhere  = (a # Db.adId) `ExEq` ExRef adId
    }

selectAdsByIds :: [Int] -> Merge [(Ref Db.Ad, Day)]
selectAdsByIds adIds = concat <$> sequenceA (map (select . selectAdById) adIds)
