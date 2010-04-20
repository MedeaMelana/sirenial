{-# LANGUAGE EmptyDataDecls #-}

module BuurtlinkTables where

import Sirenial.Tables

import Data.Time.Calendar


-- Table indices
data Ad
data AdWeek
data Town

-- Tables
tableAd      = Table "ads"        :: Table Ad
tableAdWeek  = Table "ads_weeks"  :: Table AdWeek
tableTown    = Table "towns"      :: Table Town

-- Primary keys
adId      = primKey tableAd
adWeekId  = primKey tableAdWeek
townId    = primKey tableTown

-- Other fields
adStatus        = Field      tableAd     "status"      :: Field Ad String
adWeekAdId      = foreignKey tableAdWeek "adId"        tableAd
adWeekTownId    = foreignKey tableAdWeek "townIdCopy"  tableTown
adWeekStartsOn  = Field      tableAdWeek "startsOn"    :: Field AdWeek Day
townName        = Field      tableTown   "townName"    :: Field Town String
townAdWeekPrice = Field      tableTown   "adWeekPrice" :: Field Town Int
