{-# LANGUAGE EmptyDataDecls #-}

module BuurtlinkTables where

import Sirenial.Tables

-- Table indices
data Ad
data AdWeek
data Town

-- Tables
tableAd      = Table "ads"        adId      :: Table Ad
tableAdWeek  = Table "ads_weeks"  adWeekId  :: Table AdWeek
tableTown    = Table "towns"      townId    :: Table Town

-- Primary keys
adId      = primKey tableAd
adWeekId  = primKey tableAdWeek
townId    = primKey tableTown

-- Other fields
adStatus        = Field      tableAd     "status"      TyString
adWeekAdId      = foreignKey tableAdWeek "adId"        tableAd
adWeekTownId    = foreignKey tableAdWeek "townIdCopy"  tableTown
adWeekStartsOn  = Field      tableAdWeek "startsOn"    TyDay
townName        = Field      tableTown   "townName"    TyString
townAdWeekPrice = Field      tableTown   "adWeekPrice" TyInt
