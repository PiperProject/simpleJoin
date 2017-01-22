#!/usr/bin/env python

# -------------------------------------- #
import os, sys

# import sibling packages HERE!!!
adaptersPath  = os.path.abspath( __file__ + "/../../../../adapters" )
sys.path.append( adaptersPath )
from adapters import Adapter

# settings dir
settingsPath  = os.path.abspath( __file__ + "/../../core" )
sys.path.append( settingsPath )
import settings

# -------------------------------------- #


DEBUG      = settings.DEBUG
NOSQL_TYPE = settings.NOSQL_TYPE


################
#  DICT MERGE  #
################
def dictMerge( a, b ) :
  c = a.copy()
  c = c.update(b)
  return c


#################
#  SIMPLE JOIN  #
#################
def simpleJoin( cursor, idLists, joinAttr, pred ) :

  ad = Adapter.Adapter( NOSQL_TYPE )

  # assume all ids in db are unique
  idDict = {}
  for currList in idLists :
    for currID in l :
      res1   = ad.get( currID, cursor )
      atddtVal = res1[ joinAttr ]
      idDict[ currID ] = attVal

  # get ids with identical joinAttr
  targetIDs = []
  for d1 in idDict :
    newkey = idDict[ d1 ]
    targetIDs.append( d1 )
    for d2 in idDict :
      if not d1 == d2 :
        if idDict[d2] == newKey :
          targetIDs.append( d2 )

  # grab all vals per joined id
  currResDictList = []
  for i in targetIDs :
    res = ad.get( i, cursor )
    currResDictList.append( res )

  return currResDict
