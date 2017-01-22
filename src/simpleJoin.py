#!/usr/bin/env python

# -------------------------------------- #
import os, sys

# import sibling packages HERE!!!
adaptersPath  = os.path.abspath( __file__ + "/../../../../adapters" )
sys.path.append( adaptersPath )
#from adapters import Adapter
import Adapter

# settings dir
settingsPath  = os.path.abspath( __file__ + "/../../core" )
sys.path.append( settingsPath )
import settings

# -------------------------------------- #


DEBUG      = settings.DEBUG

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
def simpleJoin( nosql_type, cursor, idLists, joinAttr, pred ) :

  ad = Adapter.Adapter( nosql_type )

  # assume all ids in db are unique
  # ids per joinAttr
  idDict = {}
  for currList in idLists :
    for currID in currList :
      res1   = ad.get( currID, cursor )
      attVal = res1[ joinAttr ]
      idDict[ currID ] = attVal

  if DEBUG :
    print "idDict = " + str(idDict)

  # get ids with identical joinAttr
  targetIDs = []
  for k1 in idDict :
    att = idDict[ k1 ]
    for k2 in idDict :
      if not k1 == k2 :
        if idDict[k2] == att :
          targetIDs.append( k2 )

  if DEBUG :
    print "targetIDs = " + str(targetIDs)

  # grab all vals per joined id
  currResDictList = []
  for i in targetIDs :
    res = ad.get( i, cursor )
    currResDictList.append( res )

  return currResDictList
