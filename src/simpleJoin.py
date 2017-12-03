#!/usr/bin/env python

# -------------------------------------- #
import logging, os, sys

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

class SimpleJoin( object ) :

  ################
  #  ATTRIBUTES  #
  ################
  nosql_type = None   # the type of nosql database under consideration
  cursor     = None   # pointer to target database instance


  ##########
  #  INIT  #
  ##########
  def __init__( self, nosql_type, cursor ) :
    self.cursor     = cursor
    self.nosql_type = nosql_type


  #################
  #  SIMPLE JOIN  #
  #################
  def simplejoin( self, idLists, joinAttr, pred ) :
  
    logging.debug( "  SIMPLEJOIN : idLists  = " + str( idLists ) )
    logging.debug( "  SIMPLEJOIN : joinAttr = " + str( joinAttr ) )
    logging.debug( "  SIMPLEJOIN : pred     = " + str( pred ) )

    ad = Adapter.Adapter( self.nosql_type )
  
    # assume all ids in db are unique
    # ids per joinAttr
    idDict = {}
    for currList in idLists :
      for currID in currList :
        res1   = ad.get( currID, self.cursor )
        attVal = res1[ joinAttr ]
        idDict[ currID ] = attVal

    logging.debug( "  SIMPLEJOIN : idDict = " + str( idDict ) )
  
    # get ids with identical joinAttr
    targetIDs = []
    for k1 in idDict :
      att = idDict[ k1 ]
      for k2 in idDict :
        if not k1 == k2 :
          if idDict[k2] == att :
            targetIDs.append( k2 )
  
    logging.debug( "  SIMPLEJOIN : targetIDs = " + str( targetIDs ) )

    # grab all vals per joined id
    currResDictList = []
    for i in targetIDs :
      res = ad.get( i, self.cursor )
      currResDictList.append( res )

    logging.debug( "  SIMPLEJOIN : currResDictList = " + str( currResDictList ) )

    return currResDictList


  ################
  #  DICT MERGE  #
  ################
  def dictMerge( a, b ) :
    c = a.copy()
    c = c.update(b)
    return c
  

#########
#  EOF  #
#########
