#!/usr/bin/env python

'''
Test_simplejoin.py
'''

#############
#  IMPORTS  #
#############
# standard python packages
import logging, os, pickledb, sys, time, unittest

from pymongo import MongoClient

import SimpleJoin


######################
#  TEST SIMPLE JOIN  #
######################
class Test_simplejoin( unittest.TestCase ) :

  logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.DEBUG )
  #logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.INFO )

  ################
  #  ATTRIBUTES  #
  ################

  MONGODB_PATH = os.path.abspath( __file__ + "/../dbtmp")
  CURR_PATH    = os.path.abspath( __file__ + "/..")


  ##########################
  #  SIMPLE JOIN PICKLEDB  #
  ##########################
  # tests simplejoin on a pickledb instance
  def test_simplejoin_pickledb( self ) :

    test_id    = "test_simplejoin_pickledb"
    NOSQL_TYPE = "pickledb"

    # --------------------------------------------------------------- #
    # create db instance

    logging.info( "  " + test_id + ": initializing pickledb instance." )

    dbInst = pickledb.load( "./test_aggspack.db", False )

    # --------------------------------------------------------------- #
    # insert data

    book1 = { "author" : "Elsa Menzel",  \
              "title" : "A Martial Arts Primer", \
              "pubYear" : 2018, \
              "numCopies" : 0, \
              "categories" : ["fantasy"], \
              "cost(Dollars)" : 10 }
    book2 = { "author" : "Anna Summers", \
              "title" : "The Rising", \
              "pubYear" : 2017, \
              "numCopies" : 0, \
              "categories" : ["fantasy"], \
              "cost(Dollars)" : 9.99 }
    book3 = { "author" : "Kat Green", \
              "title" : "Frozen Space", \
              "pubYear" : 2016, \
              "numCopies" : 0, \
              "categories" : ["fantasy", "scifi"], \
              "cost(Dollars)" : 0 }
    book4 = { "author" : "Kat Green", \
              "title" : "Frozen Universe", \
              "pubYear" : 2017, \
              "numCopies" : 0, \
              "categories" : ["fantasy", "scifi"], \
              "cost(Dollars)" : 0 }
  
    dbInst.set( "bid1", book1 )
    dbInst.set( "bid2", book2 )
    dbInst.set( "bid3", book3 )
    dbInst.set( "bid4", book4 )

    # --------------------------------------------------------------- #
    # perform join

    simplejoin_op = SimpleJoin.SimpleJoin( NOSQL_TYPE, dbInst )
    res           = simplejoin_op.simplejoin( [["bid1", "bid2", "bid3", "bid4"]], "pubYear", None )

    # --------------------------------------------------------------- #
    # check results

    expected_res = [ { "pubYear":2017, \
                       "cost(Dollars)":0, \
                       "author":"Kat Green", \
                       "title":"Frozen Universe", \
                       "numCopies":0, \
                       "categories":["fantasy","scifi"]},
                     { "pubYear":2017, \
                       "cost(Dollars)":9.99, \
                       "author":"Anna Summers", \
                       "title":"The Rising", \
                       "numCopies":0, \
                       "categories":["fantasy"]} ]

    matchCount = 0

    for i in range( 0, len( res ) ) :

      actual_list = res[ i ]

      foundMatch = True
      for j in range( 0, len( expected_res ) ) :

        expected_list = expected_res[ j ]

        logging.debug( "-----------------------------------------------------------------" )
        logging.debug( "  SIMPLEJOIN PICKLEDB : actual_list   = " + str( actual_list ) )
        logging.debug( "  SIMPLEJOIN PICKLEDB : expected_list = " + str( expected_list ) )

        flag = True

        for key in actual_list :

          logging.debug( "  SIMPLEJOIN :   actual_list[ " + key + " ] = " + str( actual_list[ key ] ) )
          logging.debug( "  SIMPLEJOIN : expected_list[ " + key + " ] = " + str( expected_list[ key ] ) )

          if actual_list[ key ] == expected_list[ key ] :
            logging.debug( "  SIMPLEJOIN : True match" )

          else :
            logging.debug( "  SIMPLEJOIN : False match" )
            logging.debug( "  SIMPLEJOIN : setting foundMatch to False." )

            flag       = False
            foundMatch = False
            break

        if flag :
          foundMatch = True
          break

      logging.debug( "  SIMPLEJOIN : outerloop foundMatch = " + str( foundMatch ) )

      if foundMatch :
        matchCount += 1

    self.assertEqual( matchCount, len( res ) )

    # ---------------------------------------------------- #
    # close pickle db instance

    dbInst.deldb()


  #########################
  #  SIMPLE JOIN MONGODB  #
  #########################
  # tests simplejoin on a mongodb instance
  def test_simplejoin_mongodb( self ) :

    test_id    = "test_simplejoin_mongodb"
    NOSQL_TYPE = "mongodb"

    # --------------------------------------------------------------- #
    # create db instance

    logging.info( "  " + test_id + ": initializing mongodb instance." )

    self.createInstance_mongodb()

    # wait 5 seconds for the db to set
    time.sleep( 5 )

    client = MongoClient()
    dbInst = client.bookdb

    # --------------------------------------------------------------- #
    # insert data

    book1 = { "author" : "Elsa Menzel",  \
              "title" : "A Martial Arts Primer", \
              "pubYear" : 2018, \
              "numCopies" : 0, \
              "categories" : ["fantasy"], \
              "cost(Dollars)" : 10 }
    book2 = { "author" : "Anna Summers", \
              "title" : "The Rising", \
              "pubYear" : 2017, \
              "numCopies" : 0, \
              "categories" : ["fantasy"], \
              "cost(Dollars)" : 9.99 }
    book3 = { "author" : "Kat Green", \
              "title" : "Frozen Space", \
              "pubYear" : 2016, \
              "numCopies" : 0, \
              "categories" : ["fantasy", "scifi"], \
              "cost(Dollars)" : 0 }
    book4 = { "author" : "Kat Green", \
              "title" : "Frozen Universe", \
              "pubYear" : 2017, \
              "numCopies" : 0, \
              "categories" : ["fantasy", "scifi"], \
              "cost(Dollars)" : 0 }
  
    b    = dbInst.bookdb
    bid1 = b.insert_one( book1 ).inserted_id
    bid2 = b.insert_one( book2 ).inserted_id
    bid3 = b.insert_one( book3 ).inserted_id
    bid4 = b.insert_one( book4 ).inserted_id

    # --------------------------------------------------------------- #
    # perform join

    simplejoin_op = SimpleJoin.SimpleJoin( NOSQL_TYPE, b )
    res           = simplejoin_op.simplejoin( [[bid1, bid2, bid3, bid4]], "pubYear", None )

    # --------------------------------------------------------------- #
    # check results

    expected_res = [ { "pubYear":2017, \
                       "cost(Dollars)":0, \
                       "author":"Kat Green", \
                       "title":"Frozen Universe", \
                       "numCopies":0, \
                       "categories":["fantasy","scifi"]},
                     { "pubYear":2017, \
                       "cost(Dollars)":9.99, \
                       "author":"Anna Summers", \
                       "title":"The Rising", \
                       "numCopies":0, \
                       "categories":["fantasy"]} ]

    matchCount = 0

    for i in range( 0, len( res ) ) :

      actual_list = res[ i ]

      foundMatch = True
      for j in range( 0, len( expected_res ) ) :

        expected_list = expected_res[ j ]

        logging.debug( "-----------------------------------------------------------------" )
        logging.debug( "  SIMPLEJOIN PICKLEDB : actual_list   = " + str( actual_list ) )
        logging.debug( "  SIMPLEJOIN PICKLEDB : expected_list = " + str( expected_list ) )

        flag = True

        for key in actual_list :

          # skip keys
          if not u"_id" == key :

            logging.debug( "  SIMPLEJOIN :   actual_list[ " + key + " ] = " + str( actual_list[ key ] ) )
            logging.debug( "  SIMPLEJOIN : expected_list[ " + key + " ] = " + str( expected_list[ key ] ) )
  
            if actual_list[ key ] == expected_list[ key ] :
              logging.debug( "  SIMPLEJOIN : True match" )
  
            else :
              logging.debug( "  SIMPLEJOIN : False match" )
              logging.debug( "  SIMPLEJOIN : setting foundMatch to False." )
  
              flag       = False
              foundMatch = False
              break

        if flag :
          foundMatch = True
          break

      logging.debug( "  SIMPLEJOIN : outerloop foundMatch = " + str( foundMatch ) )

      if foundMatch :
        matchCount += 1

    self.assertEqual( matchCount, len( res ) )

    # ---------------------------------------------------- #
    # drop collections

    dbInst.bookdb.drop()

    # ---------------------------------------------------- #
    # close mongo db instance

    client.close()
  
    # get instance id
    os.system( "pgrep mongod 2>&1 | tee dbid.txt" )
    fo     = open( "dbid.txt", "r" )
    dbid   = fo.readline()
    fo.close()
    os.system( "rm " + self.CURR_PATH + "/dbid.txt" )
  
    logging.debug( "TEST_SIMPLEJOIN_MONGODB : dbid = " + dbid )
  
    os.system( "kill " + dbid )


  #############################
  #  CREATE INSTANCE MONGODB  #
  #############################
  def createInstance_mongodb( self ) :

    logging.info( "CREATE INSTANCE MONGODB : Creating mongo db instance at " + self.MONGODB_PATH + "\n\n" )

    # establsih clean target dir for db
    if not os.path.exists( self.MONGODB_PATH ) :
      os.system( "mkdir " + self.MONGODB_PATH + " ; " )
    else :
      os.system( "rm -rf " + self.MONGODB_PATH + " ; " )
      os.system( "mkdir " + self.MONGODB_PATH + " ; " )

    # build mongodb instance
    os.system( "mongod --dbpath " + self.MONGODB_PATH + " &" )


#########
#  EOF  #
#########
