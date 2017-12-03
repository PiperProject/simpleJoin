#!/usr/bin/env python

import copy, os, pickledb, string, sys, unittest

#####################
#  UNITTEST DRIVER  #
#####################
def unittest_driver() :

  print
  print "***************************************"
  print "*   RUNNING TEST SUITE FOR AGGSPACK   *"
  print "***************************************"
  print

  os.system( "python -m unittest Test_simplejoin.Test_simplejoin.test_simplejoin_mongodb" )
  os.system( "python -m unittest Test_simplejoin.Test_simplejoin.test_simplejoin_pickledb" )


#########################
#  THREAD OF EXECUTION  #
#########################
unittest_driver()


#########
#  EOF  #
#########
