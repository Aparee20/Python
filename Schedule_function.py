import sys
import json
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
import psycopg2
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
logging.basicConfig()


def run_job1():
    print 'print after 1 sec'


def run_job2():
    print 'print after 2 sec'


sc = SparkContext()
sqlContext = SQLContext(sc)

scheduler = BlockingScheduler()
scheduler.add_job(run_job1,'interval',seconds=1)
scheduler.add_job(run_job2,'interval',seconds=2)
scheduler.start()









