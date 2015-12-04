# =============================================================================
# IMPORTS
# =============================================================================
from datetime import datetime, timedelta
from multiprocessing import Process
from praw.errors import ExceptionList, APIException, InvalidCaptcha, InvalidUser, RateLimitExceeded
from pytz import timezone
from requests.exceptions import HTTPError, ConnectionError, Timeout
from socket import timeout
from urllib2 import URLError
from urllib2 import urlopen
import ConfigParser
import MySQLdb
import argparse
import json
import logging
import os
import parsedatetime.parsedatetime as pdt
import pprint
import praw
import sys
import time

# =============================================================================
# GLOBALS
# =============================================================================

# Reads the config file
config = ConfigParser.ConfigParser()
config.read("twitch.cfg")

# Reddit info
user_agent = "RedditLive bot by /u/zathegfx"
reddit = praw.Reddit(user_agent = user_agent)
USER = config.get("reddit", "username")
PASS = config.get("reddit", "password")

# Database connection info
DB_HOST = config.get("database", "host")
DB_NAME = config.get("database", "database")
DB_USER = config.get("database", "username")
DB_PASS = config.get("database", "password")
DB_TABLE = config.get("database", "table")

# Json user data
JSONFILE = config.get("json", "filename")

# =============================================================================
# Functions
# =============================================================================

# ----------------------------------------------------
# --- check if the database tables have been created
# ----------------------------------------------------
def create_database():
    conn = MySQLdb.connect (host = DB_HOST,
                           user = DB_USER,
                           passwd = DB_PASS,
                           db = DB_NAME)
    cursor = conn.cursor ()

    q = "CREATE TABLE IF NOT EXISTS " + DB_TABLE + """ (
            ID int NOT NULL AUTO_INCREMENT key,
            redditUserName VARCHAR(50) NOT NULL,
            twitchUserName VARCHAR(50) NOT NULL,
            console VARCHAR(5) NULL,
            onlineStatus int NOT NULL
        )"""
    
    cursor.execute(q)
    
# -----------------------------
# --- get user data from JSON
# -----------------------------
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

def check_for_new_users():
    db = MySQLdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME )
    
    with open(os.path.join(__location__, JSONFILE)) as json_file:
        json_data = json.load(json_file)
        json_size = len(json_data['sherpasArray'])
        
        for index in range(json_size):
            # Get data from JSON file
            username = json_data['sherpasArray'][index]['username']
            twitchUserName = json_data['sherpasArray'][index]['twitch']
            console = json_data['sherpasArray'][index]['console']
            
            cursor = db.cursor()
            
            currentTime1 = datetime.now(timezone('UTC'))
            currentTime  = format(currentTime1, '%Y-%m-%d %H:%M:%S')
            
            cmd = "SELECT * FROM " + DB_TABLE + " WHERE redditUserName = %s"
            cursor.execute(cmd, [username])
            results = cursor.fetchall()
            
            if (len(results) > 0):
                # If entry exists, skip
                return True;
            else:
                # Enter new user into table
                cmd = "INSERT INTO " + DB_TABLE + " (redditUserName, twitchUserName, console, onlineStatus) VALUES (%s, %s, %s, %s)"
                cursor.execute(cmd, [username, twitchUserName, console, 0])
                print currentTime + ' - Inserted new record into table: ' + username + ' | ' + twitchUserName + ' | ' + console
            
            db.commit()
    
# -----------------------------
# --- check if user is online
# -----------------------------
def check_user_status(user):
    """ 
    returns =
        0: online
        1: offline
        2: not found
        3: error 
    """
    url = 'https://api.twitch.tv/kraken/streams/' + user
    try:
        info = json.loads(urlopen(url, timeout = 15).read().decode('utf-8'))
        if info['stream'] == None:
            status = 1
        else:
            status = 0
    except URLError as e:
        if e.reason == 'Not Found' or e.reason == 'Unprocessable Entity':
            status = 2
        else:
            status = 3
    return status
  
# -------------------------------------------------------------------------
# --- get user from table and check status - update table based on status
# -------------------------------------------------------------------------
def get_user():
    db = MySQLdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME )
    cursor = db.cursor()
    
    currentTime1 = datetime.now(timezone('UTC'))
    currentTime  = format(currentTime1, '%Y-%m-%d %H:%M:%S')
    
    cmd = "SELECT * FROM " + DB_TABLE
    cursor.execute(cmd)
    results = cursor.fetchall()
    
    for row in results:
        
        username = row[1]
        twitchUserName = row[2]
        status = check_user_status(row[2])
        
        if (status != row[4]):
            cmd = "UPDATE " + DB_TABLE + " set onlineStatus='%s' WHERE twitchUserName='" + twitchUserName + "'"
            cursor.execute(cmd, [status])
            print currentTime + ' - ONLINE STATUS CHANGE FOR [ ' + username + ' ] - STATUS: [ ' + str(status) + ' ]'
            
        db.commit()

# =============================================================================
# MAIN
# =============================================================================

def main():
    reddit.login(USER, PASS)
    db = MySQLdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME )
    
    print "start"
    while True:
        try:
            get_user()
            time.sleep(60)
        except Exception as err:
           print  'There was an error in main(): '
           print err
           sys.exit(0)
    
# =============================================================================
# RUNNER
# =============================================================================

if __name__ == '__main__':
    Process(target=create_database).start()
    Process(target=check_for_new_users()).start()
    Process(target=main).start()