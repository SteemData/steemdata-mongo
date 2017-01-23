import getopt
import os
import sys
import traceback
from pprint import pprint

from steem import Steem

from bitsports import refresh_bitsports
from mongostorage import MongoStorage
from scraper import scrape_all_users, scrape_operations, scrape_virtual_operations, \
    scrape_active_posts, scrape_misc, override


def run_worker(worker_name):
    while True:
        # init db
        mongo = MongoStorage(db_name=os.environ['DB_NAME'],
                             host=os.environ['DB_HOST'],
                             port=os.environ['DB_PORT'])
        mongo.ensure_indexes()
        stm = Steem(node='ws://51.15.54.34:8090')

        try:
            if worker_name == "scrape_all_users":
                scrape_all_users(mongo, stm)
            elif worker_name == "scrape_operations":
                scrape_operations(mongo, stm)
            elif worker_name == "scrape_virtual_operations":
                scrape_virtual_operations(mongo, stm)
            elif worker_name == "scrape_active_posts":
                scrape_active_posts(mongo, stm)
            elif worker_name == "scrape_misc":
                scrape_misc(mongo)
            elif worker_name == "refresh_bitsports":
                refresh_bitsports(mongo, stm)
            elif worker_name == "override":
                override(mongo)
        except (KeyboardInterrupt, SystemExit):
            exit("Quitting...")
        except:
            print("EXCEPTION: %s():" % worker_name)
            pprint(traceback.format_exc())


def start_worker(argv):
    worker = None
    try:
        opts, args = getopt.getopt(argv, "hw:o", ["worker=", "options="])
    except getopt.GetoptError:
        print('__main__.py -w <worker_name>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('__main__.py -w <worker_name>')
            sys.exit()
        elif opt in ("-w", "--worker"):
            worker = arg
        elif opt in ("-o", "--options"):
            pass

    workers = [
        'scrape_all_users',
        'scrape_operations',
        'scrape_virtual_operations',
        'scrape_active_posts',
        'scrape_misc',
        'refresh_bitsports',
        'override',
    ]
    if worker not in workers:
        quit("ERROR: Invalid or no worker specified!")

    run_worker(worker)


if __name__ == "__main__":
    print("Starting worker: %s" % sys.argv)
    start_worker(sys.argv[1:])
