import getopt
import sys

from runner import run_worker


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
        'scrape_operations',
        'scrape_blockchain',
        'validate_operations',
        'scrape_all_users',
        'scrape_all_users_quick',
        'scrape_prices',
        'refresh_dbstats',
        'override',
    ]
    if worker not in workers:
        print("ERROR: Invalid or no worker specified!")
        quit(1)

    run_worker(worker)


if __name__ == "__main__":
    print("Starting worker: %s" % sys.argv)
    start_worker(sys.argv[1:])
