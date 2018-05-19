import argparse
import itertools
import logging
import logging.handlers
import numpy as np
import os
import sys
from subprocess import Popen, PIPE
from time import sleep


def generate_random_bandwidth_value(distribution):
    r_bound = np.random.randint(1, 5)
    l_bound = r_bound - 1
    high = distribution[r_bound]
    low = distribution[l_bound]
    
    return np.random.uniform(low, high + 1.0)


def generate(port, hosts, distribution, filename):
    log = 'New bandwidth values:\n'
    f = open(filename, "w")
    
    for pair in itertools.permutations(hosts, 2):
        bandwidth_value = generate_random_bandwidth_value(distribution)
        val = "%s:%s:%f" % (pair[0], pair[1], bandwidth_value)
        print >> f, val
        log += "%s\n" % val
    
    f.close()
    
    #result = Popen(["python", "shape_traffic_client.py", "--port",
    #    port, "--set-file", filename],
    #    stdout=PIPE).communicate()[0]
    
    logging.info(log)
    
    
def init_logging():
    log_filename = 'bandwidth_history.log'
    logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%d-%m-%Y %H:%M:%S')
    
    if os.path.isfile(log_filename):
        handler = logging.handlers.RotatingFileHandler(log_filename, mode='w', backupCount=5)
        handler.doRollover()


def main():
    parser = argparse.ArgumentParser(description="Throttle cluster bandwidth "
                                     "according to a distribution.", 
                                     epilog = "Example Usage: "
                                     "python throttle_bandwidth_distribution.py -p 2345 --hosts 10.10.10.10 10.10.10.11 --distribution 60 150 260 380 650 -i 1000")
    parser.add_argument("-p", "--port", metavar="", dest="port",
                        type=int, action="store", required=True,
                        help="Port number for bandwidth-throttler communication")                                 
    parser.add_argument("--hosts", metavar="", dest="hosts", required=True,
                        type=str, nargs="+", action="store",
                        help="Name for the cluster.")
    parser.add_argument("-d", "--distribution", metavar="", dest="distribution", required=True,
                        type=float, nargs=5, action="store",
                        help="Space-separated list of 5 floating points representing the distribution")
    parser.add_argument("-i", "--interval", metavar="", dest="interval",
                        type=int, action="store", default=None,
                        help="Interval in miliseconds at which new bandwidth values should be generated")
    parser.add_argument("-o", "--output", metavar="", dest="output_filename",
                        type=str, action="store", default="bandwidths.txt",
                        help="Name of the file where the bandwidths will be printed")
    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit(0)
        
    init_logging()
        
    if args.interval is None:
        generate(args.port, args.hosts, args.distribution, args.output_filename)
    else:
        sleep_seconds = args.interval / 1000
        #TODO  maybe something more precise
        while True:
            generate(args.port, args.hosts, args.distribution, args.output_filename)
            sleep(sleep_seconds)


if __name__ == "__main__":
    main()

