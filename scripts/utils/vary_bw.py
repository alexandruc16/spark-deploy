import argparse
import time
import numpy as np
from subprocess import Popen, PIPE
 

# generate a number in the given distribution 
def get_value(distribution):
    # this generates a number between 1 and 4, which represents the current quartile
    # r_bound is the upper bound of the quartile
    r_bound = np.random.randint(1, 5)
    # l_bound is the lower bound of the quartile
    l_bound = r_bound - 1
    # get the actual values from the distribution
    low = distribution[l_bound]
    high = distribution[r_bound]
    # get a value within the quartile
    return int(np.random.uniform(low, high + 1.0))
    
    
def main():
    parser = argparse.ArgumentParser(description="Use wondershaper to vary "
                                     "bandwidth according to a distribution.", 
                                     epilog = "Example Usage: "
                                     ".python vary_bw.py -i 5 -d 60 150 260 380 650")
    parser.add_argument("-i", "--interval", metavar="", dest="interval", type=int,
        action="store", help="interval (seconds) at which to generate a new value")
    parser.add_argument("-d", "--distribution", metavar="N", dest="distr", nargs=5,
        type=float, action="store", help="interval at which to generate a new value")
    args = parser.parse_args()
      
    while True:
        value = str(get_value(args.distr))
        out = Popen(["bash", "/opt/wondershaper/wondershaper", "-c", "-a", "ens3"], stdout = PIPE, stderr = PIPE).communicate()[0]
        out = Popen(["bash", "/opt/wondershaper/wondershaper", "-a", "ens3", "-u", value, "-d", value], stdout = PIPE, stderr = PIPE).communicate()[0]
        
        print(value)
        
        time.sleep(args.interval)

if __name__ == '__main__':
    main()

