from syncRawFluksoData import * 
from utils import getTimeSpent
import time


def main():
    nb_min = 1  # min between 2 queries
    interval = nb_min * 60  # sec

    i = 0
    max_it = 2
    while True:

        beg = time.time()

        # run query : syncRawFluksoData
        sync("automatic", "", "")

        # change configuration

        i += 1
        if i >= max_it:  # if we only want to run a certain amount of queries
            break

        end = time.time()
        diff = end - beg
        if diff < interval:
            print("Waiting before next query...")
            time.sleep(interval - diff) 

        print("==================================================================")
        print("==================================================================")


if __name__ == "__main__":
    main()