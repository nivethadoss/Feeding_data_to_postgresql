import re
import os
import sys
import logging
import traceback
from datetime import datetime as dt
from pymongo import MongoClient
import psycopg2


def main():

    client = MongoClient("localhost")
    results = client.nivi.CTC_Tigerresults
    count = 0

    for i, run in enumerate(results.find(no_cursor_timeout = True)):
        doc_id = "{}".format(run['_id'])
        count  = count + 1


        try:
            if (run['outcome']['test_date'] != None):
                testdate = run["outcome"]["test_date"]
                order_date = run['version']['date']
                order = order_date[:-16]
                print(i, "test_date : " + testdate,"order_date : " + order)
            else:
                print(doc_id)
        except Exception as err:
            print("rejected doc '{}': {}\r".format(doc_id, err))
    print(count)

if __name__ == "__main__":
    main()
    print("finished taking test_date")