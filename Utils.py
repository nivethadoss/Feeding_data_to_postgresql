import sys
from datetime import datetime as dt

class Utils:

    @staticmethod
    def validation(record):
        start = None
        valid =False

        if "ptcStartTest" in record["outcome"]:
            start_dt = f"{record['outcome']['ptcStartTest']}"
            end_dt = f"{record['outcome']['ptcEndTest']}"
            try:
                start_dt = dt.strptime(start_dt, '%H:%M:%S')
            except:
                start_dt = dt.strptime(start_dt, '%H:%M.%S')
                end_dt = dt.strptime(end_dt, '%H:%M.%S')

        else:
            if "endTest" in record["outcome"]:
                end_dt = f"{record['outcome']['endTest']}"
                end_dt = end_dt[:-5]
                end_dt = dt.strptime(end_dt, '%Y-%m-%dT%H:%M:%S')
            if 'startTest' in record['outcome']:
                start_dt = f"{record['outcome']['startTest']}"
                start_dt = start_dt[:-5]
                start_dt = dt.strptime(start_dt, '%Y-%m-%dT%H:%M:%S')
            else:
                print("{}, {}, {}".format(record["_id"], record["outcome"]["startTest"], record["outcome"]["endTest"]))


        return start_dt, end_dt
