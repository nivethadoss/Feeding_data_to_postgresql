
# importing all the necessary modules
import re
import os
import sys
import logging
import traceback
from datetime import datetime as dt
from pymongo import MongoClient
import psycopg2
from Utils import Utils

# getting the entire current directory 
path of this file
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class CoherenceError(Exception):
    pass

def insert_cal_measures(pg, res_id, criteria, r_measures, bol):

    keep = {}
    foff = None
    for foff in criteria.keys():
        current_foff = criteria[foff]
        for power in current_foff.keys():
            p_value = power
            bounds = current_foff[power]

            for name, value in r_measures.items():
                name = re.sub('^[RT]X_', '', name)
                value = float(value)
                if name in bounds:

                    p_min = bounds[name][0]
                    if p_min is not None:
                        p_min = float(p_min)
                    p_max = bounds[name][1]
                    if p_max is not None:
                       p_max = float(p_max)
                else:
                    p_min = 0.0
                    p_max = 0.0
                pg.execute("""
                                    INSERT INTO calibration_measures
                                    (calibration_result_id, name, foff, power, value, min, max, ok)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                    """,
                           (res_id, name, foff, p_value, value, p_min, p_max, bol))



def insert_ver_measures(pg, res_id, criteria, r_measures, bol):
    keep = {}

    foff = None
    power = None
    for foff in criteria.keys():
        current_foff = criteria[foff]
        for power in current_foff.keys():
            p_value = power
            bounds = current_foff[power]

            for name, value in r_measures.items():
                name = re.sub('^[RT]X_', '', name)
                value = float(value)

                if name in bounds:
                    p_min = bounds[name][0]
                    p_max = bounds[name][1]
                else:
                    p_min = 0
                    p_max = 0
                if type(p_min) == list:

                    pg.execute("""
                        INSERT INTO verification_measures1
                        (verification_result_id, name, foff, power, value, min, max, ok)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                               (res_id, name, foff, p_value, value, p_min, p_max,  bol))

                else:
                    pg.execute("""
                                INSERT INTO verification_measures
                                (verification_result_id, name, foff, power, value, min, max, ok)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """,(res_id, name, foff, p_value, value, p_min, p_max,  bol))






def build_criteria(m_criteria, fc, foff_list, power_list):
    ret = {}


    for key, value in m_criteria.items():
        if key in ['consumption_max', 'Record_temp_min']:
            continue
        key = re.sub('^[RT]X_', '', key)

        m = re.search('_m(in|ax)$', key)
        index = 2
        if m:
            match = m.group(0)
            index = 1
            if match == '_min':
                index = 0

        key = re.sub('_m(in|ax)$', '', key)
        if fc not in ret:
            ret[fc] = {}

        if type(foff_list) == int:
            foff_list = [foff_list]
        min_max_list = []

        for j, foff in enumerate(foff_list):
            if foff not in ret[fc]:
                ret[fc][foff] = {}

            if type(power_list) == list:
                for k, power in enumerate(power_list):
                    if power in ret[fc][foff]:
                        if key not in ret[fc][foff][power]:
                            ret[fc][foff][power][key] = [None, None]
                            if type(value) != list :
                                ret[fc][foff][power][key][index] = value
                            else:
                                for i in value:
                                    min_max_list.append(i)
                                    ret[fc][foff][power][key][index] = min_max_list

                        else:
                            ret[fc][foff][power][key][index] = value
                    else:
                        ret[fc][foff][power] = {key: [None, None]}
                        ret[fc][foff][power][key][index] = value

            else:
                power_list = [power_list]
                for k, power in enumerate(power_list):

                    if power in ret[fc][foff]:
                        if key not in ret[fc][foff][power]:
                            ret[fc][foff][power][key] = [None, None]
                            if type(value) != list :
                                ret[fc][foff][power][key][index] = value
                            else:
                                for i in value:
                                    min_max_list.append(i)
                                    ret[fc][foff][power][key][index] = min_max_list
                            ret[fc][foff][power][key][index] = value
                    else:
                        ret[fc][foff][power] = {key: [None, None]}
                        ret[fc][foff][power][key][index] = value


    return ret

id_RX_list = []
id_TX_list = []


def insert_cal_results(pg, calibration_id, rf_type, name, test, conf):

    settings = test["settings"]
    try:
        results = test["results"] # sometimes it is dict or list
        if type(results) == list:
            if len(results) <= 3:
                results = results[0]
                if results["result"] != 0:
                    return
    except KeyError:
        return

    if results["Origin"] == 0 or results["result"] != 0:
        return

    measure_list = None
    try:
        try:
            measure_list = results["measures"]
        except:
            measure_list = results[0]["measures"]

    except KeyError:
        if results["result"] != 0:
            return
        if measure_list == None:
            return
        else:
            raise CoherenceError("results is ok but no measures present")

    if rf_type == "RX":
        try:
            power_list = settings['Pin']
        except:
             power_list= settings["preamp2_Pin"]

    if rf_type == "TX":
        try:
            power_list = settings["TX_Pout"]
        except:
            try:
                power_list = settings["Pout"]
            except:
                powerlist = []
                if rf_type + "_" + name == "TX_power_transfer_function":
                    for i in measure_list:
                        val = i["TX_Pout"]
                        powerlist.append(val)
                    power_list = powerlist


    test_result = True
    fc = conf["radioPlan"][rf_type + 'F']
    foff_list = settings["Foff"]

    m_criteria = test["measuresCriteria"]
    consumption_max = m_criteria["consumption_max"]
    criteria = build_criteria(m_criteria, fc, foff_list, power_list)
    if len(criteria) == 0:
        return


    pg.execute("""
                INSERT INTO calibration_results (calibration_id, fc, consumption_max, test_result)
                VALUES (%s, %s, %s, %s)
                RETURNING calibration_result_id
                 """, (calibration_id, fc, results["consumption"], results["result"] == 0))
    c_result_id = pg.fetchone()[0]

    for r_measures in measure_list:
        insert_cal_measures(pg, c_result_id, criteria[fc], r_measures, True)


def insert_ver_results(pg, verification_id, rf_type, test, conf):
    settings = test["settings"]
    try:
        results = test['results'] #sometimes results can be list or dictionary
    except KeyError:
        return

    if rf_type == 'RX':
        try:
            power_list = settings['Pin']
        except:
            power_list = settings["preamp2_Pin"]
    else:
        try:
            power_list = settings['TX_Pout']
        except:
            power_list = settings["Pout"]

    if type(results) != dict:
        results = results[0]

    measure_list = None
    try:
        try:
            measure_list = results['measures']
        except:
            measure_list = results["measures"]
    except KeyError:
        if results['result'] != 0:
            return
        if measure_list == None:
            return
        else:
            raise CoherenceError("result is ok but no measures present")

    if results['result'] != 0:
        return

    fc = conf["radioPlan"][rf_type + 'F']
    foff_list = settings['Foff']

    m_criteria = test["measuresCriteria"]
    consumption_max = m_criteria["consumption_max"]
    criteria = build_criteria(m_criteria, fc, foff_list, power_list)

    test_result = True

    pg.execute("""
           INSERT INTO verification_results (verification_id, fc, consumption_max, test_result)
           VALUES(%s, %s, %s, %s) 
           RETURNING verification_result_id       
           """, (verification_id, fc, results['consumption'], results['result'] == 0))

    v_result_id = pg.fetchone()[0]


    for r_measures in measure_list:
        insert_ver_measures(pg, v_result_id, criteria[fc], r_measures, True)

val_or_cal_test= re.compile('([RT]X)_(.*)_(validation|calibration)$')


def insert_verifications_calibrations(pg, run, doc_id):
    for test_name, test in run.items():
        #print(test_name)
        m = val_or_cal_test.match(test_name)
        if m is None:
            continue
        test_type = m.group(3)

        if test_type == "validation":
            settings = test["settings"]
            if settings["enabled"]:
                rf_type = m.group(1)
                name = m.group(2)
                pg.execute("""
                            INSERT INTO verifications (run_id, type, name, stop_if_fail)
                            VALUES (%s, %s, %s, %s)
                            RETURNING verification_id
                """, (run['_id'], rf_type, name, settings["stop_if_fail"]))
                verification_id = pg.fetchone()[0]
                try:
                    print(test_name)
                    insert_ver_results(pg, verification_id, rf_type, test, run["configuration"])
                except CoherenceError as err:
                    raise CoherenceError("[{}] {}".format(test_name, err))
            else:
                continue
        if test_type == "calibration":
            settings = test["settings"]
            if test_name != "RX_reference_gain_calibration":
                if settings["enabled"]:
                    rf_type = m.group(1)
                    name = m.group(2)
                    try:
                        pg.execute("""
                                INSERT INTO calibrations (run_id, type, name, stop_if_fail)
                                VALUES (%s, %s, %s, %s)
                                RETURNING calibration_id
                        """, (run["_id"], rf_type, name, settings['stop_if_fail']))
                        calibration_id = pg.fetchone()[0]
                    except:
                        import traceback
                        traceback.print_exc()

                    try:
                        print(test_name)
                        insert_cal_results(pg, calibration_id, rf_type, name, test, run["configuration"])
                    except CoherenceError as err:
                        raise CoherenceError("[{}] {}".format(test_name, err))



ptu_ref = {}
def get_or_insert_ptu(pg, raw_ptu_serial, ptu_sw_ver):
    ptu_serial = 0
    if type(raw_ptu_serial) != str:
        try:
            ptu_serial = int(raw_ptu_serial)
        except TypeError as err:
            raise CoherenceError ("missing ptu_id ({})]\r".format(err))

    try:
        ptu_id = ptu_ref[(ptu_serial, ptu_sw_ver)]
    except KeyError as err:
        pg.execute("""
            INSERT INTO ptus (ptu_serial, ptu_sw_ver)
            VALUES (%s, %s)
            RETURNING ptu_id
            """,
                   (ptu_serial, ptu_sw_ver))
        ptu_id = pg.fetchone()[0]
        pg.connection.commit()

        ptu_ref[(ptu_serial, ptu_sw_ver)] = ptu_id

    return ptu_id

utb_ref = {}
def get_or_insert_utb(pg, utb_host, utb_sw_ver, ems):
    try:
        utb_id = utb_ref[(utb_host, utb_sw_ver)]
    except KeyError:
        pg.execute("""
            INSERT INTO utbs (hostname, utb_sw_ver, ems)
            VALUES (%s, %s, %s)
            RETURNING utb_id
            """,
                   (utb_host, utb_sw_ver, ems))
        utb_id = pg.fetchone()[0]
        pg.connection.commit()
        utb_ref[(utb_host, utb_sw_ver)] = utb_id

    return utb_id


# craetign the empty dictionary to store key and values
station_ref = {}
def get_or_insert_station(pg, baseId, hw, i):

    if  baseId not in station_ref.values():
        if baseId != "null":

            pg.execute("""
                        INSERT INTO stations (station_id, hardware)
                        VALUES (%s, %s)
                        """,
                       (baseId, hw) )
            station_ref["station_id" + str(i + 1)] = baseId
        pg.connection.commit()
    return baseId
"""
def export_to_csv(cursor, directory, table):
    directory = os.path.join(os.getcwd(), directory)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(os.path.join(directory, '{}.csv'.format(table)), 'w') as f:
        cursor.copy_expert("COPY {} TO STDOUT WITH CSV HEADER DELIMITER ';'".format(table), f)

"""
def main():
    # connecting to the postgres to access the database
    con = psycopg2.connect(host="localhost", port="5432", database="postgres", user="postgres", password="postgres")
    cursor = con.cursor()
    cursor.execute(open(os.path.join(__location__, "schema.sql")).read())
    con.commit()

    #connecting to the Mongodb server to have collection access
    client = MongoClient("localhost")
    results = client.nivi.Schema_Tiger_Collection
    count =0

    #looping through each document in a collection
    for i, run in enumerate(results.find(no_cursor_timeout=True)):
        doc_id = "{}".format(run['_id'])

        #calling each function for specific process
        try:
                station_id = get_or_insert_station(cursor, run['baseStationID'], run['configuration']['hardwareVersion'], i)
                utb_id = get_or_insert_utb(cursor, run['PTB_ID'], run['PTC_SW'], run['version']['EMS'])
                ptu_id = get_or_insert_ptu(cursor, run['PTU_ID'], run['PTU_SW'])
                outcome = run['outcome']
                start_dt, end_dt = Utils.validation(run)
                testdate = run['outcome']['test_date']
                duration = int((end_dt - start_dt).total_seconds())
                isocal = start_dt.isocalendar()
                orderdate = run['version']['date']
                order = orderdate[:-16]
                print(str(i) , "order_date : "+ order,"testdate : "+ testdate)


                # pg cursor to execute the query to create runs table
                cursor.execute("""
                        INSERT INTO runs (station_id, ptu_id, utb_id, hash_id, start_date, test_date, duration, outcome_val,
                            outcome_msg, mongo_id, year_week, year_month, order_id, order_qty, order_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING run_id
                        """,
                           (station_id, ptu_id, utb_id, run['hash_id'], start_dt, testdate, duration, int(outcome['val']), outcome['msg'], doc_id,
                            "{}-{}".format(isocal[0], isocal[1]), "{}-{}".format(start_dt.year, start_dt.month),
                            run['version']['order_id'], run['version']['order_qty'], run['version']['date']))

                #fetching the run_id from runs table
                run['_id'] = cursor.fetchone()[0]
                insert_verifications_calibrations(cursor, run, doc_id)

        except CoherenceError as err:
            print("rejected doc '{}': {}\r".format(doc_id, err), file=sys.stderr)
            cursor.connection.rollback()
        except Exception as err:
            print("rejected doc '{}': unexpected error ({})\r".format(doc_id, err), file=sys.stderr)

            traceback.print_exc()
            cursor.connection.rollback()
            sys.exit()
        else:
            cursor.connection.commit()
        count = count +1

"""
    for table in ['stations', 'ptus', 'utbs', 'runs', 'verifications', 'verification_results', 'verification_measures']:
        export_to_csv(cursor, "tmp/tables", table)

"""

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    #calling the main function
    main()
    print("pg feed finished")
