import re
import os
import sys
import traceback
import logging
from datetime import datetime as dt
from pymongo import MongoClient
import psycopg2
from Utils import Utils


__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class CoherenceError(Exception):
    pass

def insert_measures(pg, res_id, criteria, measures, bol):
    keep = {}

    foff = None
    power = None
    for foff in criteria.keys():
        current_foff = criteria[foff]
        for power in current_foff.keys():
            p_value = power
            bounds = current_foff[power]
            for name, value in measures.items():
                name = re.sub('^[RT]X_', '', name)
                value = float(value)
                if name in bounds:
                    p_min = bounds[name][0]
                    p_max = bounds[name][1]
                else:
                    p_min = 0
                    p_max = 0
                pg.execute("""
                    INSERT INTO verification_measures
                    (verification_result_id, name, foff, power, value, min, max, ok)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                           (res_id, name, foff, p_value, value, p_min, p_max, bol))




def build_criteria(raw_criteria, fc, foff_list, power_list):
    ret = {}

    for key, value in raw_criteria.items():
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

        for j, foff in enumerate(foff_list):
            if foff not in ret[fc]:
                ret[fc][foff] = {}

            if type(power_list) == int:
                power_list = [power_list]

            for k, power in enumerate(power_list):

                if power in ret[fc][foff]:
                    if key not in ret[fc][foff][power]:
                        ret[fc][foff][power][key] = [None, None]
                else:
                    ret[fc][foff][power] = {key: [None, None]}

                ret[fc][foff][power][key][index] = value
    return ret




def insert_cal_results(pg, calibration_id, rf_ype, test, outcome):
    pass


def insert_ver_results(pg, verification_id, rf_type, test, outcome_val, conf, doc_id):
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
        measure_list = results['measures']
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

    criteria = test["measuresCriteria"]
    consumption_max = criteria["consumption_max"]
    criteria = build_criteria(criteria, fc, foff_list, power_list)

    test_result = True

    """
    if rf_type == 'TX':
            if type(power_list) == int:
                power_list = [power_list]
            if len(measure_list) != len(power_list):
                test_result = False

    if rf_type == 'TX':
        if type(foff_list) == int:
            foff_list = [foff_list]
        if len(measure_list) != len(foff_list):
            test_result = False

    if rf_type == 'RX':
        if len(foff_list) != len(measure_list):
            test_result = False

    """
    #print(doc_id)
    pg.execute("""
           INSERT INTO verification_results (verification_id, fc, consumption_max, test_result)
           VALUES(%s, %s, %s, %s) 
           RETURNING verification_result_id       
           """, (verification_id, fc, results['consumption'], results['result'] == 0))

    v_result_id = pg.fetchone()[0]


    for measures in measure_list:
        insert_measures(pg, v_result_id, criteria[fc], measures, True)





val_or_cal_test= re.compile('([RT]X)_(.*)_(validation|calibration)$')


def insert_verifications(pg, run, doc_id):
    for test_name, test in run.items():
        #print(test_name)
        m = val_or_cal_test.match(test_name)
        if m is None:
            continue
        test_type = m.group(3)
        if "error" not in m.group(2):
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
                        insert_ver_results(pg, verification_id, rf_type, test, run['outcome']['val'], run["configuration"], doc_id)
                    except CoherenceError as err:
                        raise CoherenceError("[{}] {}".format(test_name, err))
        """
        if test_type == "calibration":
            settings = test["settings"]
            if settings["enabled"]:
                rf_type = m.group(1)
                name = m.group(2)
                try:
                    pg.execute(""""""
                            INSERT INTO calibrations (run_id, type, name, stop_if_fail)
                            VALUES (%s, %s, %s, %s)
                            RETURNING calibration_id
                    """""", (run["_id"], rf_type, name, settings['stop_if_fail']))
                    calibration_id = pg.fetchone()[0]
                except:
                    import traceback
                    traceback.print_exc()
                try:
                    insert_cal_results(run["_id"], pg, calibration_id, rf_type, test, run['outcome']['val'], run["configuration"])
                except CoherenceError as err:
                    raise CoherenceError("[{}] {}".format(test_name, err))
        """


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
    con = psycopg2.connect(host="localhost", port="5432", database="postgres", user="postgres", password="postgres")
    cursor = con.cursor()
    cursor.execute(open(os.path.join(__location__, "schema.sql")).read())
    con.commit()
    client = MongoClient("localhost")

    results = client.nivi.CTC_Tigerresults
    stationid_list = []
    for i, run in enumerate(results.find()):
            doc_id = "{}".format(run['_id'])

            try:
                station_id = get_or_insert_station(cursor, run['baseStationID'], run['configuration']['hardwareVersion'], i)
                stationid_list.append(station_id)
                utb_id = get_or_insert_utb(cursor, run['PTB_ID'], run['PTC_SW'], run['version']['EMS'])
                ptu_id = get_or_insert_ptu(cursor, run['PTU_ID'], run['PTU_SW'])
                outcome = run['outcome']
                start_dt, end_dt = Utils.validation(run)
                duration = int((end_dt - start_dt).total_seconds())
                isocal = start_dt.isocalendar()
                cursor.execute("""
                        INSERT INTO runs (station_id, ptu_id, utb_id, start, duration, outcome_val,
                            outcome_msg, mongo_id, year_week, year_month, order_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING run_id
                        """,
                           (station_id, ptu_id, utb_id, start_dt, duration, int(outcome['val']), outcome['msg'], doc_id,
                            "{}-{}".format(isocal[0], isocal[1]), "{}-{}".format(start_dt.year, start_dt.month),
                            run['version']['order_id']))

                run['_id'] = cursor.fetchone()[0]
                insert_verifications(cursor, run, doc_id)
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
"""
    for table in ['stations', 'ptus', 'utbs', 'runs', 'verifications', 'verification_results', 'verification_measures']:
        export_to_csv(cursor, "tmp/tables", table)

"""

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
    print("pg feed finished")
