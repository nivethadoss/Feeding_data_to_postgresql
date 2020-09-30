import re
import os
import sys
import traceback
import logging
from datetime import datetime as dt
from pymongo import MongoClient
import psycopg2


__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class CoherenceError(Exception):
    pass



def insert_measures(pg, result_id, criteria, measures, is_ok):
    keep = {}

    foff = None
    power = None

    for name, value in measures.items():
        name = re.sub('^[RT]X_', '', name)
        value = float(value)

        if name == 'Foff':
            foff = value
        elif name in ['Pin', 'Pout']:
            power = value
        else:
            keep[name] = value

    if foff is None and power is None:
        if len(criteria) == 1:
            foff = list(criteria)[0]
        if len(criteria[foff]) == 1:
            power = list(criteria[foff])[0]
        if foff is None or power is None:
            raise CoherenceError("cannot guess foff and power")
    if foff is None:
        if len(criteria) != 1:
            raise CoherenceError("cannot guess foff")
        foff = list(criteria)[0]
    if power is None:
        if len(criteria[foff]) != 1:
            raise CoherenceError("cannot guess power")
        power = list(criteria[foff])[0]

    criteria = criteria[foff][power]

    for name, value in keep.items():
        try:
            bounds = criteria[name]
        except KeyError:
            bounds = [None, None]
        pg.execute("""
            INSERT INTO verification_measures
            (verification_result_id, name, foff, power, value, min, max, ok)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
                   (result_id, name, foff, power, value, bounds[0], bounds[1], is_ok))


def build_criteria(raw_criteria, fc_list, foff_list, power_list):
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

        for i, fc in enumerate(fc_list):
            if isinstance(value, list):
                if len(value) == len(fc_list):
                    by_foff = value[i]
                else:
                    raise CoherenceError("array length must be equal to the number of fc ({})".format(len(fc_list)))
            else:
                by_foff = value

            if fc not in ret:
                ret[fc] = {}

            for j, foff in enumerate(foff_list):
                if isinstance(by_foff, list) and len(by_foff) == len(foff_list):
                    by_power = by_foff[j]
                else:
                    by_power = by_foff

                if foff not in ret[fc]:
                    ret[fc][foff] = {}

                for k, power in enumerate(power_list):
                    if isinstance(by_power, list):
                        if len(by_power) == len(power_list):
                            bound = by_power[k]
                        else:
                            raise CoherenceError("cannot unnest measure criteria '{}'".format(key))
                    else:
                        bound = by_power

                    try:
                        bound = float(bound)
                        if re.search('^ANT(2_low|1_high)$', key) and bound == -1.0:
                            bound = None
                    except ValueError:  # bound is not a float
                        bound = None

                    if power in ret[fc][foff]:
                        if key not in ret[fc][foff][power]:
                            ret[fc][foff][power][key] = [None, None]
                    else:
                        ret[fc][foff][power] = {key: [None, None]}

                    bounds = ret[fc][foff][power][key]

                    if index < 2:
                        bounds[index] = bound
                    else:
                        bounds[0] = bound
                        bounds[1] = bound
    return ret


def insert_results(pg, verification_id, rf_type, test, outcome, docu_id):

    settings = test['settings']
    try:
        results = test['results']
    except KeyError:
        return

    try:
        power_list = settings['Pin' if rf_type == 'RX' else 'TX_Pout']
    except KeyError:
        power_list = settings['Pout']

    fc_list = settings[rf_type + '_Fc']
    foff_list = settings['Foff']

    criteria = test['measuresCriteria']
    consumption_max = criteria['consumption_max']
    temp_min = criteria['Record_temp_min']
    criteria = build_criteria(criteria, fc_list, foff_list, power_list)

    for i, fc in enumerate(fc_list):
        try:
            result = results[i]
        except IndexError:
            if outcome != 0:
                continue
            else:
                raise CoherenceError("not enough results")

        try:
            measure_list = result['measures']
        except KeyError:
            if result['result'] != 0:
                continue
            else:
                raise CoherenceError("result is ok but no measures present")

        if len(foff_list) * len(power_list) > len(measure_list):
            if outcome != 0:
                continue
            else:
                raise CoherenceError("not enough measures")

        r_fc = result[rf_type + '_Fc']
        doc_id = docu_id
        if fc != r_fc:
            raise CoherenceError("Fc mismatch")

        pg.execute("""
            INSERT INTO verification_results
            (verification_id, fc, temperature, temperature_min, consumption, consumption_max, ok)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING verification_result_id
            """,
                   (verification_id, r_fc, result['Record_temp'], temp_min, result['consumption'],
                    consumption_max, result['result'] == 0))
        result_id = pg.fetchone()[0]

        for measures in result['failed_measures']:
            insert_measures(pg, result_id, criteria[fc], measures, False)
        for measures in measure_list:
            insert_measures(pg, result_id, criteria[fc], measures, True)


verification_tests = re.compile('([RT]X)_(.*)_(verification|calibration)$')


def insert_verifications(pg, run, doc_id):

    for test_name, test in run.items():
        print(test_name, test)
        m = verification_tests.match(test_name)
        if m is None:
            continue

        test_type = m.group(3)
        if test_type != 'verification':
            continue
        print(doc_id)
        settings = test['settings']
        try:
            if not settings['enabled']:
                continue
        except KeyError:
            raise CoherenceError('{} has no setting "enabled"'.format(test_name))

        rf_type = m.group(1)
        name = m.group(2)

        pg.execute("""
            INSERT INTO verifications (run_id, type, name, stop_if_fail, max_retries)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING verification_id
            """,
                   (run['_id'], rf_type, name, settings['stop_if_fail'], settings['max_allowed_retries']))
        verification_id = pg.fetchone()[0]

        try:
            insert_results(pg, verification_id, rf_type, test, run['outcome']['val'], doc_id)
        except CoherenceError as err:
            raise CoherenceError("[{}] {}".format(test_name, err))


def create_datetime(outcome, time_key):
    try:
        date = outcome['test_date']
        time = outcome[time_key]
    except KeyError as err:
        raise CoherenceError("missing test_date or {} ({})]\r".format(time_key, err))

    try:
        d = "{}T{}".format(date, time)
    except ValueError as err:
        raise CoherenceError("bad format: '{}' or '{}' ({})]\r".format(date, time, err))

    return dt.strptime(d, '%Y-%m-%dT%H:%M.%S')


ptu_ref = {}


def get_or_insert_ptu(pg, raw_ptu_serial, ptu_sw_ver):
    try:
        ptu_serial = int(raw_ptu_serial)
    except TypeError as err:
        raise CoherenceError("missing ptu_id ({})]\r".format(err))

    try:
        ptu_id = ptu_ref[(ptu_serial, ptu_sw_ver)]
    except KeyError:
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
def get_or_insert_station(pg, station_id, hw, board_id):
    if board_id == "":
        board_id = None
    if station_id not in station_ref:
        pg.execute("""
            INSERT INTO stations (station_id, hardware, board_id)
            VALUES (%s, %s, %s)
            """,
                   (station_id, hw, board_id))
        pg.connection.commit()
        station_ref[station_id] = board_id
    elif station_ref[station_id] is None and board_id is not None:
        pg.execute("""
                    UPDATE stations
                    SET hardware = %s, board_id = %s
                    WHERE station_id = %s
                    """,
                   (hw, board_id, station_id))
        pg.connection.commit()

    return station_id


def export_to_csv(cursor, directory, table):
    directory = os.path.join(os.getcwd(), directory)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(os.path.join(directory, '{}.csv'.format(table)), 'w') as f:
        cursor.copy_expert("COPY {} TO STDOUT WITH CSV HEADER DELIMITER ';'".format(table), f)


def main():
    con = psycopg2.connect(host="localhost", port="5432", database="postgres", user="postgres", password="postgres")
    cursor = con.cursor()
    cursor.execute(open(os.path.join(__location__, "Lynx_schema.sql")).read())
    con.commit()
    client = MongoClient("localhost")
    query = {
        'configuration.shutdown_mode': 'EXPLOITATION',
        'version.order_id': {'$regex': 'P4_U.*'},
        '$and': [{'baseStationID': {'$ne': '00000000'}}, {'baseStationID': {'$ne': ''}}]
        # $ne stands for not equal to that value
    }  # only production Lynx
    results = client.nivi.CTC_Results
    for i, run in enumerate(results.find(query)):
        doc_id = "{}".format(run['_id'])
        print(doc_id)
        try:
            station_id = get_or_insert_station(cursor, run['baseStationID'], run['configuration']['hardwareVersion'],
                                               run['boardID'])
            utb_id = get_or_insert_utb(cursor, run['PTB_ID'], run['PTC_SW'], run['version']['EMS'])
            ptu_id = get_or_insert_ptu(cursor, run['PTU_ID'], run['PTU_SW'])
            outcome = run['outcome']
            start_dt = create_datetime(outcome, 'ptcStartTest')
            end_dt = create_datetime(outcome, 'ptcEndTest')
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

    for table in ['stations', 'ptus', 'utbs', 'runs', 'verifications', 'verification_results', 'verification_measures']:
        export_to_csv(cursor, "tmp/tables", table)
        #print(table)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
    print("feed is finished")
