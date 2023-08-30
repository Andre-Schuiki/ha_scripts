import csv
import sys
import time
import datetime
import sqlite3
import yaml
from pprint import pprint

con = sqlite3.connect("home-assistant_v2.db")
cur = con.cursor()

with open('fix_energy_statistic_costs.yaml', 'r') as file:
    config = yaml.safe_load(file)

if not config:
    sys.exit(1)

energy_statistics = config.get('energy_statistics')


def delete_statistics(statistics_id, start_date, end_date):
    csv_d = datetime.datetime.strptime(start_date, '%d.%m.%Y %H:%M:%S')
    csv_end_d = datetime.datetime.strptime(end_date, '%d.%m.%Y %H:%M:%S')
    csv_start_date_unix = time.mktime(csv_d.timetuple())
    csv_end_date_unix = time.mktime(csv_end_d.timetuple())

    delete_statistic_qry = """DELETE FROM statistics
                                              WHERE
                                                metadata_id = ?
                                                AND start_ts >= ?
                                                AND start_ts <= ?
                                                """

    rows_to_del = cur.execute(
        'SELECT * FROM statistics where metadata_id = ? AND start_ts >= ? AND start_ts <= ?',
        [statistics_id, csv_start_date_unix, csv_end_date_unix]).fetchall()
    print(f'Deleting {len(rows_to_del)} rows from statistics table!')
    del_qry = cur.execute(delete_statistic_qry, [statistics_id, csv_start_date_unix, csv_end_date_unix])

    delete_statistic_qry = """DELETE FROM statistics_short_term
                                              WHERE
                                                metadata_id = ?
                                                """

    rows_to_del = cur.execute(
        'SELECT * FROM statistics_short_term where metadata_id = ?',
        [statistics_id]).fetchall()
    print(f'Deleting {len(rows_to_del)} rows from short_term_statistics table!')
    del_qry = cur.execute(delete_statistic_qry, [statistics_id])


if energy_statistics:
    for statistic, statistic_config in energy_statistics.items():
        entity_id = statistic
        statistic_entity_id = statistic + '_' + statistic_config['type']
        print(f'----Start {statistic_entity_id}----')
        d = datetime.datetime.strptime(statistic_config['start_date'], '%d.%m.%Y')
        end_d = datetime.datetime.strptime(statistic_config['end_date'], '%d.%m.%Y')
        start_date_unix = time.mktime(d.timetuple())
        end_date_unix = time.mktime(end_d.timetuple())

        get_statistics_id_qry = "SELECT id FROM statistics_meta WHERE statistic_id = ?"
        get_statistics_id_res = cur.execute(get_statistics_id_qry, [statistic_entity_id])
        statistics_db_id = get_statistics_id_res.fetchone()

        get_statistics_entity_id_res = cur.execute(get_statistics_id_qry, [entity_id])
        statistics_entity_db_id = get_statistics_entity_id_res.fetchone()[0]

        if statistics_db_id:
            statistics_id = statistics_db_id[0]
            print(f'Statistics ID found for entity {statistic_entity_id}')
            delete_statistic_qry = """DELETE FROM statistics
                                      WHERE
                                        metadata_id = ?
                                        AND created_ts > ?
                                        AND created_ts < ?
                                        """

            rows_to_del = cur.execute('SELECT * FROM statistics where metadata_id = ? AND created_ts > ? AND created_ts < ?', [statistics_id, start_date_unix, end_date_unix]).fetchall()
            print(f'Deleting {len(rows_to_del)} rows from statistics table!')
            del_qry = cur.execute(delete_statistic_qry, [statistics_id, start_date_unix, end_date_unix])

            delete_statistic_qry = """DELETE FROM statistics_short_term
                                      WHERE
                                        metadata_id = ?
                                        AND created_ts > ?
                                        AND created_ts < ?
                                        """

            rows_to_del = cur.execute('SELECT * FROM statistics_short_term where metadata_id = ? AND created_ts > ? AND created_ts < ?', [statistics_id, start_date_unix, end_date_unix]).fetchall()
            print(f'Deleting {len(rows_to_del)} rows from short_term_statistics table!')
            del_qry = cur.execute(delete_statistic_qry, [statistics_id, start_date_unix, end_date_unix])
        else:
            print(f'Statistics ID not found for entity {statistic_entity_id}, exit!')
            sys.exit(2)

        csv_import_data = []
        if statistic_config.get('import_csv'):
            for s, s_data in statistic_config.get('import_csv').items():
                resets = s_data.get('resets').keys()
                with open(s) as f:
                    reader = csv.DictReader(f, delimiter=';')

                    meter_reading = s_data['meter_reading_start']
                    counter = 0
                    hour_sum = 0
                    statistic_sum = 0
                    date_str = ''
                    time_str = ''
                    for i, row in enumerate(reader):
                        row_time = time.strptime(row['Zeit'], '%H:%M:%S')
                        val = float(row['kWh'].replace(',', '.'))
                        if row_time.tm_min == 0 and row_time.tm_sec == 0:
                            date_str = row['Datum']
                            time_str = row['Zeit']
                            hour_sum = 0
                            counter = 0

                        hour_sum += val
                        counter += 1

                        if counter == 4:
                            record_date_str = f'{date_str} {time_str}'
                            record_date = datetime.datetime.strptime(f'{date_str} {time_str}', '%d.%m.%Y %H:%M:%S')
                            if record_date_str in resets:
                                offset = s_data['resets'].get(record_date_str)

                                if offset:
                                    meter_reading = offset['offset']
                                    #statistic_sum = offset['offset']
                                else:
                                    meter_reading = 0
                                    #statistic_sum = 0
                            meter_reading += hour_sum
                            statistic_sum += hour_sum
                            csv_import_data.append([statistics_entity_db_id, meter_reading, statistic_sum, record_date, record_date, hour_sum])
                            #print(f'{date_str} {time_str}: {hour_sum} kWh')

                # del overlapping statistics
                delete_statistics(statistics_id=statistics_entity_db_id, start_date=s_data['start_date'], end_date=s_data['end_date'])
                delete_statistics(statistics_id=statistics_id, start_date=s_data['start_date'], end_date=s_data['end_date'])


        if csv_import_data:
            for statistics_entity_db_id, hour_sum, statistic_sum, record_date, record_date, meter_reading in csv_import_data:
                try:
                    insert_qry = """INSERT INTO statistics (metadata_id, state, sum, created_ts, start_ts) VALUES (?, ?, ?, ?, ?)"""
                    created_ts = record_date + datetime.timedelta(hours=1, seconds=10)
                    created_ts_unix = time.mktime(created_ts.timetuple())
                    start_ts_unix = time.mktime(record_date.timetuple())
                    insert_res = cur.execute(insert_qry, [statistics_entity_db_id, hour_sum, statistic_sum, created_ts_unix, start_ts_unix])
                    insert_data.append([statistics_entity_db_id, hour_sum, statistic_sum, created_ts, record_date, meter_reading])
                except Exception as e:
                    e

            #con.commit()


        print(f'Loading statistics for entity_id "{statistic}"')
        get_statistic_qry_str = """SELECT s.id, metadata_id, state, sum, created_ts, start_ts, last_reset_ts, sm.statistic_id, DATETIME(s.created_ts, 'unixepoch', 'localtime'), DATETIME(s.start_ts, 'unixepoch', 'localtime')
                               FROM statistics s
                               LEFT JOIN statistics_meta sm on s.metadata_id = sm.id
                               WHERE
                                   sm.statistic_id = ?
                                   AND s.created_ts > ?
                                   AND s.created_ts < ?
                               ORDER BY s.created_ts"""

        if csv_import_data:
            c_start_date_unix = time.mktime(csv_import_data[0][3].timetuple())
            c_end_date_unix = time.mktime(end_d.timetuple())
            get_statistic_qry = cur.execute(get_statistic_qry_str, [statistic, c_start_date_unix, c_end_date_unix])
        else:
            get_statistic_qry = cur.execute(get_statistic_qry_str, [statistic, start_date_unix, end_date_unix])
        entity_statistics = get_statistic_qry.fetchall()

        #r_last_sum = entity_statistics[-1][3]
        r_last_sum = 0
        debug_data = {}

        r_last_state = 0.0
        insert_qry = """INSERT INTO statistics (metadata_id, state, sum, created_ts, start_ts) VALUES (?, ?, ?, ?, ?)"""
        insert_data = []
        # if csv_import_data:
        #     first_counter_value = entity_statistics[0][2]
        #     last_counter_value = entity_statistics[0][2]
        #     r_last_state = 0.0
        # else:
        first_counter_value = None
        state_diff_sum = []

        for i, r in enumerate(entity_statistics):
            multiplicator = None
            for date_str, val in statistic_config['periods'].items():
                r_date = datetime.datetime.strptime(r[9], '%Y-%m-%d %H:%M:%S')
                date = datetime.datetime.strptime(date_str, '%d.%m.%Y')
                if r_date < date:
                    multiplicator = val
                    break
            if not multiplicator:
                #print(f'Can\'t finde compansation val for date {r[7]}')
                continue
            if first_counter_value == None:
                first_counter_value = r[2]
                last_counter_value = r[2]
                r_last_state = 0.0
                continue

            counter_diff = r[2] - last_counter_value
            if counter_diff < 0:
                counter_diff = 0
            last_counter_value = r[2]
            r_state_diff = counter_diff * multiplicator
            state_diff_sum.append(r_state_diff)
            r_state = r_state_diff
            r_last_state = r_state
            r_sum = r_last_sum + r_state_diff
            r_last_sum = r_sum

            created_ts = r[4]
            start_ts = r[5]

            try:
                insert_res = cur.execute(insert_qry, [statistics_id, r_state, r_sum, created_ts, start_ts])
                insert_data.append([statistics_id, r_state, r_sum, created_ts, start_ts, r[8], r[9]])
            except Exception as e:
                e

            # only for debugging
            if not debug_data.get(r_date.year):
                debug_data.setdefault(r_date.year, {})

            if debug_data[r_date.year].get(r_date.month):
                debug_data[r_date.year][r_date.month][0].append(counter_diff)
                debug_data[r_date.year][r_date.month][1].append(r_state_diff)
            else:
                debug_data[r_date.year].setdefault(r_date.month, [[counter_diff], [r_state_diff]])

        # only for debugging
        debug_data_sum = {}
        for year, months in debug_data.items():
            for month, data in months.items():
                m_sum = sum(data[0])
                m_sum_eur = sum(data[1])
                debug_data_sum.setdefault(f'{year}_{month:02d}', [round(m_sum, 0), round(m_sum_eur, 2)])

        # add last state/sum to short term statistics
        try:
            insert_qry = """INSERT INTO statistics_short_term (metadata_id, state, sum, created_ts, start_ts) VALUES (?, ?, ?, ?, ?)"""
            insert_res = cur.execute(insert_qry, insert_data[-1][0:5])
        except Exception as e:
            e

        # save changes to db
        con.commit()

        print(f'added statistics for entity_id {statistic_entity_id}')
        print('Monthly Data:')
        pprint(debug_data_sum)
        print(f'----END {statistic_entity_id}----')
print('Done')