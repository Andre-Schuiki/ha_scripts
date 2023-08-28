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

if energy_statistics:
    for statistic, statistic_config in energy_statistics.items():
        statistic_entity_id = statistic + '_' + statistic_config['type']
        print(f'----Start {statistic_entity_id}----')
        d = datetime.datetime.strptime(statistic_config['start_date'], '%d.%m.%Y')
        end_d = datetime.datetime.strptime(statistic_config['end_date'], '%d.%m.%Y')
        start_date_unix = time.mktime(d.timetuple())
        end_date_unix = time.mktime(end_d.timetuple())

        get_statistics_id_qry = "SELECT id FROM statistics_meta WHERE statistic_id = ?"
        get_statistics_id_res = cur.execute(get_statistics_id_qry, [statistic_entity_id])
        get_statistics_id = get_statistics_id_res.fetchone()

        if get_statistics_id:
            statistics_id = get_statistics_id[0]
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

        # remove old statistics
        print(f'Loading statistics for entity_id "{statistic}"')
        get_statistic_qry_str = """SELECT s.id, metadata_id, state, sum, created_ts, start_ts, last_reset_ts, sm.statistic_id, DATETIME(s.created_ts, 'unixepoch'), DATETIME(s.start_ts, 'unixepoch')
                               FROM statistics s
                               LEFT JOIN statistics_meta sm on s.metadata_id = sm.id
                               WHERE
                                   sm.statistic_id = ?
                                   AND s.created_ts > ?
                                   AND s.created_ts < ?
                               ORDER BY s.created_ts"""

        get_statistic_qry = cur.execute(get_statistic_qry_str, [statistic, start_date_unix, end_date_unix])
        entity_statistics = get_statistic_qry.fetchall()

        #r_last_sum = entity_statistics[-1][3]
        r_last_sum = 0
        monthly_data = {}

        r_last_state = 0.0
        insert_qry = """INSERT INTO statistics (metadata_id, state, sum, created_ts, start_ts) VALUES (?, ?, ?, ?, ?)"""
        insert_data = []
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
            if not first_counter_value:
                first_counter_value = r[2]
                last_counter_value = r[2]
                r_last_state = 0.0
                continue

            counter_diff = r[2] - last_counter_value
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
            if monthly_data.get(r_date.month):
                monthly_data[r_date.month].append(counter_diff)
            else:
                monthly_data.setdefault(r_date.month, [counter_diff])

        # only for debugging
        monthly_data_sum = {}
        for k, v in monthly_data.items():
            v_sum = sum(v)
            monthly_data_sum.setdefault(k, v_sum)

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
        pprint(monthly_data_sum)
        print(f'----END {statistic_entity_id}----')
print('Done')