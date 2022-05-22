import csv
import json
import os
from datetime import timedelta, datetime

import psycopg2 as psycopg2
import requests as requests
from psycopg2.extras import RealDictCursor

headers = {
    'authority': 'www.tipranks.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
              'application/signed-exchange;v=b3;q=0.9',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/93.0.4577.82 Safari/537.36',
    'origin': 'https://www.tipranks.com',
    'sec-fetch-site': 'same-site',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-dest': 'document',
    'referer': 'https://www.tipranks.com',
    'accept-language': 'en-US,en;q=0.9,si;q=0.8',
}


def find_holidays():
    cursor = database()
    response = requests.get('https://www.tipranks.com/api/calendars/holidays/?break=1647721939743', headers=headers)
    if response.status_code == 200:
        json_data = response.json()
        for key, value in json_data['us'].items():
            for holiday in value:
                cursor.execute(f"SELECT * FROM holiday_calendar WHERE holiday_date = '{holiday['date']}'")
                data = cursor.fetchone()
                if data is not None:
                    # Update row
                    cursor.execute(
                        'update holiday_calendar set description = %s, partial_day = %s where holiday_date = %s'
                        , (holiday['name'], holiday['partialDay'], holiday['date']))
                else:
                    # Insert Row
                    cursor.execute('insert into holiday_calendar(holiday_date, description, partial_day)'
                                   ' values (%s, %s, %s)', (holiday['date'], holiday['name'], holiday['partialDay']))
            print('Inserted all the holiday records to the table.')
            cursor.execute("DELETE FROM working_days_copy")
            print('Deleting all the records from working_days_copy table.')
            start_date = datetime(int(key), 1, 1)
            end_date = datetime(int(key), 12, 31)
            get_working_days(start_date, end_date, cursor, key)

    else:
        print(f'Error : cannot fetch URL due to Status Code :{response.status_code}')


def read_controls():
    columns = {}
    with open(f'Control{os.sep}Control.csv') as f:
        reader = csv.DictReader(f)  # read rows into a dictionary format
        for row in reader:  # read a row as {column1: value1, column2: value2,...}
            columns[row['Variable']] = row['Value']
    return columns


def get_working_days(start, end, cursor, year):
    delta = end - start
    csv_data = read_controls()
    for i in range(delta.days + 1):
        day = start + timedelta(days=i)
        if day.weekday() not in (int(csv_data['WKND_1']), int(csv_data['WKND_2'])):
            cursor.execute(f"SELECT * FROM holiday_calendar WHERE holiday_date = '{day}'")
            data = cursor.fetchone()
            if data is None:
                cursor.execute('insert into working_days_copy(work_date, close_time, parital_day) values (%s, %s, %s)',
                               (day, None, False))
            else:
                if data['partial_day']:
                    cursor.execute('insert into working_days_copy(work_date, close_time, parital_day) values (%s, %s, '
                                   '%s)',
                                   (day, csv_data['PRT_CLS'], data['partial_day']))
    print('Inserted all the working records to the working_days_copy table.')

    cursor.execute(f"delete from working_days where date_part('year', work_date) = '{year}'")
    print('Deleting all the records from working_days table.')
    cursor.execute("SELECT * FROM working_days_copy")
    data = cursor.fetchall()
    for d in data:
        cursor.execute('insert into working_days(work_date, close_time, parital_day) values (%s, %s, %s)',
                       (d['work_date'], d['close_time'], d['parital_day']))
    print('Inserted all the working records to the working_days table.')


def database():
    """Function to connect to the PostgreSQL Database

    Returns:
        [object]: Cursor object
    """
    # Read Credentials.json
    json_data = dict(json.load(open(f'Control{os.sep}Credentials.json')))

    # Assign values from control.csv
    db = json_data["database"]
    user = json_data["user"]
    pw = json_data["password"]
    host = json_data["host"]
    port = json_data["port"]

    # Connecting to DB and Cursor creation
    conn = psycopg2.connect(database=db, user=user, password=pw, host=host, port=port)
    conn.autocommit = True

    return conn.cursor(cursor_factory=RealDictCursor)


if __name__ == '__main__':
    find_holidays()
