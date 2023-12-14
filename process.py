import os, sys, time, json, urllib3, requests, multiprocessing
import mysql.connector
from mysql.connector import Error
import datetime

urllib3.disable_warnings()


# THI = (1.8×Tmax+32) - [(0.55 - 0.0055×RHmin)×(1.8×Tmax - 26.8)]

class Process:

    def __init__(self):

        # Mysql Database Settings
        self.host = 'localhost'
        self.database = 'richard'
        self.user = 'root'
        self.password = 'G1d30nk0sg3189'
        self.start = '20010301'
        self.end = '20221231'

        self.processes = 5  # Please do not go more than five concurrent requests.

        self.request_template = r"https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,T2MDEW,T2MWET,TS,T2M_RANGE,T2M_MAX,T2M_MIN&community=AG&longitude={longitude}&latitude={latitude}&start={start}&end={end}&format=JSON"
        self.filename_template = "File_Lat_{latitude}_Lon_{longitude}.csv"

        self.messages = []
        self.times = {}



    def download_function(self, collection):

        request, filepath = collection
        response = requests.get(url=request, verify=False, timeout=30.00).json()

        try:
            connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                                 password=self.password)

            if connection.is_connected():
                longitude = response['geometry']['coordinates'][0]
                latitude = response['geometry']['coordinates'][1]
                t2m_max_records = response['properties']['parameter']['T2M_MAX']
                t2m_max_dates = list(t2m_max_records.keys())
                cursor = connection.cursor()
                for t2m_max_date in t2m_max_dates:
                    t2m_max_value = t2m_max_records[t2m_max_date]
                    t2m_max_date_formatted = datetime.datetime.strptime(t2m_max_date, '%Y%m%d').date()
                    t2m_max_date_formatted_str = t2m_max_date_formatted.strftime('%Y-%m-%d')

                    sql_insert_query = "insert into weather_data(date_value,longitude,latitude,T2M_MAX) values (\'{date_value}\',{longitude},{latitude},{T2M_MAX})".format(
                        date_value=t2m_max_date_formatted_str, longitude=longitude, latitude=latitude,
                        T2M_MAX=t2m_max_value)
                    cursor.execute(sql_insert_query)
                    connection.commit()


        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def execute(self):
        Start_Time = time.time()

        try:
            connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                                 password=self.password)

            if connection.is_connected():
                sql_select_query = "select id,lon_trunc,lat_trunc from data_points where is_processed=0 and (lat_trunc between -90 and 90) and (lon_trunc between -180 and 180) order by id"
                cursor = connection.cursor()
                cursor.execute(sql_select_query)
                records = cursor.fetchall()  # get all records
                print("Total Number Of Data Points: ", cursor.rowcount)
                requests = []
                for record in records:
                    record_id = record[0]
                    longitude = record[1]
                    latitude = record[2]
                    request = self.request_template.format(latitude=latitude, longitude=longitude,
                                                           start=self.start, end=self.end)
                    filename = self.filename_template.format(latitude=latitude, longitude=longitude)
                    requests.append((request, filename))

                    sql_update_query = "UPDATE data_points SET is_processed =1 WHERE trim(id)={id}".format(
                        id=record_id)
                    cursor.execute(sql_update_query)
                    connection.commit()

        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

                requests_total = len(requests)
                pool = multiprocessing.Pool(self.processes)
                x = pool.imap_unordered(self.download_function, requests)

                for i, df in enumerate(x, 1):
                    sys.stderr.write('\rExporting {0:%}'.format(i / requests_total))

                self.times["Total Script"] = round((time.time() - Start_Time), 2)
                print("\n")
                print("Total Script Time:", self.times["Total Script"])


if __name__ == '__main__':
    Process().execute()
