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
        self.database = 'power_nasa_weather'
        self.user = 'root'
        self.password = 'G1d30nk0sg3189'
        self.start = '20160101'
        self.end = '20221208'

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
                sql_select_query = "select distinct lon_trunc,lat_trunc from data_points where is_duplicate = 0 and is_processed_temp=0"
                cursor = connection.cursor()
                cursor.execute(sql_select_query)
                records = cursor.fetchall()  # get all records
                print("Total Number Of Data Points: ", cursor.rowcount)
                requests = []
                for record in records:
                    longitude = record[0]
                    latitude = record[1]
                    request = self.request_template.format(latitude=latitude, longitude=longitude,
                                                           start=self.start, end=self.end)
                    filename = self.filename_template.format(latitude=latitude, longitude=longitude)
                    requests.append((request, filename))

                    sql_update_query = "UPDATE data_points SET is_processed_temp =1 WHERE trim(lon_trunc)={longitude} AND trim(lat_trunc)={latitude}".format(
                        latitude=latitude, longitude=longitude)
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
