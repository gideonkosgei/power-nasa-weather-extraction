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
        self.database = 'kapiti_weather'
        self.user = 'root'
        self.password = 'G1d30nk0sg3189'
        # self.start = '19880101'
        self.start = '20160101'
        self.end = '20230325'

        self.processes = 5  # Please do not go more than five concurrent requests.
        self.request_template = r"https://power.larc.nasa.gov/api/temporal/hourly/point?parameters=RH2M&community=AG&longitude={longitude}&latitude={latitude}&start={start}&end={end}&format=JSON"
        self.filename_template = "File_Lat_{latitude}_Lon_{longitude}.csv"

        self.messages = []
        self.times = {}

    def download_function(self, collection):
        request, filepath, record_id = collection
        response = requests.get(url=request, verify=False, timeout=30.00).json()

        try:
            connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                                 password=self.password, autocommit=True)

            if connection.is_connected():
                longitude = response['geometry']['coordinates'][0]
                latitude = response['geometry']['coordinates'][1]
                rh2m_records = response['properties']['parameter']['RH2M']
                cursor = connection.cursor()
                # get all keys from dictionary
                rh2m_datetimes = list(rh2m_records.keys())
                # substring list values using list comprehension
                rh2m_dates = [rh2m_datetime[:-2] for rh2m_datetime in rh2m_datetimes]
                # get unique elements
                rh2m_dates_unique = set(rh2m_dates)
                records_to_update = []
                for rh2m_date_unique in rh2m_dates_unique:
                    rh2m_values = []

                    # get values that contains the date -> the keys have time component that should not be considered
                    for key, value in rh2m_records.items():  # iter on both keys and values
                        if key.startswith(rh2m_date_unique):
                            rh2m_values.append(value)

                    # get the minimum relative humidity
                    rh2m_min_value = min(rh2m_values)
                    rh2m_max_value = max(rh2m_values)
                    rh2m_date_unique_formatted = datetime.datetime.strptime(rh2m_date_unique, '%Y%m%d').date()
                    rh2m_date_unique_formatted_str = rh2m_date_unique_formatted.strftime('%Y-%m-%d')
                    # record_to_update = (rh2m_min_value,rh2m_max_value, rh2m_date_unique_formatted_str, longitude, latitude)
                    record_to_update = (rh2m_min_value, rh2m_max_value, rh2m_date_unique_formatted_str, longitude, latitude)
                    records_to_update.append(record_to_update)

                sql_update_query = """ insert into rm_min_max_data(RH_MIN,RH_MAX,date_value,longitude,latitude) values (%s,%s,%s,%s,%s)"""
                cursor.executemany(sql_update_query, records_to_update)
                sql_update_query2 = "UPDATE data_points SET is_processed =1 WHERE trim(id)={id}".format(id=record_id)
                cursor.execute(sql_update_query2)

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
                sql_select_query = "select id,lon_trunc,lat_trunc from data_points where (lat_trunc between -90 and 90) and (lon_trunc between -180 and 180) order by id"

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
                    print(request)
                    filename = self.filename_template.format(latitude=latitude, longitude=longitude)
                    requests.append((request, filename, record_id))

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
