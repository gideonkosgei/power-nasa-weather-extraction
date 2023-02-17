import os, sys, time, json, urllib3, requests, multiprocessing
import mysql.connector
from mysql.connector import Error

urllib3.disable_warnings()


def download_function(collection):
    request, filepath = collection
    response = requests.get(url=request, verify=False, timeout=30.00).json()
    with open(filepath, 'w') as file_object:
        json.dump(response, file_object)

    print(response)


class Process:

    def __init__(self):
        # Mysql Database Settings
        self.host = 'localhost',
        self.database = 'power_nasa_weather',
        self.user = 'root',
        self.password = 'G1d30nk0sg3189'
        self.start = '20140101'
        self.end = '20221231'

        self.processes = 5  # Please do not go more than five concurrent requests.

        self.request_template = r"https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,T2MDEW,T2MWET,TS,T2M_RANGE,T2M_MAX,T2M_MIN&community=RE&longitude={longitude}&latitude={latitude}&start={start}&end={end}&format=JSON".format
        {self.start, self.end}
        self.filename_template = "File_Lat_{latitude}_Lon_{longitude}.csv"

        self.messages = []
        self.times = {}

    def execute(self):

        try:
            connection = mysql.connector.connect(host='localhost', database='power_nasa_weather', user='root',
                                                 password='G1d30nk0sg3189')
            start_time = time.time()

            if connection.is_connected():
                sql_select_query = \
                    "select id, lon_trunc, lat_trunc from data_points where is_processed=0 limit 5"
                cursor = connection.cursor()
                cursor.execute(sql_select_query)
                records = cursor.fetchall()  # get all records
                print("Total Number Of Data Points: ", cursor.rowcount)
                requests = []

                for row in records:
                    longitude = row[1]
                    latitude = row[2]

                    request = self.request_template.format(latitude=latitude, longitude=longitude, start=self.start,
                                                           end=self.end)
                    filename = self.filename_template.format(latitude=latitude, longitude=longitude)
                    requests.append((request, filename))

                requests_total = len(requests)
                print(requests_total)
                pool = multiprocessing.Pool(self.processes)
                x = pool.imap_unordered(download_function, requests)

                # for i, df in enumerate(x, 1):
                #     sys.stderr.write('\rExporting {0:%}'.format(i / requests_total))
                #
                # self.times["Total Script"] = round((time.time() - start_time), 2)
                #
                # print("\n")
                # print("Total Script Time:", self.times["Total Script"])


        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")


Process().execute()
