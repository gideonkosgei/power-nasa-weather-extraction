import sys, time, json,os, urllib3, requests, multiprocessing
import mysql.connector
import datetime
import pandas as pd
import csv

urllib3.disable_warnings()


class Nasa:
    def __init__(self, host, username, password, database):

        self.db_config = {
            "host": host,
            "user": username,
            "password": password,
            "database": database,
        }

        self.conn = None

        # NASA POWER Settings
        self.start = '20010101'
        self.end = '20231201'
        self.processes = 5  # Please do not go more than five concurrent requests.
        self.request_template = r"https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,RH2M&community=AG&longitude={longitude}&latitude={latitude}&start={start}&end={end}&format=JSON"
        # self.request_template = r"https://power.larc.nasa.gov/api/temporal/hourly/point?parameters=T2M,RHM&community=AG&longitude={longitude}&latitude={latitude}&start={start}&end={end}&format=JSON"

        self.messages = []
        self.times = {}

    def build_db_objects(self):
        conn = mysql.connector.connect(**self.db_config)
        if conn.is_connected:
            try:
                cursor = conn.cursor()
                print('Create Database Objects')

                table_nasa_geopoints_original = """
                CREATE TABLE IF NOT EXISTS nasa_geopoints_all(
                  id int NOT NULL AUTO_INCREMENT,
                  event_id int DEFAULT NULL,
                  event_date date DEFAULT NULL,
                  longitude varchar(20) DEFAULT NULL,
                  latitude varchar(20) DEFAULT NULL,
                  longitude_edit varchar(20) DEFAULT NULL,
                  latitude_edit varchar(20) DEFAULT NULL,
                  PRIMARY KEY (id)
                ) 
                """
                # cursor.execute(table_nasa_geopoints_original)

                table_nasa_geopoints_unique = """
                  CREATE TABLE IF NOT EXISTS nasa_geopoints_unique(
                  id int NOT NULL AUTO_INCREMENT,
                  longitude_edit varchar(20) DEFAULT NULL,
                  latitude_edit varchar(20) DEFAULT NULL,
                  processed tinyint DEFAULT '0',
                  PRIMARY KEY (id))
                """
                cursor.execute(table_nasa_geopoints_unique)

                conn.commit()
                cursor.close()
                conn.close()
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    def purge_db_objects(self):
        conn = mysql.connector.connect(**self.db_config)
        if conn.is_connected:
            try:
                cursor = conn.cursor()
                print('Purge DB Objects')
                purge_nasa_geopoints_all = "Truncate table nasa_geopoints_all"
                cursor.execute(purge_nasa_geopoints_all)

                purge_nasa_geopoints_unique = "Truncate table nasa_geopoints_unique"
                cursor.execute(purge_nasa_geopoints_unique)

                conn.commit()
                cursor.close()
                conn.close()
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    def fetch_all_geopoints(self):
        conn = mysql.connector.connect(**self.db_config)
        if conn.is_connected:
            try:
                cursor = conn.cursor()
                print('Fetch All Geopoints')

                sql_all_geopoints = """    
                INSERT INTO nasa_geopoints_all(event_id,event_date,longitude,latitude,longitude_edit,latitude_edit)
                SELECT event_id, event_date, longitude, latitude,round(longitude*20)/20,round(latitude*20)/20 
                FROM reports.alt_test_day_events;
                """
                cursor.execute(sql_all_geopoints)
                conn.commit()
                cursor.close()
                conn.close()
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    def fetch_distinct_geopoints(self):
        conn = mysql.connector.connect(**self.db_config)
        if conn.is_connected:
            try:
                cursor = conn.cursor()
                print('Fetch Distinct Geopoints')

                sql_distinct_geopoints = """    
                    INSERT INTO nasa_geopoints_unique(longitude_edit,latitude_edit)
                    SELECT DISTINCT longitude_edit,latitude_edit FROM nasa_geopoints_all
                   """
                cursor.execute(sql_distinct_geopoints)
                conn.commit()
                cursor.close()
                conn.close()
            except mysql.connector.Error as err:
                print(f"Error: {err}")


    def build_request(self):
        try:
            conn = mysql.connector.connect(**self.db_config)
            if conn.is_connected:

                sql_select_query = ("select id,longitude_edit,latitude_edit from nasa_geopoints_unique where "
                                    "processed= 0")
                cursor = conn.cursor()
                cursor.execute(sql_select_query)
                records = cursor.fetchall()  # get all records
                print("Total Number Of Geo Points: ", cursor.rowcount)
                requests= []
                for record in records:
                    record_id = record[0]
                    longitude = record[1]
                    latitude = record[2]
                    # start = record[3]
                    # end = record[4]

                    request = self.request_template.format(latitude=latitude, longitude=longitude,
                                                           start=self.start, end=self.end)
                    requests.append((request, record_id))
                cursor.close()
                conn.close()
            return requests
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def execute(self):
        requests = nasa.build_request()
        # print(requests)
        Start_Time = time.time()
        requests_total = len(requests)
        filepath = "T2M_RH2M.csv"
        if requests is None:
            print('No Request To Process')
        else:

            conn = mysql.connector.connect(**self.db_config)
            if conn.is_connected:

                pool = multiprocessing.Pool(self.processes)
                results = pool.imap_unordered(self.send_requests, requests)
                pool.close()
                pool.join()
                cursor = conn.cursor()
                data = []

                # Check if the file exists
                if os.path.isfile(filepath):
                    # Read the existing CSV file into a DataFrame
                    existing_df = pd.read_csv(filepath)
                else:
                    # If the file doesn't exist, create a new DataFrame
                    existing_df = pd.DataFrame()
                    existing_df = pd.DataFrame(existing_df, columns=['record_id', 'longitude', 'latitude', 'date_value', 'T2M','RH2M'])

                for i, result in enumerate(results, 1):

                    record_id = result['_id']
                    longitude = result['geometry']['coordinates'][0]
                    latitude = result['geometry']['coordinates'][1]
                    t2m_max_records = result['properties']['parameter']['T2M']
                    rh2m_records = result['properties']['parameter']['RH2M']
                    t2m_max_dates = list(t2m_max_records.keys())

                    for t2m_max_date in t2m_max_dates:
                        t2m_max_value = t2m_max_records[t2m_max_date]
                        rh2m_value = rh2m_records[t2m_max_date]
                        t2m_max_date_formatted = datetime.datetime.strptime(t2m_max_date, '%Y%m%d').date()
                        t2m_max_date_formatted_str = t2m_max_date_formatted.strftime('%Y-%m-%d')
                        data.append([record_id, longitude, latitude, t2m_max_date_formatted_str,t2m_max_value,rh2m_value])
                    sql_update_query = "UPDATE nasa_geopoints_unique SET processed =1 WHERE trim(id)={id}".format(id=record_id)
                    cursor.execute(sql_update_query)
                    conn.commit()
                    sys.stderr.write('\rExporting {0:%}'.format(i / requests_total))
                cursor.close()
                conn.close()

                df = pd.DataFrame(data, columns=['record_id', 'longitude', 'latitude', 'date_value', 'T2M','RH2M'])
                # Concatenate the existing and new DataFrames
                combined_df = pd.concat([existing_df, df], ignore_index=True)

                # Save the combined DataFrame to the CSV file without writing column headings if it's not the first time
                combined_df.to_csv(filepath, mode='w',index=False)

                self.times["Total Script"] = round((time.time() - Start_Time), 2)
                print("\n")
                print("Total Script Time:", self.times["Total Script"])

    def execute_rh(self):
        requests = nasa.build_request()
        # print(requests)
        Start_Time = time.time()
        requests_total = len(requests)
        filepath = "RH_MIN.csv"
        if requests is None:
            print('No Request To Process')
        else:

            conn = mysql.connector.connect(**self.db_config)
            if conn.is_connected:
                # print(requests)
                pool = multiprocessing.Pool(self.processes)
                results = pool.imap_unordered(self.send_requests, requests)

                pool.close()
                pool.join()
                cursor = conn.cursor()
                data_rh = []



                for i, result in enumerate(results, 1):
                    sys.stderr.write('\rExporting {0:%}'.format(i / requests_total))
                    record_id = result['_id']
                    longitude = result['geometry']['coordinates'][0]
                    latitude = result['geometry']['coordinates'][1]
                    rh2m_records = result['properties']['parameter']['RH2M']

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
                        rh2m_date_unique_formatted = datetime.datetime.strptime(rh2m_date_unique, '%Y%m%d').date()
                        rh2m_date_unique_formatted_str = rh2m_date_unique_formatted.strftime('%Y-%m-%d')
                        data_rh.append([record_id, longitude, latitude, rh2m_date_unique_formatted_str, rh2m_min_value])


                    sql_update_query = "UPDATE nasa_geopoints_unique SET processed =1 WHERE trim(id)={id}".format(
                        id=record_id)
                    cursor.execute(sql_update_query)
                    conn.commit()
                cursor.close()
                conn.close()

                df = pd.DataFrame(data_rh, columns=['record_id', 'longitude', 'latitude', 'date_value', 'RH2M_MIN'])

                # Check if the file exists
                if os.path.isfile(filepath):
                    df.to_csv(filepath, mode='a', header=False, index=False)
                else:
                    # If the file doesn't exist, create a new DataFrame
                    df.to_csv(filepath, mode='a', header=True, index=False)


                self.times["Total Script"] = round((time.time() - Start_Time), 2)
                print("\n")
                print("Total Script Time:", self.times["Total Script"])

    def send_requests(self, collection):
        request, record_id = collection
        results = requests.get(url=request, verify=False, timeout=30.00).json()
        results['_id'] = record_id
        return results

    def merge_tmax_rhmin(self):
        df_t2m_max = pd.read_csv('T2M_MAX.csv')
        df_rh_min = pd.read_csv('RH_MIN.csv')
        df_t2m_max_rh_min = pd.merge(df_t2m_max, df_rh_min, on=['record_id','date_value','longitude', 'latitude'], how='left')
        df_t2m_max_rh_min.to_csv('T2M_MAX_RH_MIN.csv', mode='w', index=False)

    def compute_thi(self):
        df_thi = pd.read_csv('T2M_MAX_RH_MIN.csv')
        df_thi['THI'] = (1.8 * df_thi['T2M_MAX'] + 32) - ((0.55 - 0.0055 * df_thi['RH2M_MIN']) * (1.8 * df_thi['T2M_MAX'] - 26.8))
        df_thi['THI'] = df_thi['THI'].round(2)
        df_thi.to_csv('THI.csv', mode='w', index=False)

    def event_linkage(self):

        conn = mysql.connector.connect(**self.db_config)
        if conn.is_connected:
            try:
                cursor = conn.cursor()
                sql_evnt_geopoints= "SELECT event_id,event_date date_value,longitude_edit longitude,latitude_edit latitude FROM nasa_geopoints_all"
                df_sql_evnt_geopoints = pd.read_sql(sql_evnt_geopoints, conn)
                df_sql_evnt_geopoints.to_csv('geo_points.csv', mode='w', index=False)

                df_thi = pd.read_csv('THI.csv')
                df_geo_points = pd.read_csv('geo_points.csv')

                df_event_thi = pd.merge(df_geo_points, df_thi,on=['longitude','latitude','date_value'], how='inner')

                df_test_day = pd.read_csv('testday-2023-09-19.csv')

                df_final_output = pd.merge(df_test_day, df_event_thi, on=['event_id'], how='left')

                df_final_output.to_csv('final_results.csv', mode='w', index=False)
                cursor.close()

                conn.close()
            except mysql.connector.Error as err:
                print(f"Error: {err}")


if __name__ == '__main__':
    # nasa = Nasa(host=os.environ.get('DB_HOST'), username=os.environ.get('DB_USER'),
    #             password=os.environ.get('DB_PASSWORD'), database=os.environ.get('DB_SCHEMA'))

    nasa = Nasa(host='127.0.0.1', username='root',password='G1d30nk0sg3189', database='weather_data')

    # nasa.build_db_objects() #Create Database Objects
    # nasa.purge_db_objects() # Housekeeping
    # nasa.fetch_all_geopoints() #Fetch All Geopoints
    # nasa.fetch_distinct_geopoints()  # Fetch Distinct Geopoints
    nasa.execute()
    # count = 0
    # while count < 3:
    #     try:
    #         nasa.execute_rh()
    #         count += 1
    #     except Exception as e:
    #         print(f"An error occurred: {e}")
    # nasa.execute_rh()
    # nasa.merge_tmax_rhmin()
    # nasa.compute_thi()
    # nasa.event_linkage()



