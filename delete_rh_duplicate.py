import urllib3
import mysql.connector
from mysql.connector import Error

urllib3.disable_warnings()


class Process:

    def __init__(self):
        # Mysql Database Settings
        self.host = 'localhost'
        self.database = 'power_nasa_weather'
        self.user = 'root'
        self.password = 'G1d30nk0sg3189'

    def execute(self):
        try:
            connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                                 password=self.password)
            if connection.is_connected():
                cursor = connection.cursor()

                sql_insert_query = "DELETE t1 FROM rm_min_data t1 INNER JOIN rm_min_data t2 WHERE  t1.id < t2.id AND t1.data_point_id = t2.data_point_id and t1.date_value = t2.date_value ;"

                cursor.execute(sql_insert_query)
                connection.commit()

        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()


if __name__ == '__main__':
    Process().execute()
