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

                sql_insert_query = "call sp_update_rhmin0()"
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
