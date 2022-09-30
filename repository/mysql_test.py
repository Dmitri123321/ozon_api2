import pymysql


host = '194.87.112.103'
database = 'wb'
user = 'wbuser'
password = 'wbpassword'
port = 9000

connection = pymysql.connect(host=host, user= user, password=password, database=database, port=port)
connection.autocommit(True)
if connection.server_version:
    print(True)
cursor = connection.cursor(pymysql.cursors.DictCursor)

# cursor.execute("SELECT VERSION()")

# version = cursor.fetchone()
#
# print("Database version: {}".format(version[0]))
query = 'select * from companies'
# query = 'show tables'
cursor.execute(query)

version = cursor.fetchall()


print(version)