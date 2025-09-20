import requests
import mysql.connector

sites = [
    "https://wikipedia.org",
    "https://example.org",
    "https://google.com",
    "https://youtube.com"
]

mydb = mysql.connector.connect(user='root', password='pass',
                              host='127.0.0.1',
                              auth_plugin='pass')
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE test")
mycursor.execute("CREATE TABLE test customers (site VARCHAR(255), content VARCHAR(255))")

for site in sites:
    response = requests.get(site)
    mycursor.execute("INSERT INTO test sites (site, content) VALUES (%s, %s)", (site, response.content))
mydb.commit()
