#!/bin/sh

IP = "localhost"
PORT = 8086
USER = "myuser"
PASSWORD = "mypass"

#run from remote
#echo "create user $USER"
#curl "http://$IP:$PORT/query" --data-urlencode "q=CREATE USER $USER WITH PASSWORD $PASSWORD WITH ALL PRIVILEGES"


#run locally
influx -execute 'CREATE USER myuser WITH PASSWORD mypass WITH ALL PRIVILEGES'
influx -execute 'SHOW DATABASES' -username  myuser -password mypass
influx -execute 'CREATE DATABASE mydb' -username  myuser -password mypass
