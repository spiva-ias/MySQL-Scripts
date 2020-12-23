# -*- coding: utf-8 -*-
import mysql.connector
dbname = "contextual"
user = "monitor_rw"
password = "cZTWoQj9tLovDIKbXGkaWvirrZxuujWCcZFWmhPbKA5qonNTlegLdo6mV2JBgS2S"
host = "admantx-rds-cluster.cluster-cr1mveowdnqe.us-east-1.rds.amazonaws.com"

key_from = "f90f922140feb7c2ac3574c8dda94cea8b95db14cac0d2582409cab5c1cd9511" #TTD
key_to = "49d2dc07acd9eff540ded01867adfbcbc4139f5fd7cb0eed6c717edb10ffa8fd" #Amazon


print "connecting to the Database"
try:
    cnx = mysql.connector.connect(user=user, password=password,
                              host=host, database=dbname, connection_timeout=10)
    cursor = cnx.cursor()
except Exception, e:
    print "Error during the connection to DB:" + str(e)
    exit()


try:
    query = "SELECT id,name FROM contextual.profile where apikey = '" + str(key_from) + "'"
    cursor.execute(query)
    data = cursor.fetchone()
    id_from = data[0]
    name_from = data[1]

except Exception, e:
    print "Error retrieving id from Key_from:"  + str(e)
    exit()

try:
    query = "SELECT id,name FROM contextual.profile where apikey = '" + str(key_to) + "'"
    cursor.execute(query)
    data = cursor.fetchone()
    id_to = data[0]
    name_to = data[1]
except Exception, e:
    print "Error retrieving id from Key_to:" + str(e)
    exit()

print "Copying customer segments from Key: '" + name_from + "' to key '" + name_to +"'"


print "Getting IDs od Segments ..."


ids = set()
query = "SELECT segment_id FROM contextual.mtm_profile_segment where profile_id = " + str(id_from) + " AND published=true"
cursor.execute(query)
for c in cursor:
    id = c[0]
    ids.add(id)

for id in ids:
    try:
        query = "INSERT INTO contextual.mtm_profile_segment SET profile_id = " + str(id_to) + ", segment_id= " + str(id) + ",published=true"
        print query
        #cursor.execute(query)
        #cnx.commit()
    except Exception, e:
        print str(e)



cursor.close()
cnx.close()
