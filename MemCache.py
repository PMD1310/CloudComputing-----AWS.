import csv
import sys
from flask import Flask,render_template,request
import pymysql  #connect to rds--mysql
import time   #query time
import cStringIO
import pymysql
from flask import Flask
from flask import render_template
from flask import request
import glob
import os
import time
import datetime
import redis
import hashlib
import pickle as cPickle
import json

#Database Connection Parameters.

ACCESS_KEY_ID = 'AKIAIITB5NIDXEGV5TQQ'
ACCESS_SECRET_KEY = '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'
hostname = 'mydatabase.c0v8bn6kjtk4.us-east-2.rds.amazonaws.com'
username = 'pramod1310'
password = 'TolstoyOrwell07'
database = 'MyDatabase'
myConnection = pymysql.connect(host=hostname, user=username, passwd=password, db=database, autocommit=True,cursorclass=pymysql.cursors.DictCursor, local_infile=True)

print 'Database Connected'


APP_ROOT = os.path.dirname(os.path.abspath(__file__))
print(APP_ROOT)

app = Flask(__name__)

#Function call to Main program and its definition
@app.route('/')
def main():
    print "Welcome to the PhotoSharing Application"
    return render_template('memcache.html')

@app.route('/Upload',methods=['get','post'])
def Upload():
    return render_template('Upload.html')

@app.route('/uploadImage',methods=['get','post'])
def upload1():
    global file_name
    f = request.files['upload_files']
    file_name = f.filename
    print (file_name)
    titlename = request.form['file']
    #Get the filename from the user and open the windows explorer to select images
    global newfile
    newfile = os.path.abspath(file_name)
    print "new file down"
    print newfile
    return render_template('CSVExecution.html')

@app.route('/csvimport',methods=['get','post'])
def csvimport():
    print "i am here"
    cur = myConnection.cursor()
    cur.text_factory = str  # allows utf-8 data to be stored
    # traverse the directory and process each .csv file
    print newfile
    global split_file
    split_file = file_name.split('.')[0]
    tablename = split_file
    print "table name is"
    print tablename
    with open(newfile) as f:
        reader = csv.reader(f)
        header = True
        print "value"
        print tablename
        sql1 = "DROP TABLE IF EXISTS %s" %tablename
        print  sql1
        cur.execute(sql1)
        print cur.execute(sql1)
        for row in reader:
                # gather column names from the first row of the csv
                header = False
                line=row
                break
        column_name="( "
        sql="CREATE TABLE %s"%tablename
        for i in line:
                #sql = "%s", ".join(["%s varchar(50)"%i ])
                column_name+=i+" VARCHAR(50), "

        sql+=column_name+" id_no int  PRIMARY KEY NOT NULL AUTO_INCREMENT);"
        print sql
        cur.execute(sql)
        print "sql executed the create query"
        #sql3 = "ALTER TABLE " +tablename + " ADD id_no int  PRIMARY KEY NOT NULL AUTO_INCREMENT"
        #print sql3
        #cur.execute(sql3)
        #print row
        newline="\\\n"
        print newfile
        print "this is it"
        insert_str="""LOAD DATA LOCAL INFILE \'""" +newfile+ """\' INTO TABLE """ +tablename+ """ FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES;"""
        print insert_str
        cur.execute(insert_str)
        myConnection.commit()
        cur.close()
        return render_template("success.html")

@app.route('/Limit',methods=['get','post'])
def limit():
        limit = request.form['limit']
        query1 = "Select * from all_month where locationSource='" + limit + "';"
        starttime = time.time()
        print(starttime)
        with myConnection.cursor() as cursor:
            cursor.execute(query1)
        myConnection.commit()
        cursor.close()
        endtime = time.time()
        print('endtime')
        totalsqltime = endtime - starttime
        print(totalsqltime)
        return render_template("success.html", time1=totalsqltime)

@app.route('/querywithparam', methods=['POST'])
def querywithparam():
    	satavg1= request.form['satavg1']
	satavg2= request.form['satavg2']
	zip1 = request.form['zip1']
	zip2 = request.form['zip2']
	locquery="(select  count(distinct Education.INSTNM) AS INSTNM ,USZipcodes.city as city from USZipcodes INNER JOIN Education on USZipcodes.city  = Education.CITY where USZipcodes.zip between   "+zip1+" and "+zip2+" AND Education.SAT_AVG between  "+satavg1+" and "+satavg2+" GROUP BY USZipcodes.city);"
	print locquery
       	with myConnection.cursor() as cursor:
          cursor.execute(locquery)
          myConnection.commit()
	global result1
	result1 = []
	data = []
	global x1
	for row in cursor:
        	result1.append(row)
    	x1 = [x['INSTNM'] for x in result1]
    	x2 = [x['city'] for x in result1]
	global result2
    	result2 = []
    	print x1
    	print x2
    	print result2
    	for p in x2:
        	result2.append(p)
	print(result2) 
	cursor.close()   
       	return render_template('success.html',zipped_data=x1,zipped_data2=result2)

@app.route('/barexecution',methods=['POST'])
def barchartexecution():
	return render_template('BarGraph.html',zipped_data=x1,zipped_data2=result2)

@app.route('/piechartexecution',methods=['POST'])
def piechartexecution():
	return render_template('PieChart.html',zipped_data=x1,zipped_data2=result2)

@app.route('/linechartexecution',methods=['POST'])
def linechartexecution():
	return render_template('Line.html',zipped_data=x1,zipped_data2=result2)

@app.route('/scatterchartexecution',methods=['POST'])
def scatterchartexecution():
	return render_template('Scatter.html',zipped_data=x1,zipped_data2=result2)



#the main function to call the main definition first
if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')

