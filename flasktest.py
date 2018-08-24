# import libraries
from flask import Flask, jsonify
import urllib.request as urlreq
import json
import datetime
import pandas as pd
import pymysql.cursors
from math import sin, cos, sqrt, atan2, radians
from datetime import datetime

app = Flask(__name__)

# Connect to the database
connection = pymysql.connect(host='127.0.0.1',
                             user='root',
                             password='1234',
                             db='EventGPS',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

#source_loc = []

@app.route('/EventGPS-api/googlemaps-api/route/<source>/<dest>', methods=['GET'])
def get_route(source,dest):
    #source = 'Gaiety%20Theatre'
    #dest = 'Olympia%20Theatre'
    #print(source)
    #print(dest)
    global source_loc
    v=[]
    source_loc= []
    time=[]
    key = 'AIzaSyAx20Dxy6BzGGZRuKxX-JREJ0_2fgwb2qY'
    url = urlreq.urlopen(
        "https://maps.googleapis.com/maps/api/directions/json?origin=" + source + "&destination=" + dest + "&alternatives=true&key=" + key)  # query
    jdata = url.read().decode()  # reading and decoding the raw data
    data1 = json.loads(jdata)
    for i in range(len(data1['routes'])):
        v.append(''.join(list(data1['routes'][i]['overview_polyline'].values())))
        source_loc1= []
        #time1=[]
        #time1.append(data1['routes'][i]['legs'][0]['duration']['text'])
        time1=data1['routes'][i]['legs'][0]['duration']['text']
        source_loc1.append(data1['routes'][i]['legs'][0]['start_location'])
        a = len(data1['routes'][i]['legs'][0]['steps'])
        # data1['routes'][0]['legs'][0]['steps']
        for x in range(a):
            source_loc1.append(data1['routes'][i]['legs'][0]['steps'][x]['end_location'])
        source_loc.append(source_loc1)
        time.append(time1)
    df = pd.DataFrame(source_loc)
    df1=df.T
    df1.fillna(method='ffill',inplace=True)
    p=df1.to_json()
    df2=pd.DataFrame([v,time])
    #df2=df2.T
    return (df2.to_json())

@app.route('/EventGPS-api/sql/result/<date>/<time>', methods=['GET'])
def get_tasks(date,time):
    date1 = datetime.strptime(date, "%Y-%m-%d").date()
    time1 = datetime.strptime(time, "%H:%M:%S").time()
    with connection.cursor() as cursor:
            # Create a new record
        cursor.execute("SELECT * from details1 where Date = %s and ((PreStartTime <= %s and StartTime > %s) or (EndTime <= %s and PostEndTime > %s)) and (PreStartTime != '00:00:00' and EndTime != '00:00:00' and PostEndTime != '00:00:00')",
                (date1, time1, time1, time1, time1))
        result = cursor.fetchall()
        print(result)


    #finally:
     #   connection.close()

    l = []
    for i in range(len(result)):
        loc = {'lat': float(result[i]['Latitude'])}
        loc.update({'lng': float(result[i]['Longitude'])})
        l.append(dict(loc))

    R = 6373.0
    val = []
    for j in range(len(l)):
        print("distance with venue : ", (result[j]['VenueName']))
        print(source_loc)
        for i in range(len(source_loc)):
            lat1 = radians(l[j]['lat'])
            lon1 = radians(l[j]['lng'])
            for k in range(len(source_loc[i])):
                lat2 = radians(source_loc[i][k]['lat'])
                lon2 = radians(source_loc[i][k]['lng'])
                dlon = lon2 - lon1
                dlat = lat2 - lat1
                a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
                c = 2 * atan2(sqrt(a), sqrt(1 - a))
                distance = R * c
                if distance <= result[j]['Radius']:
                    val.append(result[j])
                    print("Result:", distance)
    for i in range(len(val)):
        val[i]['EndTime'] = str(val[i]['EndTime'])
        val[i]['PostEndTime'] = str(val[i]['PostEndTime'])
        val[i]['StartTime'] = str(val[i]['StartTime'])
        val[i]['PreStartTime'] = str(val[i]['PreStartTime'])
        val[i]['Radius'] = str(val[i]['Radius'])
    df2=pd.DataFrame(val)
    if len(df2) > 1:
        df2.drop_duplicates(inplace=True)
    df2=df2.T
    df3=df2.to_json()
    return (df3)

@app.route('/EventGPS-api/events/all/<date>', methods=['GET'])
def get_events(date):
    date1 = datetime.strptime(date, "%Y-%m-%d").date()
    with connection.cursor() as cursor:
            # Create a new record
        cursor.execute("SELECT * from details1 where Date = %s",(date1))
        result = cursor.fetchall()
        #print(result)
    for i in range(len(result)):
        result[i]['Date'] = str(result[i]['Date'])
        result[i]['EndTime'] = str(result[i]['EndTime'])
        result[i]['PostEndTime'] = str(result[i]['PostEndTime'])
        result[i]['StartTime'] = str(result[i]['StartTime'])
        result[i]['PreStartTime'] = str(result[i]['PreStartTime'])
        result[i]['Radius'] = str(result[i]['Radius'])

    df2=pd.DataFrame(result)
    df2=df2.T
    df3=df2.to_json()
    return (df3)

if __name__ == '__main__':
    app.debug=False
    app.run(host='0.0.0.0',port=443)