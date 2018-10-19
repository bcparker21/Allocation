#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import datetime
import numpy as np
import os, subprocess

# Get GTFS Directory Location
GTFS_Location0=raw_input("Input Folder Path: ")
GTFS_Location=GTFS_Location0+"/"

# Read in data
stop_times=pd.read_csv(GTFS_Location+'stop_times.txt')
stops=pd.read_csv(GTFS_Location+'stops.txt')
trips=pd.read_csv(GTFS_Location+'trips.txt')
routes=pd.read_csv(GTFS_Location+'routes.txt')
calendar=pd.read_csv(GTFS_Location+'calendar.txt')

# Merge stuff
stop_times_stops=pd.merge(stop_times,stops)
stop_times_stops_trips=pd.merge(stop_times_stops,trips)
stop_times_stops_trips_routes=pd.merge(stop_times_stops_trips,routes)
full_GTFS=pd.merge(stop_times_stops_trips_routes,calendar)
df=pd.DataFrame(full_GTFS)

# Convert departure_time to time values
df['departure_time']=pd.to_timedelta(df.departure_time,unit='h')
df['departure_time']=df['departure_time']/np.timedelta64(1,'h')

# Find the total span of for each trip_id
df1=df.groupby('trip_id').aggregate({'departure_time':['min','max']})
df1.columns = ['_'.join(col).strip() for col in df1.columns.values]
df1['trip_span']=df1['departure_time_max']-df1['departure_time_min']
df=pd.merge(full_GTFS,df1,on='trip_id')

# Convert daily spans to annual
df['monday']=df['monday']*52
df['tuesday']=df['tuesday']*52
df['wednesday']=df['wednesday']*52
df['thursday']=df['thursday']*52
df['friday']=df['friday']*52
df['saturday']=df['saturday']*52
df['sunday']=df['sunday']*52
df['annual_hours']=(df['trip_span']*df['monday'])+(df['trip_span']*df['tuesday'])+(df['trip_span']*df['wednesday'])+(df['trip_span']*df['thursday'])+(df['trip_span']*df['friday'])+(df['trip_span']*df['saturday'])+(df['trip_span']*df['sunday'])

# Sum trip annual hours by stop, lose unneeded columns
df=df.groupby(['stop_id']).sum(skipna=False,numeric_only=False)
df.drop(df.columns.difference(['stop_id','annual_hours']), 1, inplace=True)
df=pd.merge(stops,df,on='stop_id')
df.index

# TODO: Spatial join with City Limits shapefile

# TODO: Aggregate by City

# Export
folder_name=str(os.path.basename(GTFS_Location0))
filename="annual_hours_by_stop_{}.csv".format(folder_name)
df.to_csv(filename,index=False)
print('done!')