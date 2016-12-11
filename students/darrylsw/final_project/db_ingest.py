#!/usr/bin/env python3

import pandas as pd 
import time
import os

from influxdb import DataFrameClient

def get_data(input_file):
	pdata = pd.read_csv(input_file)
	pdata['Date'] = os.path.getctime(input_file)
	pdata.set_index(['Date'])
	return pdata

def update_data (pdata):
	pdata ['Used'] = pdata ['Capacity'] - pdata ['Free']
	pdata ['Capacity_tb'] = pdata['Capacity'] / 1024
	pdata ['Allocated_tb'] = pdata['Allocated'] / 1024
	pdata ['Used_tb'] = pdata['Used'] / 1024
	return pdata

def group_data (pdata, dstype):
	tmp_data = pdata.loc[pdata['Type'] == dstype]
	group_data = tmp_data.groupby(['Cluster']).sum()
	group_data['Used_pct'] = group_data['Used'] / group_data['Capacity']
	group_data['Allocated_pct'] = group_data['Allocated'] / group_data['Capacity']
	return group_data

def create_json (measurement, team, objecttype, dstype, cluster, date, val):
	json = [
	 	{
	 		"measurement": measurement,
	 		"tags": {
	 			"team": team,
	 			"object": objecttype,
	 			"dstype": dstype,
	 			"cluster": cluster
	 		},
	 		"time": date,
	 		"fields": {
	 			"value": val
	 		}
	 	}
 	]


def import2db (pdata):
	tag_team = "vpt"
	tag_object = "storage"
	#tag_type = dstype


	host = "localhost"
	port = "8086"
	user = ""
	password = ""
	dbname = "db"

	#for index, row in pdata.iterrows():
		#json_used = create_json ("Used_tb", tag_team, tag_object, row['Type'], row['Cluster'], row['Date'], row['Used_tb'])
		#json_allocated = create_json ("Allocated_tb", tag_team, tag_object, row['Type'], row['Cluster'], row['Date'], row['Allocated_tb'])
		#json_capacity = create_json ("Capacity_tb", tag_team, tag_object, row['Type'], row['Cluster'], row['Date'], row['Capacity_tb'])
		#json_used_pct = create_json ("Used_pct", tag_team, tag_object, row['Type'], row['Cluster'], row['Date'], row['Used_pct'])
		#json_allocated_pct = create_json ("Allocated_pct", tag_team, tag_object, row['Type'], row['Cluster'], row['Date'], row['Allocated_pct'])
		#print (json_used)

def main():
	all_data = get_data ('test-data/datastore_20161207.csv')

	all_data = update_data (all_data)

	nfs_data = group_data (all_data, "NFS")
	vmfs_data = group_data (all_data, "VMFS")



if __name__ == '__main__':
	main()

