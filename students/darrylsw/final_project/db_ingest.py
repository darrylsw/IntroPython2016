#!/usr/bin/env python3


"""
takes a csv listing all the datastore information from our vmware clusters such as storage used, free, capacity and 
vms on each, sum up data based upon cluster and types of storage and import into a time series database, Influxdb. 
This was what I discussed in my lightning talk toward the beginning of the quarter. The data will be used by Grafana 
for different types of graphs.

Importing of data into the database isn't functioning yet. The script is using classes so it can "easily" be expanded
to accommodate other types of data such as vm and host counts, grouping by site, operating system, hardware,...

"""

import pandas as pd
import os

from influxdb import DataFrameClient, InfluxDBClient

class df:
	"""
	dataframe class for default data
	"""
	tag_team = "vpt"

	def __init__(self, input_file):
		pdata = pd.read_csv(input_file)
		ctime = os.path.getctime(input_file)
	
		# special actions for storage data
		if self.tag_object == "storage":
			pdata ['Used'] = pdata ['Capacity'] - pdata ['Free']
			pdata ['Capacity_tb'] = pdata['Capacity'] / 1024
			pdata ['Allocated_tb'] = pdata['Allocated'] / 1024
			pdata ['Used_tb'] = pdata['Used'] / 1024
		
			tmp_pdata = pdata.loc[pdata['Type'] == "VMFS"]
			self.vmfs_data = tmp_pdata.groupby(['Cluster']).sum()
			self.vmfs_data['Used_pct'] = self.vmfs_data['Used'] / self.vmfs_data['Capacity']
			self.vmfs_data['Allocated_pct'] = self.vmfs_data['Allocated'] / self.vmfs_data['Capacity']
			self.vmfs_data['Date'] = ctime
			self.vmfs_data.set_index(['Date'])

			tmp_pdata = pdata.loc[pdata['Type'] == "NFS"]
			self.nfs_data = tmp_pdata.groupby(['Cluster']).sum()
			self.nfs_data['Used_pct'] = self.nfs_data['Used'] / self.nfs_data['Capacity']
			self.nfs_data['Allocated_pct'] = self.nfs_data['Allocated'] / self.nfs_data['Capacity']
			self.nfs_data['Date'] = ctime
			self.nfs_data.set_index(['Date'])
		else: # our generic data
			self.data=pdata
			self.data['Date'] = ctime
			self.data.set_index(['Date'])

class storage_df (df):
	"""
	used for storage data 
	"""
	tag_object = "storage"

	def create_json_list (self):
		""""
		for storage data we want to track:
			used_tb, allocated_tb, capacity_tb, used_pct, allocated_pct
		"""
		json_list = []
		for tag_dstype in ["VMFS", "NFS"]:
			for cluster, row in self.vmfs_data.iterrows():
				json_used = create_json ("Used_tb", self.tag_team, self.tag_object, tag_dstype, cluster, int(row['Date']), row['Used_tb'])
				json_list.append (json_used)

				json_allocated = create_json ("Allocated_tb", self.tag_team, self.tag_object, tag_dstype, cluster, int(row['Date']), row['Allocated_tb'])
				json_list.append (json_allocated)

				json_capacity = create_json ("Capacity_tb", self.tag_team, self.tag_object, tag_dstype, cluster, int(row['Date']), row['Capacity_tb'])
				json_list.append (json_capacity)

				json_used_pct = create_json ("Used_pct", self.tag_team, self.tag_object, tag_dstype, cluster, int(row['Date']), row['Used_pct'])
				json_list.append (json_used_pct)

				json_allocated_pct = create_json ("Allocated_pct", self.tag_team, self.tag_object, tag_dstype, cluster, int(row['Date']), row['Allocated_pct'])
				json_list.append (json_allocated_pct)
		return json_list

class vm_df (df):
	"""
	future class for vm data
	"""
	tag_object = "vm"

class host_df (df):
	"""
	future class for host data
	"""
	tag_object = "host"

		
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
	 		"time": int(date),
	 		"fields": {
	 			"value": val
	 		}
	 	}
 	]
	return json

def import2db (json_list):
	"""
	take a list of json data and import it into our database
	Work in progress
	"""

	host = "localhost"
	port = "8086"
	user = ""
	password = ""
	dbname = "db"

	client = InfluxDBClient(host, port, user, password, dbname)

	for json in json_list:
		pass
		#print (json)
		# work in progress
		#client.write_points(json_used)
		#client.write_points(json_allocated)
		#client.write_points(json_capacity)
		#client.write_points(json_used_pct)
		#client.write_points(json_allocated_pct)

def main():
	storage_df_data = storage_df('test-data/datastore_20161207.csv')

if __name__ == '__main__':
	main()
	"""
	eventually want the python script to take the filename as an argument
	"""

