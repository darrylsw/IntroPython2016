"""
Test db_ingest.py
"""

import os
from db_ingest import create_json, df, storage_df, import2db

filename = './test-data/datastore_20161207.csv'

def test_get_data():
	ds_df = storage_df(filename)
	assert "Capacity" in ds_df.vmfs_data
	assert "1.4814" in ds_df.vmfs_data['Date'].head(1).to_string()

def test_group_data():
	ds_df = storage_df(filename)

	assert "vpt" == ds_df.tag_team 
	assert "storage" == ds_df.tag_object

	assert "7.8125" in ds_df.vmfs_data['Capacity_tb'].head(1).to_string()
	assert "4.0039" in ds_df.vmfs_data['Allocated_tb'].head(1).to_string()
	assert "5.8593" in ds_df.vmfs_data['Used_tb'].head(1).to_string()
	assert ".75" in ds_df.vmfs_data['Used_pct'].head(1).to_string()
	assert ".51" in ds_df.vmfs_data['Allocated_pct'].head(1).to_string()

	assert "2.441406" in ds_df.nfs_data['Capacity_tb'].head(1).to_string()
	assert "2.246094" in ds_df.nfs_data['Allocated_tb'].head(1).to_string()
	assert "0.488281" in ds_df.nfs_data['Used_tb'].head(1).to_string()
	assert "0.2" in ds_df.nfs_data['Used_pct'].head(1).to_string()
	assert "0.92" in ds_df.nfs_data['Allocated_pct'].head(1).to_string()


def test_create_json():

	json = create_json('Capacity_tb', 'vpt', 'storage', 'VMFS', 'cluster1', 1481415659.0, 1000)

	assert "Capacity_tb" in json[0]['measurement']
	assert "vpt" in json[0]['tags']['team']
	assert "storage" in json[0]['tags']['object']
	assert "VMFS" in json[0]['tags']['dstype']
	assert "cluster1" in json[0]['tags']['cluster']
	assert 1481415659.0 == json[0]['time']
	assert 1000 == json[0]['fields']['value']


def test_import2db():
	ds_df = storage_df(filename)
	storage_json_list = ds_df.create_json_list()
	import2db(storage_json_list)

	







