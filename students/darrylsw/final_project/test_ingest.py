"""
Test db_ingest.py
"""

import os
from db_ingest import get_data, update_data, group_data, import2db

filename = './test-data/datastore_20161207.csv'
#ds_df = get_data('./test-data/datastore_20161207.csv')

def test_get_data():
	ds_df = get_data(filename)
	assert "Cluster" in ds_df.head(1)
	assert "Datastore" in ds_df.head(1)
	assert 20 == ds_df['Cluster'].count()
	assert "1.48141" in ds_df.head(1).to_string()
	print

def test_update_data():
	ds_df = get_data(filename)
	update_df = update_data(ds_df)
	assert "500" in update_df['Used'].head(1).to_string()
	assert "0.976562" in update_df['Capacity_tb'].head(1).to_string()
	assert "0.78125" in update_df['Allocated_tb'].head(1).to_string()
	assert "0.488281" in update_df['Used_tb'].head(1).to_string()

def test_group_data():
	ds_df = get_data(filename)
	update_df = update_data(ds_df)
	group_df = group_data(update_df, "VMFS")
	
	assert "7.81" in group_df['Capacity_tb'].head(1).to_string()
	assert "4.0039" in group_df['Allocated_tb'].head(1).to_string()
	assert "5.8593" in group_df['Used_tb'].head(1).to_string()
	assert ".75" in group_df['Used_pct'].head(1).to_string()
	assert ".51" in group_df['Allocated_pct'].head(1).to_string()

def test_import2db():
	ds_df = get_data(filename)
	update_df = update_data(ds_df)
	group_df_vmfs = group_data(update_df, "VMFS")

	import2db(group_df_vmfs)
	assert False



# def test_add_date():
# 	ds_df = get_data('./test-data/datastore_20161207.csv')
# 	date=add_date('./test-data/datastore_20161207.csv', ds_df)
# 	print (date)

# 	now=add_date('./test-data/datastore_20161207.csv', ds_df, "12-1-2016")
# 	assert False




