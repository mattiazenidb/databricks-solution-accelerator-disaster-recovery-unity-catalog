# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC - Clone all content of metastore
# MAGIC   - Storage credentials
# MAGIC   - External locations
# MAGIC   - Catalogs, Databases, Tables, Views
# MAGIC   - ACLs
# MAGIC - What about data? It only works with external tables

# COMMAND ----------

def get_catalogs(source_workspace_url, source_workspace_header):
  response = requests.get('{}/catalogs/'.format(source_workspace_url), headers=source_workspace_header)
  return response.json()

def get_schemas(source_workspace_url, source_workspace_header, catalog_name):
  response = requests.get('{}schemas?catalog_name={}'.format(source_workspace_url, catalog_name), headers=source_workspace_header)
  return response.json()

def get_tables(source_workspace_url, source_workspace_header, catalog_name, schema_name):
  response = requests.get('{}tables?catalog_name={}&schema_name={}'.format(source_workspace_url, catalog_name, schema_name), headers=source_workspace_header)
  return response.json()

# COMMAND ----------

def post_catalog(source_workspace_url, source_workspace_header, element):
  response = requests.post('{}/catalogs/'.format(source_workspace_url), json=element, headers=source_workspace_header)
  return response.json()

def post_schema(source_workspace_url, source_workspace_header, element):
  response = requests.post('{}schemas'.format(source_workspace_url), json=element, headers=source_workspace_header)
  return response.json()

def post_table(source_workspace_url, source_workspace_header, element):
  response = requests.post('{}tables'.format(source_workspace_url), json=element, headers=source_workspace_header)
  return response.json()

# COMMAND ----------

def create_catalogs(destination_workspace_url, destination_workspace_header, catalogs):
  for element in process_catalogs(catalogs):
    post_catalogs(element)
    print(response)

# COMMAND ----------

def process_catalogs(catalogs_get_output):
  if 'catalogs' not in catalogs_get_output:
    return
  
  catalogs = []
  
  for catalog_in_output in catalogs_get_output['catalogs']:
    catalog = {}
    catalog['name'] = catalog_in_output['name']
    catalog['owner'] = catalog_in_output['name']
    catalogs.append(catalog)
  return catalogs

# COMMAND ----------

import requests

source_workspace_url = dbutils.widgets.get('source_workspace_url')
source_workspace_header = {'Authorization': 'Bearer ' + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)}

destination_workspace_url = dbutils.widgets.get('destination_workspace_url')
destination_workspace_header = {'Authorization': 'Bearer ' + dbutils.widgets.get('destination_workspace_token')}

catalogs = get_catalogs(source_workspace_url, source_workspace_header)
schemas = get_schemas(source_workspace_url, source_workspace_header, 'catalog_junichi_maruyama')
tables = get_tables(source_workspace_url, source_workspace_header, 'catalog_junichi_maruyama', 'manufacturing_junichi_maruyama')

#print(catalogs)
#print(schemas)
#print(tables)
#create_catalogs(destination_workspace_url, destination_workspace_header, catalogs)

element = {'name': 'manufacturing_junichi_maruyama', 'owner': 'junichi.maruyama@databricks.com', 'catalog_name': 'catalog_junichi_maruyama'}
post_schema(destination_workspace_url, destination_workspace_header, element)

element = {'name': 'pump', 'owner': 'junichi.maruyama@databricks.com', 'catalog_name': 'catalog_junichi_maruyama', 'schema_name': 'manufacturing_junichi_maruyama', 'table_type': 'MANAGED', 'data_source_format': 'DELTA', 'properties': {'delta.lastCommitTimestamp': '1657105958000', 'delta.lastUpdateVersion': '0', 'delta.minReaderVersion': '1', 'delta.minWriterVersion': '2'}, 'columns': [{'name': 'country', 'type_text': 'string', 'type_json': '{"name":"country","type":"string","nullable":true,"metadata":{}}', 'type_name': 'STRING', 'type_precision': 0, 'type_scale': 0, 'position': 0, 'nullable': True}, {'name': 'manufacturer', 'type_text': 'string', 'type_json': '{"name":"manufacturer","type":"string","nullable":true,"metadata":{}}', 'type_name': 'STRING', 'type_precision': 0, 'type_scale': 0, 'position': 1, 'nullable': True}, {'name': 'pumpId', 'type_text': 'int', 'type_json': '{"name":"pumpId","type":"integer","nullable":true,"metadata":{}}', 'type_name': 'INT', 'type_precision': 0, 'type_scale': 0, 'position': 2, 'nullable': True}, {'name': 'region', 'type_text': 'string', 'type_json': '{"name":"region","type":"string","nullable":true,"metadata":{}}', 'type_name': 'STRING', 'type_precision': 0, 'type_scale': 0, 'position': 3, 'nullable': True}]}
post_table(destination_workspace_url, destination_workspace_header, element)
