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

def create_catalogs(destination_workspace_url, destination_workspace_header, catalogs):
  for element in process_catalogs(catalogs):
    response = requests.post(destination_workspace_url, json=element, headers=destination_workspace_header)
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

source_workspace_url = 'https://adb-984752964297111.11.azuredatabricks.net/api/2.0/unity-catalog/'
source_workspace_header = {'Authorization': 'Bearer ' + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)}

destination_workspace_url = 'https://adb-8752583164848723.3.azuredatabricks.net/api/2.0/unity-catalog/'
destination_workspace_header = {'Authorization': 'Bearer ' + dbutils.widgets.get('destination_workspace_token')}

catalogs = get_catalogs(source_workspace_url, source_workspace_header)
schemas = get_schemas(source_workspace_url, source_workspace_header, 'catalog_junichi_maruyama')
tables = get_tables(source_workspace_url, source_workspace_header, 'catalog_junichi_maruyama', 'manufacturing_junichi_maruyama')

print(catalogs)
print(schemas)
print(tables)
#create_catalogs(destination_workspace_url, destination_workspace_header, catalogs)
