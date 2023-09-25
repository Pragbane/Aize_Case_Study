# Databricks notebook source
# MAGIC %md
# MAGIC ####This notebook covers all the security features used in this case study###

# COMMAND ----------

# MAGIC %md
# MAGIC ####Point1: Create all the secrets in Azure Key vault####
# MAGIC ####Point2: Create secret Scopes in Databricks and refer that secrets in creation of mounts

# COMMAND ----------

# MAGIC %md
# MAGIC ####Initialize the global param set####

# COMMAND ----------

# MAGIC %run "../Includes/Global_param_set"

# COMMAND ----------

dbutils.secrets.listScopes()
client_id = dbutils.secrets.get(scope = 'Aize_Secrets', key = 'Clientid')
tenant_id = dbutils.secrets.get(scope = 'Aize_Secrets', key = 'tenantid')
client_secret = dbutils.secrets.get(scope = 'Aize_Secrets', key = 'Clientsecret')


# COMMAND ----------

# MAGIC %md
# MAGIC #####Configuration set up########

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

pip install azure-storage-blob

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Script to create all the mount points in DBFS based on the container created in Azure blob storage###

# COMMAND ----------

from azure.storage.blob import BlobServiceClient
access_key=dbutils.secrets.get(scope='Aize_Secrets',key='databricksaccountkey')
blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=access_key)
#containers = blob_service_client.get_service_properties().list_containers()
containers = blob_service_client.list_containers(include_metadata=False)
for container in containers:
    container_name=container['name']
    dbutils.fs.mount(source= f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
                     mount_point= f"{mount_point}/{container_name}",
                     extra_configs=configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/aize_case_study/"))

# COMMAND ----------

