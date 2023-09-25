# Databricks notebook source
# MAGIC %md
# MAGIC ####This notebook contains all the common functions used in this case study####

# COMMAND ----------

##### Create Unique Prefix#####
def unique_prefix(geocode,previous_geocode,next_geocode):
    original_pos_prev=0
    original_pos_next=0
    for original_pos_prev in range(len(geocode)):
        if geocode[original_pos_prev]==previous_geocode[original_pos_prev]:
            original_pos_prev+=1
        else:
            break
    for original_pos_next in  range(len(geocode)):
        if geocode[original_pos_next]==next_geocode[original_pos_next]:
            original_pos_next+=1
        else:
            break
    if original_pos_prev > original_pos_next:
        return geocode[0:original_pos_prev+1:1]
    else:
        return geocode[0:original_pos_next+1:1]


# COMMAND ----------

