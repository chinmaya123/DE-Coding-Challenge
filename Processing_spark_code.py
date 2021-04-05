#!/usr/bin/env python
# coding: utf-8

# In[10]:


from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,StringType,DecimalType
from functools import reduce


# In[5]:


df=spark.read.json('/Users/chinmaya/Documents/data_pipeline/filtered_data.json')


# In[6]:


df1=df.select(F.col('base'),F.col('start_at'),F.col('end_at'),
    F.explode(F.array(F.col('rates')))
  )


# In[7]:


field_names = [field.name for field in next(field for field in df1.schema.fields if field.name=="col").dataType.fields]


# In[8]:


dfs = [df1.select(F.lit(1).alias('id'),F.lit(field_name).alias("dt"), F.col(f"col.{field_name}.CAD").alias('CAD'), F.col(f"col.{field_name}.GBP").alias('GBP'),
       F.col(f"col.{field_name}.JPY").alias('JPY'),F.col(f"col.{field_name}.NZD").alias('NZD'),F.col(f"col.{field_name}.USD").alias('USD'),
       ) for field_name in field_names]


# In[11]:


resultant_df=reduce(lambda x,y: x.union(y), dfs)


# In[12]:


resultant_df_without_dup=resultant_df.dropDuplicates()


# In[22]:


resultant_df_without_dup.write.format("jdbc")   .mode("overwrite")   .option("url", "jdbc:sqlite:/Users/chinmaya/Documents/sqllite/test.db")   .option("dbtable", "history")   .save()


# In[19]:


df1.select(F.lit(1).alias('id'),F.col('start_at').alias('start_date'),F.col('end_at').alias('end_date'),F.col('base')).write.format("jdbc")   .mode("overwrite")   .option("url", "jdbc:sqlite:/Users/condenast/Documents/sqllite/test.db")   .option("dbtable", "Currency")   .save()


# In[25]:





# In[ ]:




