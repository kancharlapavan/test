#!/usr/bin/env python
# coding: utf-8

# In[91]:


#Importing SparkSession
from  pyspark.sql import SparkSession
from  pyspark.sql.functions import *
#Creating Spark Session Object
spark = SparkSession.builder.appName('data manipulation').getOrCreate()


# In[92]:


#Reading csv dataset
df_sales = spark.read.csv('C:\\Users\\kanch\\Downloads\\Products\\Products\\sales.csv')


# In[93]:


df_sales.show(10)


# In[94]:


df_sales=spark.read.option('header','TRUE').csv('C:\\Users\\kanch\\Downloads\\Products\\Products\\sales.csv',inferSchema='TRUE')
df_sales.show()
df_sales.createOrReplaceTempView("df_sales")


# In[95]:


df_products = spark.read.csv('C:\\Users\\kanch\\Downloads\\Products\\Products\\products.csv')
df_products.show(10)


# In[96]:


df_products=spark.read.option('header','TRUE').csv('C:\\Users\\kanch\\Downloads\\Products\\Products\\products.csv',inferSchema='TRUE')
df_products.show(10)
df_products.createOrReplaceTempView("df_products")


# In[97]:


df_sellers=spark.read.csv('C:\\Users\\kanch\\Downloads\\Products\\Products\\sellers.csv')
df_sellers=spark.read.option('header','TRUE').csv('C:\\Users\\kanch\\Downloads\\Products\\Products\\sellers.csv',inferSchema='TRUE')
df_sellers.show(10)
df_sellers.createOrReplaceTempView("df_sellers")


# In[134]:


from pyspark.sql.functions import max
a=df_sales.join(df_products,df_sales.product_id == df_products.product_id, "inner").groupby([df_products.product_name,df_products.product_id]).agg(sum(df_sales.num_pieces_sold).alias("sales"))
a.show()


# In[124]:


highest_sales = a.select("product_id","sales").orderBy(desc("sales")).first()
highest_sales


# In[72]:





# In[ ]:




