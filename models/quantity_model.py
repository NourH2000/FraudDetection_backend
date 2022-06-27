#!/usr/bin/env python
# coding: utf-8

# In[1]:


from cassandra.cluster import Cluster
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

from pyspark.sql import SQLContext
import numpy as np
from pyspark.sql.functions import split, col


# In[3]:


#new spark session 
spark = SparkSession.builder.appName('PPA detection').getOrCreate()


# In[4]:


#connection to cassandra database and cnas keyspace
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('cnas')


# In[6]:


#get parameters (start_date and end_date)
import sys

date_debut=sys.argv[1]
date_fin = sys.argv[2]

query = "SELECT *  FROM cnas  WHERE date_paiement >= '{}' AND date_paiement <= '{}' LIMIT 200 ALLOW FILTERING;".format(date_debut,date_fin)
rows = session.execute(query)
#print the data : print(rows)


# In[ ]:


#transform the cassandra.cluster ( rows) to pandas dataframe to make some changes
dftable = pd.DataFrame(list(rows))
# print the data : print (df)


# In[ ]:


# transformation :

#remplacer None avec -1 ( aucune affection) dans la coloumn affection
dftable.affection.fillna(value=-1, inplace=True)

#remplacer None avec 0 ( aucune quantitée rejetée ) dans la coloumn qte_rejet
dftable.qte_rejet.fillna(value=0, inplace=True)


# delete rows where the quantite_med == 0
dftable.drop(dftable[dftable['quantite_med'] == 0].index, inplace = True)

#delete rejected quantity , but before this , we need to save the rows having quantity rejected > 0
#df_rejected = dftable[dftable['qte_rejet'] > 0]
#dftable.drop(dftable[dftable['qte_rejet'] > 0].index, inplace = True)


#remplacer None avec 0 ( aucune durée spécifiée ) dans la coloumn duree_traitement
dftable.duree_traitement.fillna(value=0, inplace=True)

#change the type of some lines 
dftable = dftable.astype({"affection": str})
dftable = dftable.astype({"fk": float})
dftable = dftable.astype({"age": int})
dftable = dftable.astype({"date_paiement": str})



# In[ ]:


# garder les coloumns qu'on est besoin 
dftable=dftable[['id','fk','codeps','affection','age','applic_tarif','date_paiement','num_enr','sexe','ts','quantite_med','qte_rejet']]
# print the columns that we need : dftable.info()
#add column 'count_medicament' that gives the count of every num_enr
dftable['count_Medicament'] = dftable.groupby('num_enr')['num_enr'].transform('count')
# In[ ]:


# split the table into two table : rejected one and accepted one
rejected = dftable[dftable['qte_rejet'] > 0]
accepted = dftable[dftable['qte_rejet'] == 0]


# In[ ]:


#Create spark dataframe for the two pandas table (accepted and rejected)
sparkdf = spark.createDataFrame(accepted)
#rejected_sparkdf = spark.createDataFrame(rejected)


# In[ ]:


#transform the affection column to array of int ( splited by ',')
sparkdf = sparkdf.withColumn("affection", split(col("affection"), ",").cast("array<int>"))


# In[ ]:


#sort the affection array 
import pyspark.sql.functions as F
sparkdf = sparkdf.withColumn('affection', F.array_sort('affection'))


# In[ ]:


## put the age in ranges
from pyspark.sql.functions import udf
@udf("String")
def age_range(age):
    if age >= 0 and age <= 5:
        return '0-5'
    elif age > 5 and age <= 10:
        return '6-10'
    elif age > 10 and age <= 16:
        return '11-16' 
    elif age > 16 and age <= 24:
        return '17-24' 
    elif age > 24 and age <= 60:
        return '25-60' 
    elif age > 60 and age <= 76:
        return '61-76' 
    else:
        return '75+'
    


sparkdf = sparkdf.withColumn("age", age_range(col("age")))


# In[ ]:


# transform the affection column to a string again ( so we can index it)
from pyspark.sql.functions import col, concat_ws
sparkdf = sparkdf.withColumn("affection",
   concat_ws(",",col("affection")))


# In[ ]:


### Handling Categorical Features
from pyspark.ml.feature import StringIndexer
indexer=StringIndexer(inputCols=["sexe","applic_tarif","ts","affection","age"],outputCols=["sex_indexed","applic_tarif_indexed",
                                                                         "ts_indexes","affection_indexes","age_indexes"])
df_r=indexer.setHandleInvalid("keep").fit(sparkdf).transform(sparkdf)


# In[ ]:


# put the data into one vector 
from pyspark.ml.feature import VectorAssembler
featureassembler=VectorAssembler(inputCols=['id','fk','age_indexes','sex_indexed','affection_indexes',
                          'ts_indexes','quantite_med',],outputCol="Features")
output=featureassembler.transform(df_r)


# In[ ]:


#prepare the data to fit it to the model 
finalized_data=output.select("Features","quantite_med")


# In[ ]:


# call and fit the model 
from pyspark.ml.regression import LinearRegression
##train test split
train_data,test_data=finalized_data.randomSplit([0.75,0.25])
regressor=LinearRegression(featuresCol='Features', labelCol='quantite_med', maxIter=10, regParam=0.3, elasticNetParam=0.8)
regressor=regressor.fit(train_data)


# In[ ]:


# result
#print("Coefficients are : %s" % str(regressor.coefficients ))
#print("Intercept: %s" % str(regressor.intercept))


# In[ ]:
### Predictions
pred_results=regressor.evaluate(test_data)
## Final comparison
# print the prediction vs the vector : pred_results.predictions.show()

## get the Training_date
from datetime import date
today = date.today()

# In[ ]:

### Performance Metrics
pred_results.r2,pred_results.meanAbsoluteError,pred_results.meanSquaredError


# In[ ]:


# print the final predicted quantity ( rounded ) vs the real quantity
# get the prediction column
prediction = pred_results.predictions
# round the prediction
from pyspark.sql.functions import round, col
prediction = prediction.select("Features"  , "quantite_med", round(col('prediction'))).withColumnRenamed("round(prediction, 0)","Predicted_quantity").withColumnRenamed("quantite_med","Descripted_quantity")
# keep the suspected line
prediction = prediction.where("quantite_med > round(prediction, 0) ")
#prediction.show(50)

# add a predicted rejection 
Final_result = prediction.withColumn("Rejection", prediction.Descripted_quantity - prediction.Predicted_quantity)
Final_result.show()
#Final_result.count()
# joint the original data with the resukt_data to get all the informations
Final_result = Final_result.join(output,Final_result.Features ==  output.Features,"inner")
Final_result.select("date_paiement").show()

#add Count_medicament_suspected that count the count of suspected time this drug shows
from pyspark.sql import Window
Final_result = Final_result.withColumn('Count_medicament_suspected', F.count(
    'num_enr').over(Window.partitionBy('num_enr')))

##connection to cassandra database and fraud keyspace
session2 = cluster.connect('fraud')
#get the last id of training 
bigId = session2.execute("select * from params where param='Max_Id_Entrainement_Quantity';")
id_training = bigId.one().value

#iterate the data (insert it into cassandra keyspace) : 
data_collect = Final_result.collect()
query = "INSERT INTO Quantity_result (id , fk , no_assure , id_entrainement , quantite_med , quantite_predicted , qte_rejet_predicted , count_medicament , count_medicament_suspected , num_enr , date_entrainement , date_paiement ) VALUES (now() ,%s, %s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s ,%s )"
num_assuree = 1
for row in data_collect:
    # while looping through each
    # row printing the data of Id, Name and City
    print(row["fk"])
    future = session2.execute(query, [row["fk"] ,num_assuree , id_training , row["Descripted_quantity"] , row["Predicted_quantity"] ,row["Rejection"] ,row["count_Medicament"] ,row["Count_medicament_suspected"] , row["num_enr"] , today , row["date_paiement"] ])
    num_assuree = num_assuree +1

#Increment the id of training 
session2.execute("UPDATE params SET value = value + 1 WHERE param ='Max_Id_Entrainement_Quantity' ;")

    

