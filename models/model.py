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


spark = SparkSession.builder.appName('ppa').getOrCreate()


# In[4]:


cluster = Cluster(['127.0.0.1'])


# In[5]:


session = cluster.connect('cnas')


# In[6]:
import sys

date_debut=sys.argv[1]
date_fin = sys.argv[2]

query = "SELECT *  FROM cnas  WHERE date_paiement >= '{}' AND date_paiement <= '{}'  ALLOW FILTERING;".format(date_debut,date_fin)
rows = session.execute(query)

# In[7]:


rows
    


# In[8]:


df = pd.DataFrame(list(rows))


# In[10]:


dftable = df

#remplacer None avec -1 ( aucune affection) dans la coloumn affection
dftable.affection.fillna(value=-1, inplace=True)

#remplacer None avec Rien ( aucun motif de rejet ) dans la coloumn motif_rejet
#dftable.motif_rejet.fillna(value='rien', inplace=True)

#remplacer None avec 0 ( aucune quantitée rejetée ) dans la coloumn qte_rejet
dftable.qte_rejet.fillna(value=0, inplace=True)

# delete the REPCM coloumn
dftable = dftable.drop(columns=['repcm'])

# delete the motif_rejet coloumn
dftable = dftable.drop(columns=['motif_rejet'])

# delete the POSOLOGIE coloumn
dftable = dftable.drop(columns=['posologie'])

# delete rows where the quantite_med == 0
dftable.drop(dftable[dftable['quantite_med'] == 0].index, inplace = True)

#delete rejected quantity , but before this , we need to save the rows having quantity rejected > 0
#df_rejected = dftable[dftable['qte_rejet'] > 0]
#dftable.drop(dftable[dftable['qte_rejet'] > 0].index, inplace = True)

# delete the NO_LOT coloumn
dftable = dftable.drop(columns=['no_lot'])

#remplacer None avec 0 ( aucune durée spécifiée ) dans la coloumn duree_traitement
dftable.duree_traitement.fillna(value=0, inplace=True)

dftable = dftable.astype({"affection": str})
dftable = dftable.astype({"fk": float})
dftable = dftable.astype({"age": int})


# In[11]:


# garder les coloumns qu'on est besoin 
dftable=dftable[['id','fk','codeps','affection','age','applic_tarif','date_paiement','num_enr','sexe','ts','quantite_med','qte_rejet']]
dftable.info()


# In[17]:


sparkdf = spark.createDataFrame(dftable)
sparkdf.printSchema()
spark_rejected = sparkdf.filter(sparkdf.qte_rejet > 0)
sparkdf = sparkdf.filter(sparkdf.qte_rejet == 0)

  
#spark_rejected.select("qte_rejet").show()
#sparkdf.select("qte_rejet").show()


# In[18]:


#print(sparkdf.count())
#print(spark_rejected.count())


# In[21]:


sparkdf = sparkdf.withColumn("affection", split(col("affection"), ",").cast("array<int>"))
sparkdf.printSchema()

#df.sort(col("affection_splited").asc(),col("affection_splited").asc()).show(truncate=False)
import pyspark.sql.functions as F
sparkdf = sparkdf.withColumn('affection', F.array_sort('affection'))


# In[22]:


sparkdf.show()


# In[23]:


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
sparkdf.show()


# In[24]:


from pyspark.sql.functions import col, concat_ws
sparkdf = sparkdf.withColumn("affection",
   concat_ws(",",col("affection")))


# In[25]:


sparkdf.printSchema()


# In[26]:


### Handling Categorical Features
from pyspark.ml.feature import StringIndexer
indexer=StringIndexer(inputCols=["sexe","applic_tarif","ts","affection","age"],outputCols=["sex_indexed","applic_tarif_indexed",
                                                                         "ts_indexes","affection_indexes","age_indexes"])
df_r=indexer.setHandleInvalid("keep").fit(sparkdf).transform(sparkdf)


# In[27]:


df_r.columns


# In[28]:


from pyspark.ml.feature import VectorAssembler
featureassembler=VectorAssembler(inputCols=['id','fk','age_indexes','sex_indexed','affection_indexes',
                          'ts_indexes','quantite_med',],outputCol="Independent Features")
output=featureassembler.transform(df_r)


# In[29]:


output.select('Independent Features').show()


# In[30]:


output.show()


# In[31]:


finalized_data=output.select("Independent Features","quantite_med")
finalized_data.show(10)


# In[32]:


from pyspark.ml.regression import LinearRegression
##train test split
train_data,test_data=finalized_data.randomSplit([0.75,0.25])
regressor=LinearRegression(featuresCol='Independent Features', labelCol='quantite_med', maxIter=10, regParam=0.3, elasticNetParam=0.8)
regressor=regressor.fit(train_data)


# In[33]:


print("Coefficients: %s" % str(regressor.coefficients))


# In[34]:


print("Intercept: %s" % str(regressor.intercept))


# In[35]:


### Predictions
pred_results=regressor.evaluate(test_data)


# In[36]:


## Final comparison
pred_results.predictions.show()


# In[37]:


### PErformance Metrics
pred_results.r2,pred_results.meanAbsoluteError,pred_results.meanSquaredError


# In[38]:


Final_result = pred_results.predictions.where("quantite_med > prediction ")

from pyspark.sql.functions import round, col
Final_result = Final_result.select("Independent Features"  , "quantite_med", round(col('prediction')))
Final_result.show(50)


# In[39]:


## resulta + quantité rejeté > 0 : pour la vérification
#Final_result = pred_results.predictions.where("quantite_med > prediction  ").join(Qnt_rejetée)
#Final_result.filter(Final_result["qte_rejet"] > 0).show(100)
#print("le taux de rejet par les controle est :  ", pred_results.predictions.where("quantite_med > prediction  ").count()*100/sparkdf.count() ,"%")
#print("\n le taux de rejet par le model  est :  ", Qnt_rejetée.filter(Qnt_rejetée["qte_rejet"] > 0).count()*100/sparkdf.count() ,"%")


# In[40]:


## new column pour comparer la quantité rejeté reel avec celle de la prédiciton 
#Comparer =Final_result.filter(Final_result["qte_rejet"] > 0).withColumn("Quantité rejeté de la prédiction",Final_result.quantite_med - Final_result.prediction.cast(IntegerType()))


# In[41]:


# converting the difference to int 
#from pyspark.sql.types import IntegerType
#Comparer = Comparer.withColumn("Quantité rejeté de la prédiction", Comparer["Quantité rejeté de la prédiction"].cast(IntegerType()))


# In[ ]:





# In[ ]:





# In[ ]:




