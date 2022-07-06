try:
    from pyspark.sql import Window
    from pyspark.sql.functions import round, col
    from datetime import date
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.feature import StringIndexer
    from pyspark.sql.functions import col, concat_ws
    from pyspark.sql.functions import udf
    import pyspark.sql.functions as F
    import sys
    from cassandra.cluster import Cluster
    import pandas as pd
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext

    from pyspark.sql import SQLContext
    import numpy as np
    from pyspark.sql.functions import split, col

    # new spark session
    spark = SparkSession.builder.appName('PPA detection').getOrCreate()

    # connection to cassandra database and cnas keyspace
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('cnas')
    # get parameters (start_date and end_date)

    date_debut = sys.argv[1]
    date_fin = sys.argv[2]

    query = "SELECT *  FROM cnas  WHERE date_paiement >= '{}' AND date_paiement <= '{}' LIMIT 300 ALLOW FILTERING;".format(
        date_debut, date_fin)
    rows = session.execute(query)
    # print the data : print(rows)
    # transform the cassandra.cluster ( rows) to pandas dataframe to make some changes
    dftable = pd.DataFrame(list(rows))
    # print the data : print (df)
    # transformation :

    # remplacer None avec -1 ( aucune affection) dans la coloumn affection
    dftable.affection.fillna(value=-1, inplace=True)

    # remplacer None avec 0 ( aucune quantitée rejetée ) dans la coloumn qte_rejet
    dftable.qte_rejet.fillna(value=0, inplace=True)

    # delete rows where the quantite_med == 0
    dftable.drop(dftable[dftable['quantite_med'] == 0].index, inplace=True)

    # delete rejected quantity , but before this , we need to save the rows having quantity rejected > 0
    #df_rejected = dftable[dftable['qte_rejet'] > 0]
    #dftable.drop(dftable[dftable['qte_rejet'] > 0].index, inplace = True)

    # remplacer None avec 0 ( aucune durée spécifiée ) dans la coloumn duree_traitement
    dftable.duree_traitement.fillna(value=0, inplace=True)

    # change the type of some lines
    dftable = dftable.astype({"affection": str})
    dftable = dftable.astype({"fk": float})
    dftable = dftable.astype({"age": int})
    dftable = dftable.astype({"date_paiement": str})
    # garder les coloumns qu'on est besoin
    dftable = dftable[['id', 'fk', 'codeps', 'affection', 'age', 'applic_tarif',
                       'date_paiement', 'num_enr', 'sexe', 'ts', 'quantite_med', 'qte_rejet']]
    # print the columns that we need : dftable.info()
    # add column 'count_medicament' that gives the count of every num_enr
    dftable['count_Medicament'] = dftable.groupby(
        'num_enr')['num_enr'].transform('count')
    # split the table into two table : rejected one and accepted one
    rejected = dftable[dftable['qte_rejet'] > 0]
    accepted = dftable[dftable['qte_rejet'] == 0]
    # Create spark dataframe for the two pandas table (accepted and rejected)
    sparkdf = spark.createDataFrame(accepted)
    #rejected_sparkdf = spark.createDataFrame(rejected)
    # transform the affection column to array of int ( splited by ',')
    sparkdf = sparkdf.withColumn("affection", split(
        col("affection"), ",").cast("array<int>"))
    # sort the affection array
    sparkdf = sparkdf.withColumn('affection', F.array_sort('affection'))
    # put the age in ranges

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
    # transform the affection column to a string again ( so we can index it)
    sparkdf = sparkdf.withColumn("affection",
                                 concat_ws(",", col("affection")))

    # Handling Categorical Features
    indexer = StringIndexer(inputCols=["sexe", "applic_tarif", "ts", "affection", "age"], outputCols=["sex_indexed", "applic_tarif_indexed",
                                                                                                      "ts_indexes", "affection_indexes", "age_indexes"])
    df_r = indexer.setHandleInvalid("keep").fit(sparkdf).transform(sparkdf)
    # put the data into one vector
    featureassembler = VectorAssembler(inputCols=['id', 'fk', 'age_indexes', 'sex_indexed', 'affection_indexes',
                                                  'ts_indexes', 'quantite_med', ], outputCol="Features")
    output = featureassembler.transform(df_r)
    # prepare the data to fit it to the model
    finalized_data = output.select("Features", "quantite_med")
    # call and fit the model
    # train test split
    train_data, test_data = finalized_data.randomSplit([0.75, 0.25])
    regressor = LinearRegression(featuresCol='Features', labelCol='quantite_med',
                                 maxIter=10, regParam=0.3, elasticNetParam=0.8)
    regressor = regressor.fit(train_data)

    # Predictions
    pred_results = regressor.evaluate(test_data)
    # get the Training_date
    today = date.today()

    pred_results.r2, pred_results.meanAbsoluteError, pred_results.meanSquaredError
    # print the final predicted quantity ( rounded ) vs the real quantity
    # get the prediction column
    prediction = pred_results.predictions
    # round the prediction
    prediction = prediction.select("Features", "quantite_med", round(col('prediction'))).withColumnRenamed(
        "round(prediction, 0)", "Predicted_quantity").withColumnRenamed("quantite_med", "Descripted_quantity")
    # keep the suspected line
    prediction = prediction.where("quantite_med > round(prediction, 0) ")
    # prediction.show(50)

    # add a predicted rejection
    Final_result = prediction.withColumn(
        "Rejection", prediction.Descripted_quantity - prediction.Predicted_quantity)
    Final_result.show()
    # Final_result.count()
    # joint the original data with the resukt_data to get all the informations
    Final_result = Final_result.join(
        output, Final_result.Features == output.Features, "inner")
    Final_result.select("date_paiement").show()

    # add Count_medicament_suspected that count the count of suspected time this drug shows
    Final_result = Final_result.withColumn('Count_medicament_suspected', F.count(
        'num_enr').over(Window.partitionBy('num_enr')))

    # connection to cassandra database and fraud keyspace
    session2 = cluster.connect('fraud')
    # get the last id of training
    bigIdValue = session2.execute(
        "select * from params where param='Max_Id_Entrainement_Quantity';")
    id_training = bigIdValue.one().value

    # iterate the data (insert it into cassandra keyspace) :
    data_collect = Final_result.collect()
    query = "INSERT INTO Quantity_result (id , fk , no_assure , id_entrainement , quantite_med , quantite_predicted , qte_rejet_predicted , count_medicament , count_medicament_suspected , num_enr , date_entrainement , date_paiement ) VALUES (now() ,%s, %s ,%s , %s ,%s ,%s ,%s ,%s ,%s ,%s ,%s )"
    num_assuree = 1
    for row in data_collect:
        print(row["fk"])
        future = session2.execute(query, [row["fk"], num_assuree, id_training, row["Descripted_quantity"], row["Predicted_quantity"],
                                          row["Rejection"], row["count_Medicament"], row["Count_medicament_suspected"], row["num_enr"], today, row["date_paiement"]])
        num_assuree = num_assuree + 1

    # insert the HistoryTrainings into cassandra table
    type_training = 1
    status = " "
    query = "INSERT INTO HistoryTrainings (id , date , statut , type ) VALUES (%s, %s ,%s ,%s) "
    adToHistory = session2.execute(
        query, [id_training, today, status, type_training])

    # Increment the id of training
    session2.execute(
        "UPDATE params SET value = value + 1 WHERE param ='Max_Id_Entrainement_Quantity' ;")

except:
    print("An exception occurred")