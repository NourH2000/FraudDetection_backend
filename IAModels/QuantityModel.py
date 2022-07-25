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
    spark = SparkSession.builder.appName('Quantity model ').getOrCreate()

    # connection to cassandra database and cnas keyspace
    cluster = Cluster(['127.0.0.1'])

    # get the Training_date
    today = date.today()
    # get parameters (start_date and end_date)

    date_debut = sys.argv[1]
    date_fin = sys.argv[2]

    # connection to cassandra database and fraud keyspace
    session2 = cluster.connect('frauddetection')

    # get the last id of training
    bigIdValue = session2.execute(
        "select * from params where param='Max_Id_Entrainement_Quantity';")
    id_training = bigIdValue.one().value
    # insert the History into cassandra table
    type_training = 1
    status = 0
    query = "INSERT INTO History (id , date , status , type, date_debut , date_fin ) VALUES (%s, %s ,%s ,%s,%s ,%s) "
    addToHistory = session2.execute(
        query, [id_training, today, status, type_training, date_debut, date_fin])

    # Increment the id of training
    session2.execute(
        "UPDATE params SET value = value + 1 WHERE param ='Max_Id_Entrainement_Quantity' ;")

    # connect to the CNAS  database (temporary)
    session = cluster.connect('cnas')

    query = "SELECT *  FROM cnas  WHERE date_paiement >= '{}' AND date_paiement <= '{}'  ALLOW FILTERING;".format(
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

    # generer num_assuré
    from pyspark.sql.functions import rand, when
    Final_result = Final_result.withColumn(
        "no_assure", round(rand()*(20-5)+5, 0))

    # count the num_assuré
    Final_result = Final_result.withColumn('count_assure', F.count(
        'no_assure').over(Window.partitionBy('no_assure')))
    Final_result.select("count_assure").show()

    # iterate the data (insert it into cassandra keyspace) :
    data_collect = Final_result.collect()
    query = "INSERT INTO Quantity_result (id , fk , no_assure , id_entrainement , quantite_med , quantite_predicted , qte_rejet_predicted , count_medicament , count_medicament_suspected  , num_enr , date_entrainement ,date_paiement,affection , age , centre , codeps , date_debut , date_fin , gender  ) VALUES (now() ,%s, %s ,%s,%s ,%s ,%s ,%s,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s)"
    queryAssure = "INSERT INTO assure_result (id , fk , no_assure , id_entrainement , quantite_med , quantite_predicted , qte_rejet_predicted , count_assure  , num_enr , date_entrainement ,date_paiement,affection , age , centre , codeps , date_debut , date_fin , gender  ) VALUES (now() ,%s, %s ,%s ,%s ,%s ,%s,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s)"

    # on doit le changer ( random center) et ajouter spécialité
    import random
    a = []
    for i in range(58):
        a.append(i+1)

    for row in data_collect:
        # print(row["fk"])
        centre = random.choice(a)
        future = session2.execute(query, [row["fk"], row["no_assure"], id_training, row["Descripted_quantity"], row["Predicted_quantity"], row["Rejection"], row["count_Medicament"],
                                  row["Count_medicament_suspected"], row["num_enr"], today, row["date_paiement"], row["affection"], row["age"], centre, row["codeps"], date_debut, date_fin, row["sexe"]])
        Assure = session2.execute(queryAssure, [row["fk"], row["no_assure"], id_training, row["Descripted_quantity"], row["Predicted_quantity"], row["Rejection"],
                                  row["count_assure"], row["num_enr"], today, row["date_paiement"], row["affection"], row["age"], centre, row["codeps"], date_debut, date_fin, row["sexe"]])

    # set the status of the training = 1 ( success)
    success = 1
    query_success = "UPDATE History SET status ={} WHERE id ={} and type = 1 ;".format(
        success, id_training)
    session2.execute(query_success)

except Exception as e:
    print("An exception occurred")
    print(e)
    # set the status of the training = -1 ( failed)
    faild = -1
    query_success = "UPDATE History SET status ={} WHERE id ={} and type = 1 ;".format(
        faild, id_training)
    session2.execute(query_success)
