package com.roadaccidents

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.classification.{RandomForestClassifier, LogisticRegression, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._

object ModelTrainer {

  def trainModels(df: DataFrame, spark: SparkSession): Map[String, (PipelineModel, Map[String, Double])] = {
    println("\n=== Entraînement des modèles ===")

    val preparedDf = df.select("features", "accident_grave")
      .withColumnRenamed("accident_grave", "label")
      .na.drop()

    println(s"Dataset préparé: ${preparedDf.count()} lignes")

    val Array(trainingData, testData) = preparedDf.randomSplit(Array(0.8, 0.2), seed = 42)
    println(s"Training: ${trainingData.count()} lignes")
    println(s"Test: ${testData.count()} lignes")

    println("\n--- Distribution des classes (train) ---")
    trainingData.groupBy("label").count().show()

    val models = Map(
      "RandomForest" -> trainRandomForest(trainingData, testData),
      "LogisticRegression" -> trainLogisticRegression(trainingData, testData),
      "DecisionTree" -> trainDecisionTree(trainingData, testData)
    )

    println("\n=== Comparaison des modèles ===")
    println("%-20s %-10s %-10s %-10s %-10s".format("Modèle", "Accuracy", "Precision", "Recall", "F1-Score"))
    println("-" * 70)
    
    models.foreach { case (name, (_, metrics)) =>
      println("%-20s %-10.4f %-10.4f %-10.4f %-10.4f".format(
        name,
        metrics("accuracy"),
        metrics("precision"),
        metrics("recall"),
        metrics("f1")
      ))
    }

    models
  }

  private def trainRandomForest(trainingData: DataFrame, testData: DataFrame): (PipelineModel, Map[String, Double]) = {
    println("\n--- Random Forest ---")
    
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(10)
      .setMaxBins(32)
      .setMinInstancesPerNode(5)
      .setSeed(42)

    val pipeline = new Pipeline().setStages(Array(rf))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)
    val metrics = evaluateModel(predictions)

    val rfModel = model.stages(0).asInstanceOf[org.apache.spark.ml.classification.RandomForestClassificationModel]
    println("\n--- Feature Importances (Top 10) ---")
    rfModel.featureImportances.toArray.zipWithIndex
      .sortBy(-_._1)
      .take(10)
      .foreach { case (importance, idx) =>
        println(f"Feature $idx: $importance%.4f")
      }

    (model, metrics)
  }

  private def trainLogisticRegression(trainingData: DataFrame, testData: DataFrame): (PipelineModel, Map[String, Double]) = {
    println("\n--- Logistic Regression ---")
    
    val lr = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setRegParam(0.01)
      .setElasticNetParam(0.5)

    val pipeline = new Pipeline().setStages(Array(lr))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)
    val metrics = evaluateModel(predictions)

    (model, metrics)
  }

  private def trainDecisionTree(trainingData: DataFrame, testData: DataFrame): (PipelineModel, Map[String, Double]) = {
    println("\n--- Decision Tree ---")
    
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(10)
      .setMaxBins(32)
      .setMinInstancesPerNode(5)

    val pipeline = new Pipeline().setStages(Array(dt))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)
    val metrics = evaluateModel(predictions)

    (model, metrics)
  }

  private def evaluateModel(predictions: DataFrame): Map[String, Double] = {
    val accuracyEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = accuracyEvaluator.evaluate(predictions)

    val precisionEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")
    val precision = precisionEvaluator.evaluate(predictions)

    val recallEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")
    val recall = recallEvaluator.evaluate(predictions)

    val f1Evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")
    val f1 = f1Evaluator.evaluate(predictions)

    val aucEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
    val auc = aucEvaluator.evaluate(predictions)

    println(f"Accuracy: $accuracy%.4f")
    println(f"Precision: $precision%.4f")
    println(f"Recall: $recall%.4f")
    println(f"F1-Score: $f1%.4f")
    println(f"AUC-ROC: $auc%.4f")

    println("\n--- Matrice de confusion ---")
    predictions.groupBy("label", "prediction").count().show()

    Map(
      "accuracy" -> accuracy,
      "precision" -> precision,
      "recall" -> recall,
      "f1" -> f1,
      "auc" -> auc
    )
  }

  /**
   * CORRECTION MAJEURE : accepte le featurePipeline pour transformer les données brutes
   * Sauvegarde uniquement les colonnes scalaires (pas de Vector)
   */
  def savePredictions(
    models: Map[String, (PipelineModel, Map[String, Double])],
    rawDf: DataFrame,
    featurePipeline: PipelineModel,
    outputPath: String = "data/predictions"
  ): Unit = {
    println(s"\n=== Sauvegarde des prédictions ===")

    // ✅ Transformer les données brutes avec le pipeline de features
    val transformedDf = featurePipeline.transform(rawDf)

    val preparedDf = transformedDf.select(
      "Num_Acc", "accident_grave", "features",
      "heure", "lum", "atm", "dep", "agg", "nb_vehicules"
    )
    .withColumnRenamed("accident_grave", "label")
    .na.drop()

    models.foreach { case (modelName, (model, _)) =>
      val predictions = model.transform(preparedDf)

      // ✅ CORRECTION CSV : Extraire la probabilité de la classe 1 (accident grave)
      val probabilityUdf = udf((prob: org.apache.spark.ml.linalg.Vector) => prob(1))

      val predictionsForCsv = predictions
        .withColumn("proba_accident_grave", probabilityUdf(col("probability")))
        .select(
          "Num_Acc", "label", "prediction", "proba_accident_grave",
          "heure", "lum", "atm", "dep", "agg", "nb_vehicules"
        )

      val outputFile = s"$outputPath/${modelName.toLowerCase()}_predictions.csv"
      
      predictionsForCsv.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(outputFile)

      println(s"$modelName: prédictions sauvegardées dans $outputFile")
    }
  }

  def saveMetrics(
    models: Map[String, (PipelineModel, Map[String, Double])],
    spark: SparkSession,
    outputPath: String = "data/metrics.csv"
  ): Unit = {
    println(s"\n=== Sauvegarde des métriques ===")
    
    import spark.implicits._

    val metricsData = models.map { case (modelName, (_, metrics)) =>
      (
        modelName,
        metrics("accuracy"),
        metrics("precision"),
        metrics("recall"),
        metrics("f1"),
        metrics("auc")
      )
    }.toSeq

    val metricsDF = spark.createDataFrame(metricsData)
      .toDF("model", "accuracy", "precision", "recall", "f1", "auc")

    metricsDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(outputPath)

    println(s"Métriques sauvegardées dans $outputPath")
  }
}