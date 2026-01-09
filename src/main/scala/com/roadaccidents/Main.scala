package com.roadaccidents

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println("=" * 80)
    println("PROJET CSC5003 - ANALYSE ET PRÉDICTION DES ACCIDENTS DE LA ROUTE")
    println("=" * 80)

    val spark = SparkSession.builder()
      .appName("RoadAccidentsPrediction")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      // ÉTAPE 1: Chargement
      val dataFrames = DataLoader.loadData(spark)
      DataLoader.displayStats(dataFrames)

      // ÉTAPE 2: Nettoyage
      val cleanedData = DataCleaner.cleanAndMergeData(spark, dataFrames)
      DataCleaner.saveCleanedData(cleanedData)

      // ÉTAPE 3: Analyse exploratoire
      FeatureEngineering.analyzeCorrelations(cleanedData)
      FeatureEngineering.analyzeGeography(cleanedData)

      // ÉTAPE 4: Feature Engineering
      // ✅ RÉCUPÈRE LE PIPELINE MODEL
      val (preparedData, featurePipeline, featureNames) = FeatureEngineering.prepareFeatures(cleanedData)

      // ÉTAPE 5: Entraînement
      val models = ModelTrainer.trainModels(preparedData, spark)

      // ÉTAPE 6: Sauvegarde
      // ✅ PASSE LE PIPELINE ET LES DONNÉES BRUTES
      ModelTrainer.savePredictions(models, cleanedData, featurePipeline)
      ModelTrainer.saveMetrics(models, spark)

      // Données pour visualisation
      println("\n=== Sauvegarde des données pour visualisation ===")
      preparedData.select(
        "Num_Acc", "heure", "tranche_horaire", "lum", "atm", "dep",
        "agg", "catr", "nb_vehicules", "nb_vl", "nb_pl", "nb_2rm",
        "has_pl", "weekend", "nb_tues", "nb_hospitalises",
        "accident_grave", "lat", "long"
      )
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv("data/prepared_for_viz.csv")

      println("\n" + "=" * 80)
      println("PIPELINE TERMINÉ AVEC SUCCÈS!")
      println("=" * 80)
      println("\nFichiers générés:")
      println(" - data/cleaned_data.csv : Données nettoyées")
      println(" - data/prepared_for_viz.csv : Données pour visualisation")
      println(" - data/predictions/ : Prédictions des modèles")
      println(" - data/metrics.csv : Métriques des modèles")

    } catch {
      case e: Exception =>
        println(s"\nERREUR: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}