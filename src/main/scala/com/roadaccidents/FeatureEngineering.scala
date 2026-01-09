package com.roadaccidents

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, OneHotEncoder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._

object FeatureEngineering {
  
  /**
   * CORRECTION MAJEURE : retourne (DataFrame transformé, PipelineModel)
   * Le PipelineModel sera réutilisé pour les prédictions
   */
  def prepareFeatures(df: DataFrame): (DataFrame, PipelineModel, Array[String]) = {
    println("\n=== Préparation des features ===")

    val numericFeatures = Array(
      "heure", "minute", "jour", "mois", "weekend",
      "lum", "atm", "col", "int", "agg",
      "catr", "circ", "prof", "plan", "surf", "infra", "situ",
      "nbv", "vma",
      "nb_vehicules", "nb_vl", "nb_pl", "nb_2rm", "nb_velo", "has_pl",
      "nb_usagers"
    )

    val categoricalFeatures = Array("tranche_horaire")

    val indexers = categoricalFeatures.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(s"${colName}_index")
        .setHandleInvalid("keep")
    }

    val encoders = categoricalFeatures.map { colName =>
      new OneHotEncoder()
        .setInputCol(s"${colName}_index")
        .setOutputCol(s"${colName}_vec")
        .setDropLast(false)
    }

    val encodedCols = categoricalFeatures.map(c => s"${c}_vec")
    val allFeatureCols = numericFeatures ++ encodedCols

    val assembler = new VectorAssembler()
      .setInputCols(allFeatureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")

    val pipeline = new Pipeline()
      .setStages(indexers ++ encoders :+ assembler)

    // FIT et TRANSFORM
    val pipelineModel = pipeline.fit(df)
    val transformedDf = pipelineModel.transform(df)

    println(s"Nombre de features: ${allFeatureCols.length}")
    println(s"Features numériques: ${numericFeatures.mkString(", ")}")
    println(s"Features catégorielles: ${categoricalFeatures.mkString(", ")}")

    // ✅ RETOURNE LE MODELE EN PLUS DU DATAFRAME
    (transformedDf, pipelineModel, allFeatureCols)
  }

  def analyzeCorrelations(df: DataFrame): Unit = {
    println("\n=== Analyse des corrélations avec la gravité ===")
    
    val varsToAnalyze = Seq(
      ("lum", "Luminosité"),
      ("atm", "Conditions atmosphériques"),
      ("tranche_horaire", "Tranche horaire"),
      ("agg", "Agglomération"),
      ("catr", "Catégorie de route"),
      ("nb_vehicules", "Nombre de véhicules"),
      ("has_pl", "Présence poids lourd"),
      ("weekend", "Weekend")
    )

    varsToAnalyze.foreach { case (varName, description) =>
      println(s"\n--- $description ($varName) ---")
      
      val tauxGravite = df.groupBy(varName)
        .agg(
          count("*").alias("total"),
          sum(when(col("accident_grave") === 1, 1).otherwise(0)).alias("graves")
        )
        .withColumn("taux_gravite", round(col("graves") / col("total") * 100, 2))
        .orderBy(col("taux_gravite").desc)
      
      println(s"Taux d'accidents graves par $description:")
      tauxGravite.show(20, truncate = false)
    }
  }

  def analyzeGeography(df: DataFrame): Unit = {
    println("\n=== Analyse géographique ===")

    println("\n--- Top 20 départements - Accidents graves ---")
    df.filter(col("accident_grave") === 1)
      .groupBy("dep")
      .agg(
        count("*").alias("nb_accidents_graves"),
        avg("nb_tues").alias("moy_tues"),
        avg("nb_hospitalises").alias("moy_hospitalises")
      )
      .orderBy(col("nb_accidents_graves").desc)
      .show(20, truncate = false)

    println("\n--- Taux de gravité par département (min 50 accidents) ---")
    val depStats = df.groupBy("dep")
      .agg(
        count("*").alias("total_accidents"),
        sum(when(col("accident_grave") === 1, 1).otherwise(0)).alias("accidents_graves")
      )
      .filter(col("total_accidents") >= 50)
      .withColumn("taux_gravite", round(col("accidents_graves") / col("total_accidents") * 100, 2))
      .orderBy(col("taux_gravite").desc)
    
    depStats.show(20, truncate = false)
  }
}