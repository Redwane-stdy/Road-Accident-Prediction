package com.roadaccidents

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DataCleaner {
  
  /**
   * Nettoie et prépare les données pour l'analyse
   */
  def cleanAndMergeData(spark: SparkSession, dataFrames: Map[String, DataFrame]): DataFrame = {
    println("\n=== Nettoyage et fusion des données ===")
    
    val caracteristiques = dataFrames("caracteristiques")
    val lieux = dataFrames("lieux")
    val vehicules = dataFrames("vehicules")
    val usagers = dataFrames("usagers")
    
    // Fonction pour convertir virgule française en point
    val replaceComma = udf((s: String) => 
      if (s == null || s.trim.isEmpty) null 
      else s.replace(",", ".")
    )
    
    // Fonction pour convertir string en int de manière sécurisée
    val safeInt = (colName: String) => 
      when(col(colName).isNull or col(colName) === "" or col(colName) === ".", null)
      .otherwise(col(colName).cast("int"))
    
    // 1. Calculer la gravité maximale par accident
    // C'est ce qu'on va essayer de prédire par la suite.
    // Convertir grav en int pour pouvoir l'agréger
    val usagersClean = usagers
      .withColumn("grav_int", safeInt("grav"))
    
    val graviteParAccident = usagersClean
      .groupBy("Num_Acc")
      .agg(
        max("grav_int").alias("gravite_max"),
        count("*").alias("nb_usagers"),
        sum(when(col("grav_int") === 2, 1).otherwise(0)).alias("nb_tues"),
        sum(when(col("grav_int") === 3, 1).otherwise(0)).alias("nb_hospitalises"),
        sum(when(col("grav_int") === 4, 1).otherwise(0)).alias("nb_blesses_legers")
      )
      .withColumn("accident_grave", when(col("gravite_max") <= 3, 1).otherwise(0))
    
    // 2. Agréger les informations des véhicules par accident
    // Note: Certains véhicules en fuite peuvent ne pas avoir de conducteur dans la table usagers
    // selon la documentation BAAC (24 cas en 2024)
    // Codes catégories véhicules (catv):
    // Pour plus d'info sur les codes utilisés voir description-des-bases-de-donnees-annuelles.pdf
    val vehiculesClean = vehicules
      .withColumn("catv_int", safeInt("catv"))
    
    val vehiculesParAccident = vehiculesClean
      .groupBy("Num_Acc")
      .agg(
        count("*").alias("nb_vehicules"),
        collect_list("catv_int").alias("types_vehicules"),
        max("catv_int").alias("catv_max"),
        sum(when(col("catv_int") === 7, 1).otherwise(0)).alias("nb_vl"),
        sum(when(col("catv_int").isin(13, 14, 15, 16, 17), 1).otherwise(0)).alias("nb_pl"),
        sum(when(col("catv_int").isin(30, 31, 32, 33, 34), 1).otherwise(0)).alias("nb_2rm"),
        sum(when(col("catv_int") === 1, 1).otherwise(0)).alias("nb_velo"),
        max(when(col("catv_int").isin(13, 14, 15, 16, 17), 1).otherwise(0)).alias("has_pl")
      )
    
    // 3. Nettoyer les caractéristiques
    val caracteristiquesClean = caracteristiques
      // D'ABORD convertir toutes les colonnes string en int
      .withColumn("jour_int", safeInt("jour"))
      .withColumn("mois_int", safeInt("mois"))
      .withColumn("an_int", safeInt("an"))
      .withColumn("lum_int", safeInt("lum"))
      .withColumn("agg_int", safeInt("agg"))
      .withColumn("int_int", safeInt("int"))
      .withColumn("atm_int", safeInt("atm"))
      .withColumn("col_int", safeInt("col"))
      // Convertir lat/long avec virgule française
      .withColumn("lat_clean", replaceComma(col("lat")))
      .withColumn("long_clean", replaceComma(col("long")))
      .withColumn("lat_double", 
        when(col("lat_clean").isNull or col("lat_clean") === "", null)
        .otherwise(col("lat_clean").cast("double")))
      .withColumn("long_double", 
        when(col("long_clean").isNull or col("long_clean") === "", null)
        .otherwise(col("long_clean").cast("double")))
      // Parser l'heure au format "HH:MM" (avec deux-points)
      .withColumn("heure", 
        when(col("hrmn").isNotNull and col("hrmn") =!= "" and col("hrmn").contains(":"), 
          split(col("hrmn"), ":")(0).cast("int"))
        .otherwise(lit(null).cast("int")))
      .withColumn("minute", 
        when(col("hrmn").isNotNull and col("hrmn") =!= "" and col("hrmn").contains(":"), 
          split(col("hrmn"), ":")(1).cast("int"))
        .otherwise(lit(null).cast("int")))
      // Catégoriser les tranches horaires
      .withColumn("tranche_horaire",
        when(col("heure").isNull, "inconnu")
        .when(col("heure").between(0, 5), "nuit")
        .when(col("heure").between(6, 8), "matin_trafic")
        .when(col("heure").between(9, 11), "matinee")
        .when(col("heure").between(12, 13), "midi")
        .when(col("heure").between(14, 16), "apres_midi")
        .when(col("heure").between(17, 19), "soir_trafic")
        .when(col("heure").between(20, 23), "soiree")
        .otherwise("inconnu"))
      // Jour de la semaine
      .withColumn("date_complete", 
        when(col("an_int").isNotNull and col("mois_int").isNotNull and col("jour_int").isNotNull,
          to_date(concat(
            col("an_int"), lit("-"), 
            lpad(col("mois_int").cast("string"), 2, "0"), lit("-"),
            lpad(col("jour_int").cast("string"), 2, "0")
          )))
        .otherwise(lit(null)))
      .withColumn("jour_semaine", dayofweek(col("date_complete")))
      .withColumn("weekend", when(col("jour_semaine").isin(1, 7), 1).otherwise(0))
      // Remplacer les valeurs manquantes (sur les colonnes _int)
      .withColumn("lum", when(col("lum_int").isNull or col("lum_int") === -1 or col("lum_int") === 0, 1).otherwise(col("lum_int")))
      .withColumn("atm", when(col("atm_int").isNull or col("atm_int") === -1, 1).otherwise(col("atm_int")))
      .withColumn("col", when(col("col_int").isNull or col("col_int") === -1, 7).otherwise(col("col_int")))
      .withColumn("int", when(col("int_int").isNull or col("int_int") === 0, 1).otherwise(col("int_int")))
      .withColumn("agg", when(col("agg_int").isNull or col("agg_int") === 0, 2).otherwise(col("agg_int")))
      // Créer les colonnes finales jour, mois, an
      .withColumn("jour", col("jour_int"))
      .withColumn("mois", col("mois_int"))
      .withColumn("an", col("an_int"))
      .withColumn("lat", col("lat_double"))
      .withColumn("long", col("long_double"))
      // Nettoyer les colonnes temporaires
      .drop("jour_int", "mois_int", "an_int", "lum_int", "agg_int", "int_int", "atm_int", "col_int", 
            "lat_clean", "long_clean", "lat_double", "long_double", "date_complete")
    
    // 4. Nettoyer les lieux
    // Pour plus d'info sur les codes utilisés voir description-des-bases-de-donnees-annuelles.pdf
    val lieuxClean = lieux
      .withColumn("catr_int", safeInt("catr"))
      .withColumn("circ_int", safeInt("circ"))
      .withColumn("nbv_int", safeInt("nbv"))
      .withColumn("vosp_int", safeInt("vosp"))
      .withColumn("prof_int", safeInt("prof"))
      .withColumn("plan_int", safeInt("plan"))
      .withColumn("surf_int", safeInt("surf"))
      .withColumn("infra_int", safeInt("infra"))
      .withColumn("situ_int", safeInt("situ"))
      .withColumn("vma_int", safeInt("vma"))
      .withColumn("catr", when(col("catr_int").isNull, 0).otherwise(col("catr_int")))
      .withColumn("circ", when(col("circ_int").isNull or col("circ_int") === -1, 0).otherwise(col("circ_int")))
      .withColumn("prof", when(col("prof_int").isNull or col("prof_int") === -1, 1).otherwise(col("prof_int")))
      .withColumn("plan", when(col("plan_int").isNull or col("plan_int") === -1, 1).otherwise(col("plan_int")))
      .withColumn("surf", when(col("surf_int").isNull or col("surf_int") === -1, 1).otherwise(col("surf_int")))
      .withColumn("infra", when(col("infra_int").isNull or col("infra_int") === -1, 0).otherwise(col("infra_int")))
      .withColumn("situ", when(col("situ_int").isNull or col("situ_int") === -1, 1).otherwise(col("situ_int")))
      .withColumn("nbv", when(col("nbv_int").isNull, 2).otherwise(col("nbv_int")))
      .withColumn("vma", when(col("vma_int").isNull or col("vma_int") === 0, 50).otherwise(col("vma_int")))
      .drop("catr_int", "circ_int", "nbv_int", "vosp_int", "prof_int", "plan_int", "surf_int", "infra_int", "situ_int", "vma_int")
    
    // 5. Fusion de toutes les tables
    val datasetFinal = caracteristiquesClean
      .join(lieuxClean, Seq("Num_Acc"), "left")
      .join(vehiculesParAccident, Seq("Num_Acc"), "left")
      .join(graviteParAccident, Seq("Num_Acc"), "inner")
      .na.fill(0, Seq("nb_vehicules", "nb_vl", "nb_pl", "nb_2rm", "nb_velo", "has_pl"))
    
    println(s"Dataset final: ${datasetFinal.count()} accidents")
    println(s"Accidents graves: ${datasetFinal.filter(col("accident_grave") === 1).count()}")
    
    // Afficher quelques statistiques
    println("\n--- Distribution de la gravité ---")
    datasetFinal.groupBy("accident_grave").count().show()
    
    println("\n--- Statistiques sur les colonnes clés ---")
    datasetFinal.select("heure", "lum", "atm", "agg", "nb_vehicules", "accident_grave")
      .describe().show()
    
    datasetFinal
  }
  
  /**
   * Sauvegarde le dataset nettoyé
   */
  def saveCleanedData(df: DataFrame, outputPath: String = "data/cleaned_data.csv"): Unit = {
    println(s"\n=== Sauvegarde du dataset nettoyé vers $outputPath ===")
    
    val dfForCsv = df.drop("types_vehicules")
    println("La colonne types_vehicules a été enlevé avec succès")
    dfForCsv.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(outputPath)
    
    println("Sauvegarde terminée")
  }
}