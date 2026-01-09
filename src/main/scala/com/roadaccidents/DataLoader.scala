package com.roadaccidents

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataLoader {
  
  /**
   * Charge les 4 fichiers CSV des accidents de 2023
   * 
   * AVERTISSEMENT (selon documentation BAAC):
   * - Les données "blessé hospitalisé" depuis 2018 ne sont plus labellisées
   * - Les usagers en fuite depuis 2021 peuvent avoir des informations manquantes
   * - Base brute non corrigée des erreurs de saisie
   */
  def loadData(spark: SparkSession, basePath: String = "data"): Map[String, DataFrame] = {
    println("=== Chargement des données ===")
    
    // Schémas pour chaque fichier
    // IMPORTANT: Les données françaises utilisent la virgule comme séparateur décimal
    // On doit utiliser un point, donc on charge toutes les données en String et 
    // dans DataCleaner.scala on fera les conversions.
    val caracteristiquesSchema = StructType(Array(
      StructField("Num_Acc", StringType, nullable = false),
      StructField("jour", StringType, nullable = true),     // String car peut avoir zéros devant
      StructField("mois", StringType, nullable = true),     // String car format "05", "12", etc.
      StructField("an", StringType, nullable = true),       // String pour cohérence
      StructField("hrmn", StringType, nullable = true),     // Format "HH:MM"
      StructField("lum", StringType, nullable = true),      // String puis conversion
      StructField("dep", StringType, nullable = true),      // String (peut être "2A", "2B" pour Corse)
      StructField("com", StringType, nullable = true),      // Code INSEE (string)
      StructField("agg", StringType, nullable = true),      // String puis conversion
      StructField("int", StringType, nullable = true),      // String puis conversion
      StructField("atm", StringType, nullable = true),      // String puis conversion
      StructField("col", StringType, nullable = true),      // String puis conversion
      StructField("adr", StringType, nullable = true),
      StructField("lat", StringType, nullable = true),      // String car virgule française
      StructField("long", StringType, nullable = true)      // String car virgule française
    ))
    
    val lieuxSchema = StructType(Array(
      StructField("Num_Acc", StringType, nullable = false),
      StructField("catr", StringType, nullable = true),
      StructField("voie", StringType, nullable = true),
      StructField("v1", StringType, nullable = true),
      StructField("v2", StringType, nullable = true),
      StructField("circ", StringType, nullable = true),
      StructField("nbv", StringType, nullable = true),
      StructField("vosp", StringType, nullable = true),
      StructField("prof", StringType, nullable = true),
      StructField("pr", StringType, nullable = true),
      StructField("pr1", StringType, nullable = true),
      StructField("plan", StringType, nullable = true),
      StructField("lartpc", StringType, nullable = true),  // Virgule française
      StructField("larrout", StringType, nullable = true), // Virgule française
      StructField("surf", StringType, nullable = true),
      StructField("infra", StringType, nullable = true),
      StructField("situ", StringType, nullable = true),
      StructField("vma", StringType, nullable = true)
    ))
    
    val vehiculesSchema = StructType(Array(
      StructField("Num_Acc", StringType, nullable = false),
      StructField("id_vehicule", StringType, nullable = false),
      StructField("num_veh", StringType, nullable = true),
      StructField("senc", StringType, nullable = true),
      StructField("catv", StringType, nullable = true),
      StructField("obs", StringType, nullable = true),
      StructField("obsm", StringType, nullable = true),
      StructField("choc", StringType, nullable = true),
      StructField("manv", StringType, nullable = true),
      StructField("motor", StringType, nullable = true),
      StructField("occutc", StringType, nullable = true)
    ))
    
    val usagersSchema = StructType(Array(
      StructField("Num_Acc", StringType, nullable = false),
      StructField("id_usager", StringType, nullable = false),
      StructField("id_vehicule", StringType, nullable = false),
      StructField("num_veh", StringType, nullable = true),
      StructField("place", StringType, nullable = true),
      StructField("catu", StringType, nullable = true),
      StructField("grav", StringType, nullable = true),
      StructField("sexe", StringType, nullable = true),
      StructField("an_nais", StringType, nullable = true),
      StructField("trajet", StringType, nullable = true),
      StructField("secu1", StringType, nullable = true),
      StructField("secu2", StringType, nullable = true),
      StructField("secu3", StringType, nullable = true),
      StructField("locp", StringType, nullable = true),
      StructField("actp", StringType, nullable = true),
      StructField("etatp", StringType, nullable = true)
    ))
    
    // Chargement des fichiers
    val caracteristiques = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("encoding", "UTF-8")
      .schema(caracteristiquesSchema)
      .csv(s"$basePath/caracteristiques-2023.csv")
    
    val lieux = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("encoding", "UTF-8")
      .schema(lieuxSchema)
      .csv(s"$basePath/lieux-2023.csv")
    
    val vehicules = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("encoding", "UTF-8")
      .schema(vehiculesSchema)
      .csv(s"$basePath/vehicules-2023.csv")
    
    val usagers = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("encoding", "UTF-8")
      .schema(usagersSchema)
      .csv(s"$basePath/usagers-2023.csv")
    
    println(s"Caractéristiques: ${caracteristiques.count()} lignes")
    println(s"Lieux: ${lieux.count()} lignes")
    println(s"Véhicules: ${vehicules.count()} lignes")
    println(s"Usagers: ${usagers.count()} lignes")
    
    Map(
      "caracteristiques" -> caracteristiques,
      "lieux" -> lieux,
      "vehicules" -> vehicules,
      "usagers" -> usagers
    )
  }
  
  /**
   * Affiche des statistiques descriptives sur les données
   */
  def displayStats(dataFrames: Map[String, DataFrame]): Unit = {
    println("\n=== Statistiques descriptives ===")
    
    dataFrames.foreach { case (name, df) =>
      println(s"\n--- $name ---")
      df.printSchema()
      df.show(5, truncate = false)
    }
  }
}