package trash

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataPreprocessing {

  def main(args: Array[String]): Unit = {

    // =====================================================
    // 1. SPARK SESSION
    // =====================================================
    val spark = SparkSession.builder()
      .appName("CSC5003 - Road Safety Data Preprocessing")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("🚗 CSC5003 - Prétraitement des accidents routiers")

    // =====================================================
    // 2. CHARGEMENT DES CSV
    // =====================================================
    def loadCsv(path: String): DataFrame =
      spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .option("encoding", "UTF-8")
        .csv(path)

    val caracteristiques = loadCsv("data/caracteristiques-2023.csv")
    val lieux             = loadCsv("data/lieux-2023.csv")
    val usagers           = loadCsv("data/usagers-2023.csv")
    val vehicules         = loadCsv("data/vehicules-2023.csv") // chargé pour cohérence projet

    println(s"Caracteristiques : ${caracteristiques.count()}")
    println(s"Lieux            : ${lieux.count()}")
    println(s"Usagers          : ${usagers.count()}")

    // =====================================================
    // 3. VARIABLE CIBLE : is_grave
    // =====================================================
    // grav = 2 (Tué) ou 3 (Hospitalisé) => accident grave
    val targetUsagers = usagers
      .withColumn(
        "is_grave_tmp",
        when(col("grav").isin("2", "3"), 1).otherwise(0)
      )
      .groupBy("Num_Acc")
      .agg(
        max("is_grave_tmp").alias("is_grave")
      )

    println("Distribution de la gravité :")
    targetUsagers.groupBy("is_grave").count().show()

    // =====================================================
    // 4. DÉDOUBLONNAGE
    // =====================================================
    val caracteristiquesUnique = caracteristiques.dropDuplicates("Num_Acc")
    val lieuxUnique            = lieux.dropDuplicates("Num_Acc")

    // =====================================================
    // 5. JOINTURE FINALE
    // =====================================================
    val dfJoined = caracteristiquesUnique
      .join(lieuxUnique, Seq("Num_Acc"), "inner")
      .join(targetUsagers, Seq("Num_Acc"), "inner")

    // =====================================================
    // 6. NETTOYAGE DES VARIABLES ML
    // =====================================================

    // --- Mode de la météo (atm)
    val atmMode = dfJoined
      .filter(col("atm").isNotNull && col("atm") =!= "-1")
      .groupBy("atm")
      .count()
      .orderBy(desc("count"))
      .first()
      .getString(0)

    val dfCleaned = dfJoined
      // Lumière
      .withColumn("lum", coalesce(col("lum"), lit("0")))

      // Météo
      .withColumn(
        "atm",
        when(col("atm").isNull || col("atm") === "-1", atmMode)
          .otherwise(col("atm"))
      )

      // Latitude
      .withColumn(
        "lat_clean",
        regexp_replace(col("lat"), ",", ".").cast(DoubleType)
      )

      // Longitude
      .withColumn(
        "long_clean",
        regexp_replace(col("long"), ",", ".").cast(DoubleType)
      )

      // Agglomération & catégorie route
      .withColumn("agg", coalesce(col("agg"), lit("0")))
      .withColumn("catr", coalesce(col("catr"), lit("0")))

      // Sélection finale
      .select(
        col("Num_Acc"),
        col("lum"),
        col("atm"),
        col("agg"),
        col("catr"),
        col("lat_clean"),
        col("long_clean"),
        col("is_grave")
      )
      .filter(col("lat_clean").isNotNull && col("long_clean").isNotNull)

    println(s"Dataset final nettoyé : ${dfCleaned.count()} lignes")

    // =====================================================
    // 7. SAUVEGARDE
    // =====================================================
    dfCleaned
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv("data/preprocessed_accidents_2023")

    dfCleaned
      .write
      .mode("overwrite")
      .parquet("data/preprocessed_accidents_2023.parquet")

    println("✅ Prétraitement terminé avec succès")

    spark.stop()
  }
}
