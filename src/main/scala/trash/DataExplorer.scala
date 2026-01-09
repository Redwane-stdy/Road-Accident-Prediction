package trash

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class DataExplorer(spark: SparkSession) {
  import spark.implicits._
  
  def exploreData(data: DataFrame): Unit = {
    println("=== EXPLORATION DES DONNÉES ===")
    
    // 1. Statistiques de base
    println("\n1. Statistiques générales:")
    println(s"Nombre total d'accidents: ${data.count()}")
    println(s"Nombre de colonnes: ${data.columns.length}")
    
    // 2. Répartition de la gravité
    println("\n2. Répartition de la gravité des accidents:")
    data.groupBy("accident_grave")
      .agg(count("*").alias("nb_accidents"))
      .withColumn("pourcentage", round(col("nb_accidents") * 100 / data.count(), 2))
      .show()
    
    // 3. Top 10 des départements avec le plus d'accidents graves
    println("\n3. Top 10 départements avec accidents graves:")
    data.filter(col("accident_grave") === 1)
      .groupBy("dep")
      .agg(count("*").alias("nb_accidents_graves"))
      .orderBy(desc("nb_accidents_graves"))
      .limit(10)
      .show()
    
    // 4. Corrélations avec les conditions météo
    println("\n4. Accidents graves par conditions météorologiques:")
    data.groupBy("atm", "accident_grave")
      .agg(count("*").alias("count"))
      .groupBy("atm")
      .pivot("accident_grave")
      .sum("count")
      .na.fill(0)
      .withColumn("taux_graves", round(col("1") * 100 / (col("0") + col("1")), 2))
      .orderBy(desc("taux_graves"))
      .show()
    
    // 5. Distribution par type de véhicule
    println("\n5. Distribution par type de véhicule:")
    data.groupBy("categorie_vehicule", "accident_grave")
      .agg(count("*").alias("count"))
      .groupBy("categorie_vehicule")
      .pivot("accident_grave")
      .sum("count")
      .na.fill(0)
      .withColumn("taux_graves", round(col("1") * 100 / (col("0") + col("1")), 2))
      .orderBy(desc("taux_graves"))
      .show()
    
    // 6. Heures les plus dangereuses
    println("\n6. Distribution par heure de la journée:")
    data.groupBy("heure", "accident_grave")
      .agg(count("*").alias("count"))
      .groupBy("heure")
      .pivot("accident_grave")
      .sum("count")
      .na.fill(0)
      .withColumn("taux_graves", round(col("1") * 100 / (col("0") + col("1")), 2))
      .orderBy("heure")
      .show(24)
  }
  
  def computeKeyStatistics(data: DataFrame, predictions: DataFrame): DataFrame = {
    // Calculer les statistiques clés pour le dashboard
    val stats = data
      .agg(
        count("*").alias("total_accidents"),
        sum("accident_grave").alias("accidents_graves"),
        avg(when(col("accident_grave") === 1, 1).otherwise(0)).alias("taux_gravite")
      )
      .withColumn("taux_gravite_pourcent", round(col("taux_gravite") * 100, 2))
    
    // Ajouter les prédictions
    val predictionStats = predictions
      .agg(
        count("*").alias("total_predictions"),
        avg(when(col("gravite_reelle") === col("gravite_predite"), 1).otherwise(0))
          .alias("accuracy")
      )
      .withColumn("accuracy_pourcent", round(col("accuracy") * 100, 2))
    
    stats.crossJoin(predictionStats)
  }
}