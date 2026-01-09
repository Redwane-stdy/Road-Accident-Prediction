package trash

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._

class Predictor(spark: SparkSession) {
  import spark.implicits._
  
  def generatePredictions(data: DataFrame, models: Map[String, PipelineModel]): DataFrame = {
    
    // Utiliser le meilleur modèle (Random Forest généralement)
    val bestModel = models("random_forest")
    
    // Générer les prédictions
    val predictions = bestModel.transform(data)
    
    // Formater les résultats
    val formattedPredictions = predictions
      .select(
        monotonically_increasing_id().alias("prediction_id"),
        col("label").alias("gravite_reelle"),
        col("prediction").alias("gravite_predite"),
        col("probability").getItem(1).alias("probability_grave"),
        when(col("label") === col("prediction"), "correct")
          .otherwise("incorrect").alias("prediction_status")
      )
    
    // Statistiques de prédiction
    println("\n=== STATISTIQUES DE PRÉDICTION ===")
    val total = formattedPredictions.count()
    val correct = formattedPredictions.filter(col("prediction_status") === "correct").count()
    val accuracy = correct.toDouble / total * 100
    
    println(f"Précision globale: $accuracy%.2f%%")
    println(s"Prédictions correctes: $correct / $total")
    
    // Distribution des probabilités
    println("\nDistribution des probabilités de gravité:")
    formattedPredictions
      .select(
        floor(col("probability_grave") * 10).alias("prob_bin")
      )
      .groupBy("prob_bin")
      .agg(count("*").alias("count"))
      .orderBy("prob_bin")
      .show()
    
    formattedPredictions
  }
  
  def analyzeFeatureImportance(model: PipelineModel): Unit = {
    try {
      val rfModel = model.stages(1).asInstanceOf[org.apache.spark.ml.classification.RandomForestClassificationModel]
      
      println("\n=== IMPORTANCE DES FEATURES ===")
      println("Top 20 des features les plus importantes:")
      
      val featureImportances = rfModel.featureImportances.toArray
      val featureNames = Array(
        "heure", "moment_journee", "mois",
        "lumiere", "meteo", "etat_surface",
        "agglo", "departement", "type_route",
        "regime_circulation", "vitesse_max",
        "intersection", "type_collision",
        "type_vehicule", "obstacle_fixe"
      )
      
      featureNames.zip(featureImportances)
        .sortBy(-_._2)
        .take(20)
        .foreach { case (name, importance) =>
          println(f"$name%-30s $importance%.4f")
        }
    } catch {
      case e: Exception =>
        println("Impossible d'extraire l'importance des features")
    }
  }
}