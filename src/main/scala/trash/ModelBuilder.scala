package trash

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.sql.functions._

class ModelBuilder(spark: SparkSession) {
  import spark.implicits._
  
  def buildAndEvaluateModels(data: DataFrame): (Map[String, PipelineModel], Map[String, Double]) = {
    
    // Préparer les features
    val featureCols = data.columns.filter(_.startsWith("feature_"))
    
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    
    // Division des données
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 42)
    
    println(s"Données d'entraînement: ${trainingData.count()} lignes")
    println(s"Données de test: ${testData.count()} lignes")
    
    // Initialiser les modèles
    val models = Map(
      "logistic_regression" -> new LogisticRegression()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(100),
      
      "random_forest" -> new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setNumTrees(100)
        .setMaxDepth(10),
      
      "gradient_boosting" -> new GBTClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(50)
        .setMaxDepth(5)
    )
    
    // Évaluateur
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")
    
    // Entraîner et évaluer chaque modèle
    var trainedModels = Map[String, PipelineModel]()
    var modelEvaluations = Map[String, Double]()
    
    for ((name, classifier) <- models) {
      println(s"\n=== Entraînement du modèle: $name ===")
      
      // Créer le pipeline
      val pipeline = new Pipeline()
        .setStages(Array(assembler, classifier))
      
      // Entraîner le modèle
      val model = pipeline.fit(trainingData)
      trainedModels += (name -> model)
      
      // Faire des prédictions
      val predictions = model.transform(testData)
      
      // Évaluer le modèle
      val auc = evaluator.evaluate(predictions)
      modelEvaluations += (name -> auc)
      
      // Calculer l'accuracy
      val accuracy = predictions
        .filter(col("label") === col("prediction"))
        .count().toDouble / predictions.count()
      
      println(s"Performance du modèle $name:")
      println(s"  AUC-ROC: ${auc}")
      println(s"  Accuracy: ${accuracy}")
      
      // Afficher la matrice de confusion
      println("  Matrice de confusion:")
      predictions
        .groupBy("label", "prediction")
        .agg(count("*").alias("count"))
        .orderBy("label", "prediction")
        .show()
    }
    
    // Afficher la comparaison des modèles
    println("\n=== COMPARAISON DES MODÈLES ===")
    println("Modèle\t\tAUC-ROC")
    println("-" * 30)
    modelEvaluations.foreach { case (name, auc) =>
      println(f"$name%-20s $auc%.4f")
    }
    
    (trainedModels, modelEvaluations)
  }
}