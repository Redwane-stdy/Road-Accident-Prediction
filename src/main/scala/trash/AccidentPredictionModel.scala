package trash

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.{RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object AccidentPredictionModel {
  
  def main(args: Array[String]): Unit = {
    
    // =====================================================
    // 1. SPARK SESSION
    // =====================================================
    val spark = SparkSession.builder()
      .appName("CSC5003 - Accident Severity Prediction")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._
    
    println("=" * 80)
    println("🚦 CSC5003 - MODÉLISATION DE LA GRAVITÉ DES ACCIDENTS")
    println("=" * 80)
    
    // =====================================================
    // 2. CHARGEMENT DES DONNÉES
    // =====================================================
    println("\n📂 Chargement des données prétraitées...")
    
    val data = spark.read
      .parquet("data/preprocessed_accidents_2023.parquet")
      .withColumn("is_grave", col("is_grave").cast("double"))
      .na.drop(Seq("lat_clean", "long_clean", "lum", "atm", "agg", "catr"))
    
    println(s"✅ Dataset chargé : ${data.count()} lignes")
    
    // Distribution des classes
    println("\n📊 Distribution des classes:")
    data.groupBy("is_grave").count().show()
    
    // =====================================================
    // 3. DÉFINITION DES FEATURES
    // =====================================================
    val categoricalCols = Array("lum", "atm", "agg", "catr")
    val numericalCols   = Array("lat_clean", "long_clean")
    
    println(s"\n🔧 Features catégorielles: ${categoricalCols.mkString(", ")}")
    println(s"🔧 Features numériques: ${numericalCols.mkString(", ")}")
    
    // =====================================================
    // 4. PIPELINE DE TRANSFORMATION
    // =====================================================
    
    // StringIndexers pour les colonnes catégorielles
    val indexers: Array[PipelineStage] = categoricalCols.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(s"${colName}_idx")
        .setHandleInvalid("keep")
    }
    
    // OneHotEncoders pour les colonnes indexées
    val encoders: Array[PipelineStage] = categoricalCols.map { colName =>
      new OneHotEncoder()
        .setInputCol(s"${colName}_idx")
        .setOutputCol(s"${colName}_ohe")
    }
    
    // VectorAssembler pour combiner toutes les features
    val featureColNames = categoricalCols.map(col => s"${col}_ohe") ++ numericalCols
    
    val assembler = new VectorAssembler()
      .setInputCols(featureColNames)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    
    // =====================================================
    // 5. MODÈLE RANDOM FOREST
    // =====================================================
    val rf = new RandomForestClassifier()
      .setLabelCol("is_grave")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(10)
      .setMaxBins(32)
      .setSeed(42)
    
    // =====================================================
    // 6. CONSTRUCTION DU PIPELINE COMPLET
    // =====================================================
    val pipeline = new Pipeline()
      .setStages(indexers ++ encoders ++ Array(assembler, rf))
    
    // =====================================================
    // 7. SPLIT TRAIN / TEST (80% / 20%)
    // =====================================================
    println("\n🔀 Séparation train/test (80/20)...")
    
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2), seed = 42)
    
    println(s"✅ Train : ${train.count()} lignes")
    println(s"✅ Test  : ${test.count()} lignes")
    
    // =====================================================
    // 8. ENTRAÎNEMENT DU MODÈLE
    // =====================================================
    println("\n🎓 Entraînement du modèle Random Forest...")
    
    val startTime = System.currentTimeMillis()
    val model = pipeline.fit(train)
    val trainTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    println(f"✅ Modèle entraîné en $trainTime%.2f secondes")
    
    // =====================================================
    // 9. PRÉDICTIONS SUR LE TEST SET
    // =====================================================
    println("\n🔮 Prédictions sur le test set...")
    
    val predictions = model.transform(test)
    
    // =====================================================
    // 10. ÉVALUATION DU MODÈLE
    // =====================================================
    println("\n📊 MATRICE DE CONFUSION:")
    println("=" * 50)
    
    val confusionMatrix = predictions
      .groupBy("is_grave", "prediction")
      .count()
      .orderBy("is_grave", "prediction")
    
    confusionMatrix.show()
    
    // Calcul des métriques
    val accuracyEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("is_grave")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    
    val f1Evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("is_grave")
      .setPredictionCol("prediction")
      .setMetricName("f1")
    
    val precisionEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("is_grave")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")
    
    val recallEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("is_grave")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")
    
    val accuracy  = accuracyEvaluator.evaluate(predictions)
    val f1Score   = f1Evaluator.evaluate(predictions)
    val precision = precisionEvaluator.evaluate(predictions)
    val recall    = recallEvaluator.evaluate(predictions)
    
    println("\n📈 MÉTRIQUES D'ÉVALUATION:")
    println("=" * 50)
    println(f"✅ Accuracy           : ${accuracy * 100}%.2f %%")
    println(f"✅ F1-Score          : ${f1Score * 100}%.2f %%")
    println(f"✅ Precision (pondérée) : ${precision * 100}%.2f %%")
    println(f"✅ Recall (pondéré)     : ${recall * 100}%.2f %%")
    
    // =====================================================
    // 11. FEATURE IMPORTANCE
    // =====================================================
    println("\n🔥 FEATURE IMPORTANCE (TOP 10):")
    println("=" * 50)
    
    // Récupérer le modèle Random Forest entraîné
    val trainedRf = model.stages.last.asInstanceOf[RandomForestClassificationModel]
    
    // Récupérer les importances
    val featureImportances = trainedRf.featureImportances.toArray
    
    // Créer les noms de features (correspondant à l'ordre dans VectorAssembler)
    val featureNames = featureColNames
    
    // Créer un DataFrame avec les importances
    val importanceData = featureNames.zip(featureImportances)
      .sortBy(-_._2)
      .map { case (name, importance) => (name, importance) }
    
    val importanceDF = importanceData.toSeq.toDF("feature", "importance")
    
    println("\n🏆 TOP 10 FEATURES LES PLUS IMPORTANTES:")
    importanceDF.show(10, truncate = false)
    
    // =====================================================
    // 12. SAUVEGARDE DES RÉSULTATS
    // =====================================================
    println("\n💾 Sauvegarde des résultats...")
    
    // Créer le dossier output s'il n'existe pas
    val outputDir = "output"
    
    // Sauvegarder les prédictions avec coordonnées GPS
    // Note: On retire 'probability' car c'est un vecteur (type complexe non supporté par CSV)
    predictions
      .select("Num_Acc", "lat_clean", "long_clean", "lum", "atm", 
              "prediction", "is_grave")
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(s"$outputDir/predictions")
    
    println(s"✅ Prédictions sauvegardées dans '$outputDir/predictions/'")
    
    // Sauvegarder les feature importances
    importanceDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(s"$outputDir/feature_importance")
    
    println(s"✅ Feature importances sauvegardées dans '$outputDir/feature_importance/'")
    
    // Sauvegarder la matrice de confusion
    confusionMatrix
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(s"$outputDir/confusion_matrix")
    
    println(s"✅ Matrice de confusion sauvegardée dans '$outputDir/confusion_matrix/'")
    
    // Sauvegarder un résumé des métriques
    val metricsDF = Seq(
      ("accuracy", accuracy),
      ("f1_score", f1Score),
      ("precision", precision),
      ("recall", recall)
    ).toDF("metric", "value")
    
    metricsDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(s"$outputDir/metrics")
    
    println(s"✅ Métriques sauvegardées dans '$outputDir/metrics/'")
    
    println("\n" + "=" * 80)
    println("✨ MODÉLISATION TERMINÉE AVEC SUCCÈS !")
    println("=" * 80)
    
    spark.stop()
  }
}