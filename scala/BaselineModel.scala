:paste
val hdfsBase     = "hdfs://namenode:9000"
val processedDir = s"$hdfsBase/flu_project/processed"

import org.apache.spark.sql.functions.{col => c, lag, lead}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.Pipeline

// ---- Load and repartition by state (co-locates each state's series for the window functions below) ----
val raw = spark.read.parquet(s"$processedDir/modeling_table")
  .repartition(8, c("state_abbrev"))
  .na.drop(Seq("TMAX", "TMIN", "PRCP", "DIURNAL_RANGE", "UNWEIGHTED_ILI"))

val t0 = System.currentTimeMillis()
raw.cache()
val rawCount = raw.count()   // materializes the cache
val t1 = System.currentTimeMillis()
val recount = raw.count()    // re-read from cache — should be much faster
val t2 = System.currentTimeMillis()
println(s"Rows: $rawCount | First count (cold, triggers cache): ${t1 - t0}ms | Second count (from cache): ${t2 - t1}ms")

// ---- Build lag (autoregressive feature) and lead (forecast target), per state, ordered chronologically ----
val w = Window.partitionBy("state_abbrev").orderBy("YEAR", "WEEK")

val withLagLead = raw
  .withColumn("ILI_lag1", lag(c("UNWEIGHTED_ILI"), 1).over(w))
  .withColumn("ILI_lag2", lag(c("UNWEIGHTED_ILI"), 2).over(w))
  .withColumn("ILI_lag3", lag(c("UNWEIGHTED_ILI"), 3).over(w))
  .withColumn("ILI_lag4", lag(c("UNWEIGHTED_ILI"), 4).over(w))
  .withColumn("ILI_trend", c("ILI_lag1") - c("ILI_lag2"))   // simple momentum signal
  .withColumn("target_next_week_ili", lead(c("UNWEIGHTED_ILI"), 1).over(w))
  .na.drop(Seq("ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4", "target_next_week_ili"))

val modelDataCount = withLagLead.count()
println(s"Rows after lag/lead + dropna (loses first+last week per state): $modelDataCount")

// ---- Temporal train/test split — NOT random, respects chronological order ----
val train = withLagLead.filter(c("YEAR") <= 2022)
val test  = withLagLead.filter(c("YEAR") >= 2023)
val trainCount = train.count()
val testCount  = test.count()
println(s"Train rows (<=2022): $trainCount | Test rows (>=2023): $testCount")

train.cache()
test.cache()

// ---- Baseline 1: naive persistence (predict next week = this week) ----
val persistenceErrors = test.withColumn("abs_err", org.apache.spark.sql.functions.abs(c("target_next_week_ili") - c("ILI_lag1")))
  .withColumn("sq_err", org.apache.spark.sql.functions.pow(c("target_next_week_ili") - c("ILI_lag1"), 2))
val persistenceMAE  = persistenceErrors.agg(org.apache.spark.sql.functions.avg("abs_err")).first().getDouble(0)
val persistenceRMSE = math.sqrt(persistenceErrors.agg(org.apache.spark.sql.functions.avg("sq_err")).first().getDouble(0))
println(f"Persistence baseline — MAE: $persistenceMAE%.4f | RMSE: $persistenceRMSE%.4f")

// ---- Baseline 2: Spark MLlib GBTRegressor ----
val featureCols = Array("TMAX", "TMIN", "PRCP", "DIURNAL_RANGE", "ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4", "ILI_trend")
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
val gbt = new GBTRegressor()
  .setFeaturesCol("features")
  .setLabelCol("target_next_week_ili")
  .setMaxIter(50)
  .setMaxDepth(5)

val pipeline = new Pipeline().setStages(Array(assembler, gbt))

val trainStart = System.currentTimeMillis()
val model = pipeline.fit(train)
val trainEnd = System.currentTimeMillis()
println(s"GBTRegressor training time: ${trainEnd - trainStart}ms")

val predictions = model.transform(test)

val evaluatorRMSE = new RegressionEvaluator().setLabelCol("target_next_week_ili").setPredictionCol("prediction").setMetricName("rmse")
val evaluatorMAE  = new RegressionEvaluator().setLabelCol("target_next_week_ili").setPredictionCol("prediction").setMetricName("mae")
val evaluatorR2   = new RegressionEvaluator().setLabelCol("target_next_week_ili").setPredictionCol("prediction").setMetricName("r2")

val gbtRMSE = evaluatorRMSE.evaluate(predictions)
val gbtMAE  = evaluatorMAE.evaluate(predictions)
val gbtR2   = evaluatorR2.evaluate(predictions)

println(f"GBTRegressor — RMSE: $gbtRMSE%.4f | MAE: $gbtMAE%.4f | R2: $gbtR2%.4f")
println(f"Persistence  — RMSE: $persistenceRMSE%.4f | MAE: $persistenceMAE%.4f")
println(f"Improvement over persistence — RMSE: ${(1 - gbtRMSE/persistenceRMSE)*100}%.2f%% | MAE: ${(1 - gbtMAE/persistenceMAE)*100}%.2f%%")

predictions.select("state_abbrev", "YEAR", "WEEK", "ILI_lag1", "target_next_week_ili", "prediction")
  .orderBy("state_abbrev", "YEAR", "WEEK").show(15, truncate = false)

predictions.select("state_abbrev", "YEAR", "WEEK", "target_next_week_ili", "prediction")
  .write.mode("overwrite").parquet(s"$processedDir/baseline_predictions")
println(s"Written to $processedDir/baseline_predictions")
model.write.overwrite().save("hdfs://namenode:9000/flu_project/processed/baseline_model_artifact")
println(s"Written to hdfs://namenode:9000/flu_project/processed/baseline_model_artifact")