:paste
// Rigour pass (docs/RESEARCH_PROTOCOL.md): Spark MLlib GBT baselines on the canonical feature table.
//   1. local results/rigour/feature_table.parquet -> HDFS /flu_project/processed/rigour_feature_table (the HDFS copy is the input)
//   2. GBT-lags (lag1-4 + trend, the v1 baseline) and GBT-all (+ climate), 50 iterations, depth 5, seed 42,
//      trained on YEAR <= 2022, predicting YEAR >= 2023 (same rows as every other model)
//   3. predictions -> HDFS /flu_project/processed/rigour_gbt_predictions/<model> and results/rigour/predictions/<model>.parquet
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.functions.col

val hdfsTable = "hdfs://namenode:9000/flu_project/processed/rigour_feature_table"
spark.read.parquet("file:///opt/results/rigour/feature_table.parquet").write.mode("overwrite").parquet(hdfsTable)
val feat  = spark.read.parquet(hdfsTable).cache()
val train = feat.filter(col("YEAR") <= 2022)
val test  = feat.filter(col("YEAR") >= 2023)
println(s"GBTV2 | rows total=${feat.count()} train=${train.count()} test=${test.count()}")

val lagCols = Seq("ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4", "ILI_trend")
val allCols = Seq("TMAX", "TMIN", "PRCP", "DIURNAL_RANGE") ++ lagCols
for ((name, cols) <- Seq("GBT-lags" -> lagCols, "GBT-all" -> allCols)) {
  val pipe = new Pipeline().setStages(Array(
    new VectorAssembler().setInputCols(cols.toArray).setOutputCol("features"),
    new GBTRegressor().setLabelCol("target").setFeaturesCol("features").setMaxIter(50).setMaxDepth(5).setSeed(42)))
  val t0 = System.nanoTime()
  val model = pipe.fit(train)
  val secs = (System.nanoTime() - t0) / 1e9
  val pred = model.transform(test).select(col("state_abbrev").as("state"), col("YEAR").as("year"), col("WEEK").as("week"),
                                          col("prediction").as("pred"), col("target"))
  pred.write.mode("overwrite").parquet(s"hdfs://namenode:9000/flu_project/processed/rigour_gbt_predictions/$name")
  pred.coalesce(1).write.mode("overwrite").parquet(s"file:///opt/results/rigour/predictions_spark/$name")
  val rmse = math.sqrt(pred.selectExpr("avg(pow(pred - target, 2))").first().getDouble(0))
  println(f"GBTV2 | $name%-8s features=${cols.size} fit=${secs}%.1fs test RMSE=$rmse%.4f rows=${pred.count()}")
}
System.exit(0)
