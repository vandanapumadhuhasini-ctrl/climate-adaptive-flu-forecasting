:paste
// v3 (A1): Spark MLlib GBT baselines per horizon on the HDFS feature tables processed/v3_features/h<h>.
//   GBT-lags       : ILI lag1-4 + trend
//   GBT-lagclimate : + lead-aligned climate (TMAX_al, TMIN_al, PRCP_al, DIURNAL_RANGE_al)
// 50 iterations, depth 5, seed 42; train YEAR <= 2022, predict YEAR >= 2023.
// Predictions -> HDFS processed/v3_gbt_predictions/<model>_h<h> and results/v3/predictions_spark/<model>_h<h>.
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.functions.col

val base = "hdfs://namenode:9000/flu_project/processed"
val lags = Seq("ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4", "ILI_trend")
val aligned = Seq("TMAX_al", "TMIN_al", "PRCP_al", "DIURNAL_RANGE_al")
for (h <- 1 to 4) {
  val feat = spark.read.parquet(s"$base/v3_features/h$h").cache()
  val train = feat.filter(col("YEAR") <= 2022)
  val test = feat.filter(col("YEAR") >= 2023)
  for ((name, cols) <- Seq("GBT-lags" -> lags, "GBT-lagclimate" -> (lags ++ aligned))) {
    val model = new Pipeline().setStages(Array(
      new VectorAssembler().setInputCols(cols.toArray).setOutputCol("features"),
      new GBTRegressor().setLabelCol("target").setFeaturesCol("features").setMaxIter(50).setMaxDepth(5).setSeed(42))).fit(train)
    val pred = model.transform(test).select(col("state_abbrev").as("state"), col("YEAR").as("year"), col("WEEK").as("week"),
                                            col("prediction").as("pred"), col("target"))
    pred.write.mode("overwrite").parquet(s"$base/v3_gbt_predictions/${name}_h$h")
    pred.coalesce(1).write.mode("overwrite").parquet(s"file:///opt/results/v3/predictions_spark/${name}_h$h")
    val rmse = math.sqrt(pred.selectExpr("avg(pow(pred - target, 2))").first().getDouble(0))
    println(f"GBTV3 | h=$h $name%-15s test RMSE=$rmse%.4f rows=${pred.count()}")
  }
  feat.unpersist()
}
System.exit(0)
