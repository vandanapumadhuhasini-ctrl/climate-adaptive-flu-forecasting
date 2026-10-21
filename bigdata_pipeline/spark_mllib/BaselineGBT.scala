:paste
// Spark MLlib GBT baselines (docs/RESEARCH_PROTOCOL.md): 50 iterations, depth 5, seed 42; train YEAR <= 2022, predict YEAR >= 2023.
//   v2 (rigour pass, next week): local results/rigour/feature_table.parquet -> HDFS processed/rigour_feature_table (the HDFS
//      copy is the input); GBT-lags (lag1-4 + trend) and GBT-all (+ climate)
//      -> HDFS processed/rigour_gbt_predictions/<model>, results/rigour/predictions_spark/<model>
//   v3 (amendment A1, horizons 1-4): HDFS processed/v3_features/h<h>; GBT-lags and GBT-lagclimate (+ lead-aligned climate)
//      -> HDFS processed/v3_gbt_predictions/<model>_h<h>, results/v3/predictions_spark/<model>_h<h>
// Which part runs: --conf spark.flu.gbt=v2 | v3 | all (default all).
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

val base = "hdfs://namenode:9000/flu_project/processed"
val part = spark.conf.get("spark.flu.gbt", "all")
val lags = Seq("ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4", "ILI_trend")

// fit one GBT on YEAR <= 2022, predict YEAR >= 2023, write the predictions to HDFS and results/, return the test RMSE
def gbt(feat: DataFrame, cols: Seq[String], hdfsOut: String, localOut: String): Double = {
  val model = new Pipeline().setStages(Array(
    new VectorAssembler().setInputCols(cols.toArray).setOutputCol("features"),
    new GBTRegressor().setLabelCol("target").setFeaturesCol("features").setMaxIter(50).setMaxDepth(5).setSeed(42)))
    .fit(feat.filter(col("YEAR") <= 2022))
  val pred = model.transform(feat.filter(col("YEAR") >= 2023)).select(col("state_abbrev").as("state"), col("YEAR").as("year"),
    col("WEEK").as("week"), col("prediction").as("pred"), col("target"))
  pred.write.mode("overwrite").parquet(hdfsOut)
  pred.coalesce(1).write.mode("overwrite").parquet(localOut)
  math.sqrt(pred.selectExpr("avg(pow(pred - target, 2))").first().getDouble(0))
}

if (part == "v2" || part == "all") {
  spark.read.parquet("file:///opt/results/rigour/feature_table.parquet").write.mode("overwrite").parquet(s"$base/rigour_feature_table")
  val feat = spark.read.parquet(s"$base/rigour_feature_table").cache()
  println(s"GBTV2 | rows total=${feat.count()} train=${feat.filter(col("YEAR") <= 2022).count()} test=${feat.filter(col("YEAR") >= 2023).count()}")
  for ((name, cols) <- Seq("GBT-lags" -> lags, "GBT-all" -> (Seq("TMAX", "TMIN", "PRCP", "DIURNAL_RANGE") ++ lags))) {
    val t0 = System.nanoTime()
    val rmse = gbt(feat, cols, s"$base/rigour_gbt_predictions/$name", s"file:///opt/results/rigour/predictions_spark/$name")
    println(f"GBTV2 | $name%-8s features=${cols.size} fit+predict=${(System.nanoTime() - t0) / 1e9}%.1fs test RMSE=$rmse%.4f")
  }
  feat.unpersist()
}

if (part == "v3" || part == "all") {
  val aligned = Seq("TMAX_al", "TMIN_al", "PRCP_al", "DIURNAL_RANGE_al")
  for (h <- 1 to 4) {
    val feat = spark.read.parquet(s"$base/v3_features/h$h").cache()
    for ((name, cols) <- Seq("GBT-lags" -> lags, "GBT-lagclimate" -> (lags ++ aligned))) {
      val rmse = gbt(feat, cols, s"$base/v3_gbt_predictions/${name}_h$h", s"file:///opt/results/v3/predictions_spark/${name}_h$h")
      println(f"GBTV3 | h=$h $name%-15s test RMSE=$rmse%.4f")
    }
    feat.unpersist()
  }
}
System.exit(0)
