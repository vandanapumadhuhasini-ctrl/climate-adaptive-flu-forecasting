:paste
// A2 (docs/RESEARCH_PROTOCOL.md): Spark MLlib comparison — LinearRegression, RandomForestRegressor, GBTRegressor,
// feature sets `lags` and `all`, horizons 1..4, on HDFS processed/v3_features/h<h>.
// Model selection: ParamGridBuilder grid, each point fit on YEAR <= 2021 and scored on 2022 (RegressionEvaluator, RMSE)
// -- a temporal hold-out instead of TrainValidationSplit's random split, so no future weeks leak into training.
// Best point refit on YEAR <= 2022, test 2023-2025 predicted once.
// Outputs: HDFS processed/mllib_predictions/<model>_<fs>_h<h>, results/mllib/predictions/..., results/mllib/selection.jsonl
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression._
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.functions.col
import java.nio.file.{Files, Paths, StandardOpenOption}

val base = "hdfs://namenode:9000/flu_project/processed"
val lags = Array("ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4", "ILI_trend")
val featureSets = Seq("lags" -> lags, "all" -> (lags ++ Array("TMAX", "TMIN", "PRCP", "DIURNAL_RANGE")))
val evaluator = new RegressionEvaluator().setLabelCol("target").setPredictionCol("prediction").setMetricName("rmse")
val outFile = Paths.get("/opt/results/mllib/selection.jsonl")
Files.createDirectories(outFile.getParent)
Files.deleteIfExists(outFile)
def log(s: String): Unit = Files.write(outFile, (s + "\n").getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
def j(s: String) = "\"" + s + "\""

for (h <- 1 to 4) {
  val feat = spark.read.parquet(s"$base/v3_features/h$h").cache()
  val tr = feat.filter(col("YEAR") <= 2021).cache()
  val va = feat.filter(col("YEAR") === 2022).cache()
  val trFull = feat.filter(col("YEAR") <= 2022).cache()
  val te = feat.filter(col("YEAR") >= 2023).cache()
  for ((fsName, cols) <- featureSets) {
    val asm = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    val lr = new LinearRegression().setLabelCol("target").setFeaturesCol("features").setStandardization(true)
    val rf = new RandomForestRegressor().setLabelCol("target").setFeaturesCol("features").setSeed(42)
    val gbt = new GBTRegressor().setLabelCol("target").setFeaturesCol("features").setSeed(42).setStepSize(0.1)
    val candidates = Seq(
      ("LinearRegression", lr, new ParamGridBuilder().addGrid(lr.regParam, Array(0.0, 0.01, 0.1, 1.0))
                                                     .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0)).build()),
      ("RandomForest", rf, new ParamGridBuilder().addGrid(rf.numTrees, Array(50, 100, 200)).addGrid(rf.maxDepth, Array(5, 10)).build()),
      ("GBT", gbt, new ParamGridBuilder().addGrid(gbt.maxIter, Array(50, 100)).addGrid(gbt.maxDepth, Array(3, 5)).build()))
    for ((name, est, grid) <- candidates) {
      val pipe = new Pipeline().setStages(Array(asm, est))
      // 1. temporal model selection on 2022
      val scored = grid.map { pm => (pm, evaluator.evaluate(pipe.fit(tr, pm).transform(va))) }
      val (bestPm, bestVal) = scored.minBy(_._2)
      // 2. refit on <= 2022 with the chosen parameters, predict the test set once
      val t0 = System.nanoTime()
      val model = pipe.fit(trFull, bestPm)
      val fitS = (System.nanoTime() - t0) / 1e9
      val pred = model.transform(te).select(col("state_abbrev").as("state"), col("YEAR").as("year"), col("WEEK").as("week"),
                                            col("prediction").as("pred"), col("target"))
      val tag = s"${name}_${fsName}_h$h"
      pred.write.mode("overwrite").parquet(s"$base/mllib_predictions/$tag")
      pred.coalesce(1).write.mode("overwrite").parquet(s"file:///opt/results/mllib/predictions/$tag")
      val testRmse = math.sqrt(pred.selectExpr("avg(pow(pred - target, 2))").first().getDouble(0))
      val fitted = model.stages(1)
      val explain = fitted match {
        case m: LinearRegressionModel => s""""coefficients": {${cols.zip(m.coefficients.toArray).map { case (c, v) => s"${j(c)}: $v" }.mkString(", ")}}, "intercept": ${m.intercept}"""
        case m: RandomForestRegressionModel => s""""feature_importances": {${cols.zip(m.featureImportances.toArray).map { case (c, v) => s"${j(c)}: $v" }.mkString(", ")}}"""
        case m: GBTRegressionModel => s""""feature_importances": {${cols.zip(m.featureImportances.toArray).map { case (c, v) => s"${j(c)}: $v" }.mkString(", ")}}"""
      }
      val chosen = bestPm.toSeq.map(p => s"${j(p.param.name)}: ${p.value}").mkString(", ")
      val gridJson = scored.map { case (pm, v) => s"""{${pm.toSeq.map(p => s"${j(p.param.name)}: ${p.value}").mkString(", ")}, "val_rmse": $v}""" }.mkString(", ")
      log(s"""{"model": ${j(name)}, "features": ${j(fsName)}, "horizon": $h, "chosen": {$chosen}, "val_rmse": $bestVal, "test_rmse": $testRmse, "fit_seconds": $fitS, "n_train": ${trFull.count()}, "n_test": ${te.count()}, $explain, "grid": [$gridJson]}""")
      println(f"MLLIB | h=$h $fsName%-4s $name%-16s val=$bestVal%.4f test=$testRmse%.4f fit=$fitS%.1fs chosen=${bestPm.toSeq.map(p => s"${p.param.name}=${p.value}").mkString(",")}")
    }
  }
  Seq(feat, tr, va, trFull, te).foreach(_.unpersist())
}
println("MLLIB | done -> results/mllib/selection.jsonl")
System.exit(0)
