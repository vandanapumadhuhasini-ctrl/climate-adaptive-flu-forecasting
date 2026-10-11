:paste
val hdfsBase     = "hdfs://namenode:9000"
val processedDir = s"$hdfsBase/flu_project/processed"

import org.apache.spark.sql.functions.{col => c, sqrt, pow, avg, stddev, exp, lit, row_number}
import org.apache.spark.sql.expressions.Window

val modelingTable = spark.read.parquet(s"$processedDir/modeling_table")
val climateCols = Seq("TMAX", "TMIN", "PRCP", "DIURNAL_RANGE")

// ---- Global standardization stats (z-score each climate feature) ----
val stats = climateCols.map { colName =>
  val row = modelingTable.agg(avg(c(colName)).as("mean"), stddev(c(colName)).as("std")).first()
  colName -> (row.getAs[Double]("mean"), row.getAs[Double]("std"))
}.toMap

var standardized = modelingTable.select(
  (Seq("state_abbrev", "YEAR", "WEEK") ++ climateCols).map(cn => c(cn)): _*
)
climateCols.foreach { cn =>
  val (mean, std) = stats(cn)
  standardized = standardized.withColumn(s"${cn}_z", (c(cn) - lit(mean)) / lit(std))
}
val zCols = climateCols.map(cn => s"${cn}_z")

val left  = standardized.select((Seq("state_abbrev", "YEAR", "WEEK") ++ zCols).map(cn => c(cn).as(s"i_$cn")): _*)
val right = standardized.select((Seq("state_abbrev", "YEAR", "WEEK") ++ zCols).map(cn => c(cn).as(s"j_$cn")): _*)

val pairs = left.join(
  right,
  c("i_YEAR") === c("j_YEAR") && c("i_WEEK") === c("j_WEEK") && c("i_state_abbrev") =!= c("j_state_abbrev")
)

val sqDist = zCols.map(cn => pow(c(s"i_$cn") - c(s"j_$cn"), 2)).reduce(_ + _)
val withDist = pairs.withColumn("dist", sqrt(sqDist))

val sigmaRow = withDist.agg(avg(c("dist")).as("meanDist")).first()
val sigma = sigmaRow.getAs[Double]("meanDist")
println(s"Gaussian kernel bandwidth (mean pairwise distance): $sigma")

val withSim = withDist.withColumn("similarity", exp(-pow(c("dist"), 2) / lit(2 * sigma * sigma)))

val K = 8
val w = Window.partitionBy(c("i_state_abbrev"), c("i_YEAR"), c("i_WEEK")).orderBy(c("similarity").desc)

val knnGraph = withSim
  .withColumn("rank", row_number().over(w))
  .filter(c("rank") <= K)
  .select(
    c("i_state_abbrev").as("state_i"),
    c("j_state_abbrev").as("state_j"),
    c("i_YEAR").as("YEAR"),
    c("i_WEEK").as("WEEK"),
    c("dist"),
    c("similarity")
  )

val edgeCount = knnGraph.count()
println(s"Total directed KNN edges (K=$K): $edgeCount")

knnGraph.orderBy(c("YEAR"), c("WEEK"), c("state_i"), c("similarity").desc).show(20, truncate = false)

knnGraph.write.mode("overwrite").parquet(s"$processedDir/graph_edges_knn")
println(s"Written to $processedDir/graph_edges_knn")