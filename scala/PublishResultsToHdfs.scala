:paste
// Publish every result / evidence file (results/ = /opt/results) into HDFS /flu_project/results/, so the whole project
// lives in HDFS: predictions of the PyTorch models, metrics, summaries, logs of runs and Spark event logs.
// Writes a SHA-256 manifest (results/hdfs_publish_manifest.json, also copied to HDFS) and verifies sizes after the copy
// by reading the HDFS namespace back.
import org.apache.hadoop.fs.{FileSystem, Path}
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import scala.collection.JavaConverters._

val local = Paths.get("/opt/results")
val dest = "hdfs://namenode:9000/flu_project/results"
val fs = FileSystem.get(new java.net.URI("hdfs://namenode:9000"), sc.hadoopConfiguration)
def sha256(p: java.nio.file.Path): String = {
  val md = MessageDigest.getInstance("SHA-256"); val in = Files.newInputStream(p); val buf = new Array[Byte](1 << 20)
  try { var n = in.read(buf); while (n > 0) { md.update(buf, 0, n); n = in.read(buf) } } finally in.close()
  md.digest().map("%02x".format(_)).mkString
}
val files = Files.walk(local).iterator().asScala.filter(p => Files.isRegularFile(p))
  .filterNot(_.getFileName.toString == "hdfs_publish_manifest.json")
  // skip only event logs still being written (e.g. this very job); keep stale .inprogress logs of crashed runs (evidence)
  .filterNot(p => p.getFileName.toString.endsWith(".inprogress") &&
             System.currentTimeMillis() - Files.getLastModifiedTime(p).toMillis < 10 * 60 * 1000)
  .toList.sortBy(_.toString)   // toList: eager, so the timer below is real
val t0 = System.currentTimeMillis()
val entries = files.map { p =>
  val rel = local.relativize(p).toString.replace('\\', '/')
  val target = new Path(s"$dest/$rel")
  fs.copyFromLocalFile(false, true, new Path(p.toUri), target)
  val hdfsLen = fs.getFileStatus(target).getLen
  (rel, Files.size(p), hdfsLen, sha256(p))
}
val secs = (System.currentTimeMillis() - t0) / 1000.0
val mismatch = entries.count(e => e._2 != e._3)
val json = s"""{
  "generated_at": "${java.time.Instant.now()}",
  "hdfs_destination": "$dest",
  "files": ${entries.size}, "bytes": ${entries.map(_._2).sum}, "size_mismatches_after_copy": $mismatch, "copy_seconds": $secs,
  "entries": [
${entries.map(e => s"""    {"path": "${e._1}", "bytes": ${e._2}, "hdfs_bytes": ${e._3}, "sha256": "${e._4}"}""").mkString(",\n")}
  ]
}
"""
Files.write(local.resolve("hdfs_publish_manifest.json"), json.getBytes("UTF-8"))
fs.copyFromLocalFile(false, true, new Path(local.resolve("hdfs_publish_manifest.json").toUri), new Path(s"$dest/hdfs_publish_manifest.json"))
val summary = fs.getContentSummary(new Path(dest))
println(f"PUBLISH | ${entries.size} files, ${entries.map(_._2).sum / 1e6}%.1f MB -> $dest in $secs%.1f s; size mismatches: $mismatch; " +
        s"HDFS now holds ${summary.getFileCount} files, ${summary.getLength} B (x${fs.getDefaultReplication(new Path(dest))} replication)")
System.exit(0)
