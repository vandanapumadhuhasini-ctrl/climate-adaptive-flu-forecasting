// Typed records for the Dataset API. Preloaded with spark-shell -i (line by line, NOT :paste):
// Spark 3.5 cannot build encoders for case classes defined inside the same :paste block that uses them
// (NullPointerException in OuterScopes.getOuterScope). Used by scala/RddDatasetAudit.scala.
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
case class Obs(id: String, date: String, element: String, value: Option[Long], qflag: Option[String])
case class Station(id: String, state: String)
case class StateObs(state: String, year: String, element: String, value: Long)
case class CountSum(n: Long, s: Long)
object CountSumAgg extends Aggregator[StateObs, CountSum, CountSum] { def zero: CountSum = CountSum(0L, 0L); def reduce(b: CountSum, a: StateObs): CountSum = CountSum(b.n + 1, b.s + a.value); def merge(x: CountSum, y: CountSum): CountSum = CountSum(x.n + y.n, x.s + y.s); def finish(r: CountSum): CountSum = r; def bufferEncoder: Encoder[CountSum] = Encoders.product[CountSum]; def outputEncoder: Encoder[CountSum] = Encoders.product[CountSum] }
