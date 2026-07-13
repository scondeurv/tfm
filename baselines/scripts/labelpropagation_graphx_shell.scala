import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId

case class LabelState(label: Long, seeded: Boolean)

val inputPath = sys.props.getOrElse("tfm.input", {
  System.err.println("Missing -Dtfm.input")
  System.exit(1)
  ""
})
val outputPath = sys.props.getOrElse("tfm.output", {
  System.err.println("Missing -Dtfm.output")
  System.exit(1)
  ""
})
val partitions = sys.props.getOrElse("tfm.partitions", "4").toInt
val maxIter = sys.props.getOrElse("tfm.max_iter", "10").toInt
val persistOutput = sys.props.get("tfm.persist").exists(_.toBoolean)
val unknown = Long.MaxValue
val resultPrefix = "SPARK_BENCHMARK_RESULT_JSON:"

def parseLine(line: String): Option[(Long, Long, Option[Long])] = {
  val parts = line.trim.split('\t')
  if (parts.length < 2) None
  else {
    val src = parts(0).toLong
    val dst = parts(1).toLong
    val seed = if (parts.length >= 3) Some(parts(2).toLong) else None
    Some((src, dst, seed))
  }
}

def mergeCounts(left: Map[Long, Int], right: Map[Long, Int]): Map[Long, Int] = {
  if (left.isEmpty) right
  else if (right.isEmpty) left
  else {
    if (left.size >= right.size) {
      right.foldLeft(left) { case (acc, (label, count)) =>
        acc.updated(label, acc.getOrElse(label, 0) + count)
      }
    } else {
      left.foldLeft(right) { case (acc, (label, count)) =>
        acc.updated(label, acc.getOrElse(label, 0) + count)
      }
    }
  }
}

def majorityLabel(counts: Map[Long, Int], current: Long): Long = {
  if (counts.isEmpty) current
  else {
    counts.foldLeft((current, 0)) { case ((bestLabel, bestCount), (label, count)) =>
      if (label == unknown) {
        (bestLabel, bestCount)
      } else if (count > bestCount || (count == bestCount && label < bestLabel)) {
        (label, count)
      } else {
        (bestLabel, bestCount)
      }
    }._1
  }
}

def jsonEscape(value: String): String =
  value.replace("\\", "\\\\").replace("\"", "\\\"")

val totalStartNs = System.nanoTime()
val loadStartNs = System.nanoTime()

val lines = sc.textFile(inputPath, partitions)
val parsed = lines.flatMap(parseLine).cache()
val edges = parsed.map { case (src, dst, _) => Edge(src, dst, 1) }.cache()
val seeds = parsed.flatMap {
  case (src, _, Some(label)) => Iterator((src, label))
  case _ => Iterator.empty
}.reduceByKey((left, _) => left).cache()

val supervised = !seeds.isEmpty()

val vertices = edges
  .flatMap(edge => Iterator(edge.srcId, edge.dstId))
  .distinct()
  .map((_, ()))
  .leftOuterJoin(seeds)
  .map { case (vertexId, (_, maybeSeed)) =>
    maybeSeed match {
      case Some(label) => (vertexId, LabelState(label, seeded = true))
      case None =>
        val initial = if (supervised) unknown else vertexId
        (vertexId, LabelState(initial, seeded = false))
    }
  }

var graph = Graph(vertices, edges).partitionBy(
  org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
).cache()

graph.vertices.count()
val loadEndNs = System.nanoTime()

val execStartNs = System.nanoTime()

var iterations = 0
var changed = true

while (iterations < maxIter && changed) {
  val messages = graph.aggregateMessages[Map[Long, Int]](
    ctx => {
      val neighborLabel = ctx.dstAttr.label
      if (neighborLabel != unknown) {
        ctx.sendToSrc(Map(neighborLabel -> 1))
      }
    },
    mergeCounts
  )

  val next = graph.outerJoinVertices(messages) {
    case (_: VertexId, state: LabelState, maybeCounts: Option[Map[Long, Int]]) =>
      if (supervised && state.seeded) {
        state
      } else {
        val newLabel = majorityLabel(maybeCounts.getOrElse(Map.empty), state.label)
        LabelState(newLabel, state.seeded)
      }
  }.cache()

  next.vertices.count()
  val changedCount = graph.vertices
    .join(next.vertices)
    .filter { case (_, (left, right)) => left.label != right.label }
    .count()

  graph.unpersist(blocking = false)
  graph = next
  iterations += 1
  changed = changedCount > 0
}

val labels = graph.vertices.cache()
val labeledNodes = labels.filter { case (_, state) => state.label != unknown }.count()
val distinctLabels = labels
  .map { case (_, state) => state.label }
  .filter(_ != unknown)
  .distinct()
  .count()

val computeEndNs = System.nanoTime()

if (persistOutput) {
  labels
    .map { case (id, state) =>
      val rendered = if (state.label == unknown) "UNKNOWN" else state.label.toString
      s"$id\t$rendered"
    }
    .saveAsTextFile(outputPath)
}

val execEndNs = System.nanoTime()

val loadTimeMs = (loadEndNs - loadStartNs) / 1000000L
val computeOnlyMs = (computeEndNs - execStartNs) / 1000000L
val outputWriteMs = (execEndNs - computeEndNs) / 1000000L
val executionTimeMs = (execEndNs - execStartNs) / 1000000L
val totalTimeMs = (execEndNs - totalStartNs) / 1000000L
val endToEndMs = totalTimeMs

println(
  s"""$resultPrefix{"graph_file":"${jsonEscape(inputPath)}","max_iter":$maxIter,"supervised":$supervised,"load_time_ms":$loadTimeMs,"compute_only_ms":$computeOnlyMs,"output_write_ms":$outputWriteMs,"execution_time_ms":$executionTimeMs,"end_to_end_ms":$endToEndMs,"total_time_ms":$totalTimeMs,"iterations":$iterations,"converged":${!changed},"labeled_nodes":$labeledNodes,"distinct_labels":$distinctLabels}"""
)

System.exit(0)
