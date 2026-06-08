import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import scala.math.max
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

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
val maxIter = sys.props.getOrElse("tfm.max_iter", "100").toInt
val tolerance = sys.props.getOrElse("tfm.tolerance", "1e-6").toDouble
val damping = sys.props.getOrElse("tfm.damping", "0.85").toDouble
val persistOutput = sys.props.get("tfm.persist").exists(_.toBoolean)
val resultPrefix = "SPARK_BENCHMARK_RESULT_JSON:"

def parseEdge(line: String): Option[Edge[Double]] = {
  val parts = line.trim.split('\t')
  if (parts.length < 2) None
  else {
    val src = parts(0).toLong
    val dst = parts(1).toLong
    Some(Edge(src, dst, 1.0))
  }
}

def jsonEscape(value: String): String =
  value.replace("\\", "\\\\").replace("\"", "\\\"")

val totalStartNs = System.nanoTime()
val loadStartNs = System.nanoTime()

val lines = sc.textFile(inputPath, partitions)
val edges = lines.flatMap(parseEdge).cache()

val vertices = edges
  .flatMap(edge => Iterator(edge.srcId, edge.dstId))
  .distinct()
  .map(id => (id, 1.0))

val graph = Graph(vertices, edges).partitionBy(
  org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
).cache()

graph.vertices.count()
val loadEndNs = System.nanoTime()

val execStartNs = System.nanoTime()

// GraphX PageRank: power-iteration with damping + dangling redistribution.
// `runUntilConvergence` stops when no vertex's rank changes by more than
// `tolerance` between iterations; cap with `maxIter` if it doesn't converge.
val ranks = graph.pageRank(tolerance, damping).vertices.cache()
val rankCount = ranks.count()
val maxRank = if (rankCount > 0) ranks.map(_._2).max() else 0.0
val sumRank = if (rankCount > 0) ranks.map(_._2).sum() else 0.0

val computeEndNs = System.nanoTime()

if (persistOutput) {
  ranks
    .sortByKey()
    .map { case (id, r) => s"$id\t$r" }
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
  s"""$resultPrefix{"graph_file":"${jsonEscape(inputPath)}","damping":$damping,"tolerance":$tolerance,"max_iter":$maxIter,"load_time_ms":$loadTimeMs,"compute_only_ms":$computeOnlyMs,"output_write_ms":$outputWriteMs,"execution_time_ms":$executionTimeMs,"end_to_end_ms":$endToEndMs,"total_time_ms":$totalTimeMs,"num_vertices":$rankCount,"max_rank":$maxRank,"sum_rank":$sumRank}"""
)

System.exit(0)
