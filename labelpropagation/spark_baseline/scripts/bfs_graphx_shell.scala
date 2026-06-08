import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import scala.math.min
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
val sourceNode = sys.props.getOrElse("tfm.source", {
  System.err.println("Missing -Dtfm.source")
  System.exit(1)
  ""
}).toLong
val partitions = sys.props.getOrElse("tfm.partitions", "4").toInt
val maxLevels = sys.props.getOrElse("tfm.max_levels", "500").toInt
val persistOutput = sys.props.get("tfm.persist").exists(_.toBoolean)
val inf = Int.MaxValue
val resultPrefix = "SPARK_BENCHMARK_RESULT_JSON:"

def parseEdge(line: String): Option[Edge[Int]] = {
  val parts = line.trim.split('\t')
  if (parts.length < 2) None
  else Some(Edge(parts(0).toLong, parts(1).toLong, 1))
}

def jsonEscape(value: String): String =
  value.replace("\\", "\\\\").replace("\"", "\\\"")

def persistLinesFromDriver(outputDir: String, lines: Iterator[String]): Unit = {
  val dirPath = Paths.get(outputDir)
  Files.createDirectories(dirPath)
  val partPath = dirPath.resolve("part-00000")
  val writer = new PrintWriter(Files.newBufferedWriter(partPath, StandardCharsets.UTF_8))
  try {
    lines.foreach(writer.println)
  } finally {
    writer.close()
  }
  val successPath = dirPath.resolve("_SUCCESS")
  if (!Files.exists(successPath)) {
    Files.createFile(successPath)
  }
}

val totalStartNs = System.nanoTime()
val loadStartNs = System.nanoTime()

val lines = sc.textFile(inputPath, partitions)
val edges = lines.flatMap(parseEdge).cache()

val vertices = edges
  .flatMap(edge => Iterator(edge.srcId, edge.dstId))
  .distinct()
  .map { vertexId =>
    val initialDistance = if (vertexId == sourceNode) 0 else inf
    (vertexId, initialDistance)
  }

val graph = Graph(vertices, edges).partitionBy(
  org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
).cache()

graph.vertices.count()
val loadEndNs = System.nanoTime()

val execStartNs = System.nanoTime()

val initialGraph = graph.mapVertices {
  case (vertexId, _) => if (vertexId == sourceNode) 0 else inf
}

val bfs = initialGraph.pregel(inf, maxIterations = maxLevels)(
  (id: VertexId, currentDistance: Int, newDistance: Int) => min(currentDistance, newDistance),
  triplet => {
    if (triplet.srcAttr != inf && triplet.srcAttr < maxLevels && triplet.srcAttr + 1 < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + 1))
    } else {
      Iterator.empty
    }
  },
  (a, b) => min(a, b)
).cache()

val levels = bfs.vertices.cache()
val reachable = levels.filter { case (_, level) => level != inf }.cache()
val visitedNodes = reachable.count()
val maxLevel = if (visitedNodes > 0) reachable.map(_._2).max() else 0

val computeEndNs = System.nanoTime()

if (persistOutput) {
  levels
    .sortByKey()
    .map { case (id, level) =>
      val rendered = if (level == inf) "UNVISITED" else level.toString
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
  s"""$resultPrefix{"graph_file":"${jsonEscape(inputPath)}","source_node":$sourceNode,"max_levels":$maxLevels,"load_time_ms":$loadTimeMs,"compute_only_ms":$computeOnlyMs,"output_write_ms":$outputWriteMs,"execution_time_ms":$executionTimeMs,"end_to_end_ms":$endToEndMs,"total_time_ms":$totalTimeMs,"visited_nodes":$visitedNodes,"max_level":$maxLevel}"""
)

System.exit(0)
