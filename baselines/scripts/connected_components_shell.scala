import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
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
val persistOutput = sys.props.get("tfm.persist").exists(_.toBoolean)
val resultPrefix = "SPARK_BENCHMARK_RESULT_JSON:"

def parseUndirectedEdges(line: String): Iterator[Edge[Int]] = {
  val parts = line.trim.split('\t')
  if (parts.length < 2) Iterator.empty
  else {
    val src = parts(0).toLong
    val dst = parts(1).toLong
    Iterator(Edge(src, dst, 1), Edge(dst, src, 1))
  }
}

def jsonEscape(value: String): String =
  value.replace("\\", "\\\\").replace("\"", "\\\"")

def fnv64(value: Long, acc: Long): Long = {
  val mixed = acc ^ value
  (mixed * 0x100000001b3L) & 0xffffffffffffffffL
}

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
val edges = lines.flatMap(parseUndirectedEdges).cache()

val vertices = edges
  .flatMap(edge => Iterator(edge.srcId, edge.dstId))
  .distinct()
  .map(vertexId => (vertexId, 0))

val graph = Graph(vertices, edges).partitionBy(
  org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
).cache()

graph.vertices.count()
val loadEndNs = System.nanoTime()

val execStartNs = System.nanoTime()

val components = graph.connectedComponents().vertices.cache()
val componentCount = components.map { case (_, component) => component }.distinct().count()
val componentHash = components
  .sortByKey()
  .map { case (_, component) => component }
  .treeAggregate(0xcbf29ce484222325L)(
    (acc, component) => fnv64(component, acc),
    (left, right) => fnv64(right, left)
  )

val computeEndNs = System.nanoTime()

if (persistOutput) {
  components
    .sortByKey()
    .map { case (id, component) => s"$id\t$component" }
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
  s"""$resultPrefix{"graph_file":"${jsonEscape(inputPath)}","load_time_ms":$loadTimeMs,"compute_only_ms":$computeOnlyMs,"output_write_ms":$outputWriteMs,"execution_time_ms":$executionTimeMs,"end_to_end_ms":$endToEndMs,"total_time_ms":$totalTimeMs,"num_components":$componentCount,"component_hash":"${java.lang.Long.toUnsignedString(componentHash, 16)}"}"""
)

System.exit(0)
