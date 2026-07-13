package tfm.spark

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math.min

object SSSPGraphXJob {
  private val Inf = Double.PositiveInfinity

  private def parseEdge(line: String): Option[Edge[Double]] = {
    val parts = line.trim.split('\t')
    if (parts.length < 2) {
      None
    } else {
      val src = parts(0).toLong
      val dst = parts(1).toLong
      val weight = if (parts.length >= 3) parts(2).toDouble else 1.0
      Some(Edge(src, dst, weight))
    }
  }

  private def jsonEscape(value: String): String =
    value.replace("\\", "\\\\").replace("\"", "\\\"")

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        "Usage: SSSPGraphXJob <input> <output> <source-node> <partitions>"
      )
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val sourceNode = args(2).toLong
    val partitions = args(3).toInt

    val totalStartNs = System.nanoTime()
    val conf = new SparkConf()
      .setAppName(s"SSSPGraphXJob-$sourceNode")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    try {
      val loadStartNs = System.nanoTime()

      val lines = sc.textFile(inputPath, partitions)
      val edges = lines.flatMap(parseEdge).cache()

      val vertices = edges
        .flatMap(edge => Iterator(edge.srcId, edge.dstId))
        .distinct()
        .map { vertexId =>
          val initialDistance = if (vertexId == sourceNode) 0.0 else Inf
          (vertexId, initialDistance)
        }

      val graph = Graph(vertices, edges).partitionBy(
        org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
      ).cache()

      graph.vertices.count()
      val loadEndNs = System.nanoTime()

      val execStartNs = System.nanoTime()

      val initialGraph = graph.mapVertices {
        case (vertexId, _) => if (vertexId == sourceNode) 0.0 else Inf
      }

      val sssp = initialGraph.pregel(Inf)(
        (id: VertexId, currentDistance: Double, newDistance: Double) =>
          min(currentDistance, newDistance),
        triplet => {
          if (triplet.srcAttr != Inf && triplet.srcAttr + triplet.attr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          } else {
            Iterator.empty
          }
        },
        (a, b) => min(a, b)
      ).cache()

      val distances = sssp.vertices.cache()
      val reachable = distances.filter { case (_, d) => d != Inf }.cache()
      val reachableCount = reachable.count()
      val maxDistance = if (reachableCount > 0) reachable.map(_._2).max() else 0.0

      distances
        .map { case (id, d) =>
          val rendered = if (d == Inf) "Infinity" else d.toString
          s"$id\t$rendered"
        }
        .saveAsTextFile(outputPath)

      val execEndNs = System.nanoTime()

      val loadTimeMs = (loadEndNs - loadStartNs) / 1000000L
      val executionTimeMs = (execEndNs - execStartNs) / 1000000L
      val totalTimeMs = (execEndNs - totalStartNs) / 1000000L

      println(
        s"""{"graph_file":"${jsonEscape(inputPath)}","source_node":$sourceNode,"load_time_ms":$loadTimeMs,"execution_time_ms":$executionTimeMs,"total_time_ms":$totalTimeMs,"reachable_nodes":$reachableCount,"max_distance":$maxDistance}"""
      )
    } finally {
      sc.stop()
    }
  }
}
