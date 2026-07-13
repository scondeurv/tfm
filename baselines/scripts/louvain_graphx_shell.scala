import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
val metadataOutputPath = outputPath + "__benchmark_result"
val numNodes = sys.props.getOrElse("tfm.num_nodes", {
  System.err.println("Missing -Dtfm.num_nodes")
  System.exit(1)
  ""
}).toInt
val maxPasses = sys.props.getOrElse("tfm.max_passes", "20").toInt
val minGain = sys.props.getOrElse("tfm.min_gain", "0.000001").toDouble
val partitions = sys.props.getOrElse("tfm.partitions", "4").toInt
val persistOutput = sys.props.get("tfm.persist").exists(_.toBoolean)
val resultPrefix = "SPARK_BENCHMARK_RESULT_JSON:"

case class LouvainResult(
  communities: Array[Int],
  modularity: Double,
  numPasses: Int,
  numCommunities: Int
)

case class WeightedGraph(
  numNodes: Int,
  adj: Array[ArrayBuffer[(Int, Double)]],
  totalWeight: Double
) {
  def weightedDegree(node: Int): Double = adj(node).iterator.map(_._2).sum
}

def jsonEscape(value: String): String =
  value.replace("\\", "\\\\").replace("\"", "\\\"")

def parseWeightedEdge(line: String): Option[(Int, Int, Double)] = {
  val parts = line.trim.split('\t')
  if (parts.length < 2) None
  else {
    val src = parts(0).toInt
    val dst = parts(1).toInt
    val weight = if (parts.length >= 3) parts(2).toDouble else 1.0
    Some((src, dst, weight))
  }
}

def weightedGraphFromEdges(numNodes: Int, edges: Array[(Int, Int, Double)]): WeightedGraph = {
  val adj = Array.fill(numNodes)(ArrayBuffer.empty[(Int, Double)])
  var totalWeight = 0.0
  edges.foreach { case (u, v, w) =>
    adj(u).append((v, w))
    if (u != v) {
      adj(v).append((u, w))
      totalWeight += w
    } else {
      totalWeight += w
    }
  }
  WeightedGraph(numNodes, adj, totalWeight)
}

def computeModularity(graph: WeightedGraph, communities: Array[Int]): Double = {
  val m2 = 2.0 * graph.totalWeight
  if (m2 == 0.0) {
    0.0
  } else {
    val communityInternal = mutable.HashMap.empty[Int, Double]
    val communityDegree = mutable.HashMap.empty[Int, Double]

    var node = 0
    while (node < graph.numNodes) {
      val community = communities(node)
      val degree = graph.weightedDegree(node)
      communityDegree.update(community, communityDegree.getOrElse(community, 0.0) + degree)
      graph.adj(node).foreach { case (neighbor, weight) =>
        if (communities(neighbor) == community) {
          communityInternal.update(community, communityInternal.getOrElse(community, 0.0) + weight)
        }
      }
      node += 1
    }

    communityDegree.iterator.map { case (community, sigmaTot) =>
      val internal = communityInternal.getOrElse(community, 0.0)
      internal / m2 - math.pow(sigmaTot / m2, 2)
    }.sum
  }
}

def localMovePass(graph: WeightedGraph, communities: Array[Int]): Int = {
  val m2 = 2.0 * graph.totalWeight
  if (m2 == 0.0) {
    0
  } else {
    val sigmaTot = mutable.HashMap.empty[Int, Double]
    var node = 0
    while (node < graph.numNodes) {
      val community = communities(node)
      sigmaTot.update(community, sigmaTot.getOrElse(community, 0.0) + graph.weightedDegree(node))
      node += 1
    }

    var changed = 0
    node = 0
    while (node < graph.numNodes) {
      val currentCommunity = communities(node)
      val nodeDegree = graph.weightedDegree(node)
      val neighborCommunities = mutable.HashMap.empty[Int, Double]
      graph.adj(node).foreach { case (neighbor, weight) =>
        val community = communities(neighbor)
        neighborCommunities.update(community, neighborCommunities.getOrElse(community, 0.0) + weight)
      }

      val currentInside = neighborCommunities.getOrElse(currentCommunity, 0.0)
      val sigmaCurrent = sigmaTot.getOrElse(currentCommunity, 0.0)
      sigmaTot.update(currentCommunity, sigmaCurrent - nodeDegree)
      val removeCost = currentInside / m2 - (sigmaCurrent - nodeDegree) * nodeDegree / (m2 * m2)

      var bestCommunity = currentCommunity
      var bestGain = 0.0
      neighborCommunities.foreach { case (candidateCommunity, insideWeight) =>
        val sigmaCandidate = sigmaTot.getOrElse(candidateCommunity, 0.0)
        val gain = insideWeight / m2 - sigmaCandidate * nodeDegree / (m2 * m2) - removeCost
        if (gain > bestGain || (gain == bestGain && candidateCommunity < bestCommunity)) {
          bestGain = gain
          bestCommunity = candidateCommunity
        }
      }

      if (bestCommunity != currentCommunity) {
        communities(node) = bestCommunity
        sigmaTot.update(bestCommunity, sigmaTot.getOrElse(bestCommunity, 0.0) + nodeDegree)
        changed += 1
      } else {
        sigmaTot.update(currentCommunity, sigmaCurrent)
      }
      node += 1
    }

    changed
  }
}

def coarsenGraph(graph: WeightedGraph, communities: Array[Int]): (WeightedGraph, Array[Int]) = {
  val communityToSuper = mutable.HashMap.empty[Int, Int]
  var nextId = 0
  communities.foreach { community =>
    if (!communityToSuper.contains(community)) {
      communityToSuper.update(community, nextId)
      nextId += 1
    }
  }

  val nodeToSuper = communities.map(communityToSuper)
  val superEdges = mutable.HashMap.empty[(Int, Int), Double]

  var node = 0
  while (node < graph.numNodes) {
    val superU = nodeToSuper(node)
    graph.adj(node).foreach { case (neighbor, weight) =>
      val superV = nodeToSuper(neighbor)
      val key = if (superU <= superV) (superU, superV) else (superV, superU)
      val contribution = if (node == neighbor) weight * 2.0 else weight
      superEdges.update(key, superEdges.getOrElse(key, 0.0) + contribution)
    }
    node += 1
  }

  val remapped = superEdges.iterator.map { case ((u, v), weight) => (u, v, weight / 2.0) }.toArray
  (weightedGraphFromEdges(nextId, remapped), nodeToSuper)
}

def runLouvain(graph: WeightedGraph, maxPasses: Int, minGain: Double): LouvainResult = {
  if (graph.numNodes == 0) {
    LouvainResult(Array.emptyIntArray, 0.0, 0, 0)
  } else {
    val globalCommunities = Array.tabulate(graph.numNodes)(identity)
    var currentGraph = graph
    var previousModularity = computeModularity(graph, globalCommunities)
    var totalPasses = 0
    val effectiveMaxPasses = if (maxPasses == 0) 100 else maxPasses
    var keepRunning = true
    var outerPass = 0

    while (outerPass < effectiveMaxPasses && keepRunning) {
      val localCommunities = Array.tabulate(currentGraph.numNodes)(identity)
      var changed = 1
      var innerPass = 0
      while (innerPass < 100 && changed > 0) {
        changed = localMovePass(currentGraph, localCommunities)
        innerPass += 1
      }

      val anyMoved = localCommunities.indices.exists(index => localCommunities(index) != index)
      if (!anyMoved) {
        keepRunning = false
      } else {
        val (coarsened, nodeToSuper) = coarsenGraph(currentGraph, localCommunities)
        var index = 0
        while (index < globalCommunities.length) {
          globalCommunities(index) = nodeToSuper(globalCommunities(index))
          index += 1
        }

        val newModularity = computeModularity(graph, globalCommunities)
        val gain = newModularity - previousModularity
        totalPasses += 1
        if (gain < minGain) {
          keepRunning = false
        } else {
          previousModularity = newModularity
          currentGraph = coarsened
          if (currentGraph.numNodes <= 1) {
            keepRunning = false
          }
        }
      }
      outerPass += 1
    }

    val remap = mutable.HashMap.empty[Int, Int]
    var nextId = 0
    var index = 0
    while (index < globalCommunities.length) {
      val community = globalCommunities(index)
      val normalized = remap.getOrElseUpdate(community, {
        val value = nextId
        nextId += 1
        value
      })
      globalCommunities(index) = normalized
      index += 1
    }

    LouvainResult(globalCommunities, computeModularity(graph, globalCommunities), totalPasses, nextId)
  }
}

val totalStartNs = System.nanoTime()
val loadStartNs = System.nanoTime()

val canonicalEdges = sc.textFile(inputPath, partitions)
  .flatMap(parseWeightedEdge)
  .map { case (src, dst, weight) =>
    val key = if (src <= dst) (src, dst) else (dst, src)
    (key, weight)
  }
  .reduceByKey((left, _) => left, partitions)
  .map { case ((src, dst), weight) => (src, dst, weight) }
  .collect()

val loadTimeMs = (System.nanoTime() - loadStartNs) / 1000000.0
val graph = weightedGraphFromEdges(numNodes, canonicalEdges)

val computeStartNs = System.nanoTime()
val result = runLouvain(graph, maxPasses, minGain)
val computeOnlyMs = (System.nanoTime() - computeStartNs) / 1000000.0

val payloadBase = Map(
  "load_time_ms" -> loadTimeMs,
  "compute_only_ms" -> computeOnlyMs,
  "modularity" -> result.modularity,
  "num_communities" -> result.numCommunities,
  "num_passes" -> result.numPasses
)

val persistStartNs = System.nanoTime()
if (persistOutput) {
  val rows = result.communities.indices.iterator.map(index => s"$index\t${result.communities(index)}").toSeq
  sc.parallelize(rows, partitions).saveAsTextFile(outputPath)
}
val outputWriteMs = if (persistOutput) (System.nanoTime() - persistStartNs) / 1000000.0 else 0.0
val totalTimeMs = (System.nanoTime() - totalStartNs) / 1000000.0

val payload = payloadBase ++ Map(
  "output_write_ms" -> outputWriteMs,
  "execution_time_ms" -> computeOnlyMs,
  "end_to_end_ms" -> totalTimeMs,
  "total_time_ms" -> totalTimeMs
)

val json = payload.map { case (key, value) =>
  "\"" + jsonEscape(key) + "\":" + value.toString
}.mkString("{", ",", "}")

if (persistOutput) {
  sc.parallelize(Seq(json), 1).saveAsTextFile(metadataOutputPath)
}

println(s"${resultPrefix}${json}")
System.exit(0)
