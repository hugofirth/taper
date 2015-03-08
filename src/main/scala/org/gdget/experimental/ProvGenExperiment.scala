/** contextual-stability
  *
  * Copyright (c) 2015 Hugo Firth
  * Email: <me@hugofirth.com/>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at:
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.gdget.experimental

import java.io.FileInputStream

import com.tinkerpop.gremlin.scala._
import com.tinkerpop.blueprints.{Direction, Graph, Vertex}
import org.gdget.util.{FileUtils, Countable}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.tinkerpop.blueprints.impls.neo4j2.{Neo4j2Vertex, Neo4j2Graph}
import org.gdget.experimental.graph.TraversalPatternSummary
import org.gdget.experimental.graph.partition.{Partition, METISPartitionStrategy, PartitionedGraph, HashPartitionStrategy}



/** Description of Class
  *
  * Note: asserts don't work if we're limiting query set with range or random
  *
  * TODO: Vary the respective weights of the queries and see if that has an effect. - Done (partial)
  * TODO: Reintroduce climbing introversions (maybe in general we should do this reactively?) and measure effect. - Done (marginally worse)
  * TODO: Maybe introduce a 4th query (or rewrite 3) which emphasises a different pattern or edge. - Done
  * TODO: Try queries with random(0.01) instead of range(0,100). Would be purer.
  *
  * @author hugofirth
  */
object ProvGenExperiment extends Experiment {

  val config = Map(
    "neostore.nodestore.db.mapped_memory" -> "500M",
    "neostore.relationshipstore.db.mapped_memory" -> "25M",
    "neostore.relationshipgroupstore.db.mapped_memory" -> "10M",
    "neostore.propertystore.db.mapped_memory" -> "50M",
    "neostore.propertystore.db.strings.mapped_memory" -> "10M",
    "neostore.propertystore.db.arrays.mapped_memory" -> "10M"
  )
  val frequencies = Map("Q1" -> 10, "Q2" -> 20, "Q3" -> 70)
  val summary = new TraversalPatternSummary(frequencies, 6)
  summary.trie addBinding(Seq("AGENT", "ACTIVITY", "ENTITY"), "Q1")
  summary.trie addBinding(Seq("AGENT", "ACTIVITY", "ENTITY", "ENTITY", "ACTIVITY", "AGENT"), "Q2")
  summary.trie addBinding(Seq("ENTITY", "ACTIVITY", "AGENT"), "Q3")

  //TODO: Unsafe cast is literally the last thing I tried. Need a more elegant way of specifying label retrieval per strategy
  val neoLabelOf: (Vertex) => String = (v: Vertex) => v.asInstanceOf[Neo4j2Vertex].getRawVertex.getLabels.asScala.head.name

  //Does giving a specific id as here cripple my approach?
  def q1(implicit graph: Graph) =
    graph.V.has("__label", "AGENT").random(0.01).in("WASASSOCIATEDWITH").in("WASGENERATEDBY").toList()
  def q2(implicit graph: Graph) = {
    //GET Agents associated with two consecutive versions of a Document
    val agents = mutable.Buffer.empty[Vertex]
    graph.V.has("__label", "AGENT").random(0.01).store(agents).in("WASASSOCIATEDWITH").in("WASGENERATEDBY")
      .both("WASDERIVEDFROM").out("WASGENERATEDBY").out("WASASSOCIATEDWITH").retain(agents).toList()
  }
  //TODO: Make q3 more interesting
  def q3(implicit graph: Graph) =
    graph.V.has("__label", "ENTITY").random(0.01).out("WASGENERATEDBY").out("WASASSOCIATEDWITH").toList()

  val introversions = Vector(0.05F, 0.1F, 0.15F, 0.2F, 0.25F, 0.3F, 0.35F, 0.4F, 0.45F)
  val introversionThreshold = 0.375F

  override def run(output: String): Unit = {

    runTAPR()
    //runMETIS()
  }

  private def runTAPR(): Unit = {
    val originalNeoGraph = new Neo4j2Graph("/Users/hugofirth/Desktop/Data/provgen/graph.db", config.asJava)
    val q2Result = q2(originalNeoGraph)
    val strategy = HashPartitionStrategy(Map("output.directory" -> "provgenRefined"), Some(neoLabelOf))
    implicit val partitionedNeoGraph: PartitionedGraph with Countable = PartitionedGraph(originalNeoGraph, strategy, 8, summary)
    originalNeoGraph.shutdown()

    val v = partitionedNeoGraph.getVertex(21893556)
    println("This is the vertex: " + v + ". It has property keys: "+v.getPropertyKeys+" and id: " + v.getId)
    println("It belongs to partition: "+v.partition.id)
    print("It's neighbours are: ")
    v.partitionVertices(Direction.OUT).foreach { n =>
      println(n + ", with property keys: "+n.getPropertyKeys)
      println("It belongs to partition: "+n.partition.id)
    }

    var base = partitionedNeoGraph.getCount
    for(j <- 0 until 1) { q1 }
    val refinedQ1Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q1 traversals given 1 executions :" + refinedQ1Count)

    base = partitionedNeoGraph.getCount

    for(k <- 0 until 2) { q2 }
    val refinedQ2Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q2 traversals given 2 executions :" + refinedQ2Count)

    base = partitionedNeoGraph.getCount

    for(j <- 0 until 7) { q3 }
    val refinedQ3Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q3 traversals given 7 executions :" + refinedQ3Count)

    for(i <- 0 to 12) {
      partitionedNeoGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }

      partitionedNeoGraph.getPartitions.foreach(_.refine(maxIntro = introversionThreshold, minProb = 0.0005F))

      base = partitionedNeoGraph.getCount

      for(j <- 0 until 1) { q1 }
      val refinedQ1Count = partitionedNeoGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q1 traversals given 1 executions :" + refinedQ1Count)

      base = partitionedNeoGraph.getCount

      for(k <- 0 until 2) { q2 }
      val refinedQ2Count = partitionedNeoGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q2 traversals given 2 executions :" + refinedQ2Count)

      base = partitionedNeoGraph.getCount

      for(j <- 0 until 7) { q3 }
      val refinedQ3Count = partitionedNeoGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q3 traversals given 7 executions :" + refinedQ3Count)

    }

    val q2ResultRefined = q2

    //assert(q2Result == q2ResultRefined)
    partitionedNeoGraph.shutdown()
    FileUtils.removeAll("target/provgenRefined")
  }

  private def runMETIS(): Unit = {
    val originalNeoGraph = new Neo4j2Graph("/Users/hugofirth/Desktop/Data/provgen/graph.db", config.asJava)
    val q2Result = q2(originalNeoGraph)
    val plan = new FileInputStream("/Users/hugofirth/Desktop/Data/provgen/provgen.metis.part.8")
    val strategy = METISPartitionStrategy(Map("output.directory" -> "provgenMetis"), plan, Some(neoLabelOf))
    implicit val partitionedNeoGraph: PartitionedGraph with Countable = PartitionedGraph(originalNeoGraph, strategy, 8, summary)
    originalNeoGraph.shutdown()
    var base = partitionedNeoGraph.getCount
    for(j <- 0 until 1) { q1 }
    val refinedQ1Count = partitionedNeoGraph.getCount-base
    println("Graph partitioned with METIS. Q1 traversals given 1 executions :" + refinedQ1Count)

    base = partitionedNeoGraph.getCount

    for(k <- 0 until 2) { q2 }
    val refinedQ2Count = partitionedNeoGraph.getCount-base
    println("Graph partitioned with METIS. Q2 traversals given 2 executions :" + refinedQ2Count)

    base = partitionedNeoGraph.getCount

    for(j <- 0 until 7) { q3 }
    val refinedQ3Count = partitionedNeoGraph.getCount-base
    println("Graph partitioned with METIS. Q3 traversals given 7 executions :" + refinedQ3Count)

    for(i <- 0 to 12) {
      partitionedNeoGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }

      partitionedNeoGraph.getPartitions.foreach(_.refine(maxIntro = introversionThreshold))

      base = partitionedNeoGraph.getCount

      for(j <- 0 until 1) { q1 }
      val refinedQ1Count = partitionedNeoGraph.getCount-base
      println("METIS Graph refined "+(i+1)+" times. Q1 traversals given 1 executions :" + refinedQ1Count)

      base = partitionedNeoGraph.getCount

      for(k <- 0 until 2) { q2 }
      val refinedQ2Count = partitionedNeoGraph.getCount-base
      println("METIS Graph refined "+(i+1)+" times. Q2 traversals given 2 executions :" + refinedQ2Count)

      base = partitionedNeoGraph.getCount

      for(j <- 0 until 7) { q3 }
      val refinedQ3Count = partitionedNeoGraph.getCount-base
      println("METIS Graph refined "+(i+1)+" times. Q3 traversals given 7 executions :" + refinedQ3Count)
    }

    val q2ResultRefined = q2

    //assert(q2Result == q2ResultRefined)
    partitionedNeoGraph.shutdown()
    FileUtils.removeAll("target/provgenMetis")
  }
}
