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

import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.{Graph => BlueprintsGraph, Vertex}
import com.tinkerpop.blueprints.impls.neo4j2.{Neo4j2Graph, Neo4j2Vertex}
import com.tinkerpop.gremlin.scala._
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle
import org.gdget.experimental.graph.TraversalPatternSummary
import org.gdget.experimental.graph.big.BigGraph
import org.gdget.experimental.graph.partition.{METISPartitionStrategy, PartitionedGraph, HashPartitionStrategy}
import org.gdget.util.{FileUtils, Countable}

import scala.collection.mutable
import scala.collection.JavaConverters._

/** Description of Class
  *
  * @author hugofirth
  */
object ProvGenExperiment2 extends Experiment {

  val config = Map(
    "neostore.nodestore.db.mapped_memory" -> "500M",
    "neostore.relationshipstore.db.mapped_memory" -> "25M",
    "neostore.relationshipgroupstore.db.mapped_memory" -> "10M",
    "neostore.propertystore.db.mapped_memory" -> "50M",
    "neostore.propertystore.db.strings.mapped_memory" -> "10M",
    "neostore.propertystore.db.arrays.mapped_memory" -> "10M"
  )

  def getSummary(frequencies: Map[String, Int]) = {
    val summary = new TraversalPatternSummary(frequencies, 2)
//    summary.trie addBinding(Seq("ENTITY", "ENTITY", "ENTITY", "ENTITY", "ENTITY", "ENTITY"), "Q1")
//    summary.trie addBinding(Seq("AGENT", "ACTIVITY", "ENTITY", "ENTITY", "ACTIVITY", "AGENT"), "Q2")
//    summary.trie addBinding(Seq("ENTITY", "ACTIVITY"), "Q3")
//    summary.trie addBinding(Seq("ENTITY", "ENTITY", "ACTIVITY"), "Q3")
//    summary.trie addBinding(Seq("ENTITY", "ENTITY", "ENTITY", "ACTIVITY"), "Q3")
//    summary.trie addBinding(Seq("ENTITY", "ENTITY", "ENTITY", "ENTITY", "ACTIVITY"), "Q3")
//    summary.trie addBinding(Seq("ENTITY", "ENTITY", "ENTITY", "ENTITY", "ENTITY", "ACTIVITY"), "Q3")
//    summary.trie addBinding(Seq("ENTITY", "ACTIVITY", "AGENT", "AGENT", "AGENT", "AGENT"), "Q4")

//    summary.trie addBinding(Seq("ENTITY", "ENTITY", "ENTITY", "ENTITY", "ENTITY", "ENTITY"), "Q1")
//    summary.trie addBinding(Seq("AGENT", "ACTIVITY", "ENTITY", "ACTIVITY", "AGENT"), "Q2")
    summary.trie addBinding(Seq("AGENT", "ACTIVITY"), "Q1")
//    summary.trie addBinding(Seq("ENTITY", "ACTIVITY", "AGENT", "AGENT", "AGENT", "AGENT"), "Q4")
    summary
  }

  val neoLabelOf: (Vertex) => String = (v: Vertex) => v.asInstanceOf[Neo4j2Vertex].getRawVertex.getLabels.asScala.head.name()

  //Does giving a specific id as here cripple my approach?
  def q1(implicit graph: BlueprintsGraph) =
    graph.V.has("__label", "AGENT").as("e").out("WASASSOCIATEDWITH").toList()
//    graph.V.has("__label", "ENTITY").random(0.01).as("e").out("WASDERIVEDFROM")
//      .loop("e", {lb: LoopBundle[Vertex] => lb.loops < 6}).toList()

  def q2(implicit graph: BlueprintsGraph) = {
    //GET Agents which both edit and use a document
    val agents = mutable.Buffer.empty[Vertex]
    graph.V.has("__label", "AGENT").random(0.01).store(agents).in("WASASSOCIATEDWITH").in("WASGENERATEDBY").in("USED")
      .out("WASASSOCIATEDWITH").retain(agents).toList()
  }

  def q3(implicit graph: BlueprintsGraph) = {
//    val activities = mutable.ArrayBuffer[Vertex]()
//    graph.V.has("__label", "ENTITY").random(0.01).out("WASDERIVEDFROM").as("e").sideEffect({ v =>
//      activities ++= v.out("WASGENERATEDBY").toList()
//    }).loop("e", { lb: LoopBundle[Vertex] => lb.loops < 5 }).toList()
    graph.V.has("__label", "ENTITY").out("WASDERIVEDFROM").toList()
  }

  //TODO: switch to simpler looping construct.
  def q4(implicit graph: BlueprintsGraph) =
    graph.V.has("__label", "ENTITY").random(0.01).out("WASGENERATEDBY").out("WASASSOCIATEDWITH").as("a")
      .out("ACTEDONBEHALFOF").loop("a", {lb: LoopBundle[Vertex] => lb.loops < 4}).toList()

  val introversions = Vector(0.05F, 0.1F, 0.15F, 0.2F, 0.25F, 0.3F, 0.35F, 0.4F, 0.45F)
  val introversionThreshold = 0.45F

  override def run(output: String): Unit = {
//    println("Run 1")
    //runTAPR(getSummary(Map("Q1" -> 25, "Q2" -> 25, "Q3" -> 25, "Q4" -> 25)))
//    println("Run 2")
//    runTAPR(getSummary(Map("Q1" -> 58, "Q2" -> 16, "Q3" -> 1, "Q4" -> 25)))
    runTAPR(getSummary(Map("Q1" -> 100)))
//    runMETIS(getSummary(Map("Q1" -> 1, "Q2" -> 1, "Q3" -> 97, "Q4" -> 1)))
  }

  private def runTAPR(summary: TraversalPatternSummary): Unit = {
    val originalNeoGraph = new Neo4j2Graph("/Users/hugofirth/Desktop/Data/provgen/graph.db", config.asJava)

    //No comparing to final result when using random() filter on queries as not deterministic
    //val q2Result = q2(originalNeoGraph)

    val strategy = HashPartitionStrategy({ location: String => BigGraph(validKeys = Set("__globalId", "__label", "__external")) }, Map("output.directory" -> "provgenRefined"), Some(neoLabelOf))
    implicit val partitionedNeoGraph: PartitionedGraph with Countable = PartitionedGraph(originalNeoGraph, strategy, 8, summary)
    originalNeoGraph.shutdown()

//    val v = partitionedNeoGraph.getVertex(21893556)
//    println("This is the vertex: " + v + ". It has property keys: "+v.getPropertyKeys+" and id: " + v.getId)
//    println("It belongs to partition: "+v.partition.id)
//    print("It's neighbours are: ")
//    v.partitionVertices(Direction.OUT).foreach { n =>
//      println(n + ", with property keys: "+n.getPropertyKeys)
//      println("It belongs to partition: "+n.partition.id)
//    }

    var base = partitionedNeoGraph.getCount
//    for(j <- 0 until 3) { q1 }
//    val refinedQ1Count = partitionedNeoGraph.getCount-base
//    println("Graph refined 0 times. Q1 traversals given 3 executions :" + refinedQ1Count)
//
//    base = partitionedNeoGraph.getCount
//
//    for(j <- 0 until 3) { q2 }
//    val refinedQ2Count = partitionedNeoGraph.getCount-base
//    println("Graph refined 0 times. Q2 traversals given 3 executions :" + refinedQ2Count)
//
//    base = partitionedNeoGraph.getCount

    for(j <- 0 until 5) { q1 }
    val refinedQ1Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q1 traversals given 5 executions :" + refinedQ1Count)

    base = partitionedNeoGraph.getCount

    for(j <- 0 until 5) { q3 }
    val refinedQ3Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q3 traversals given 5 executions :" + refinedQ3Count)

////    for(j <- 0 until 3) { q4 }
////    val refinedQ4Count = partitionedNeoGraph.getCount-base
////    println("Graph refined 0 times. Q4 traversals given 3 executions :" + refinedQ4Count)
//
//    for(i <- 0 until 6) {
//      partitionedNeoGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }
//
//      partitionedNeoGraph.getPartitions.foreach(_.refine(maxIntro = introversionThreshold, minProb = 0.001F))
//
//      base = partitionedNeoGraph.getCount
//
////      for(j <- 0 until 3) { q1 }
////      val refinedQ1Count = partitionedNeoGraph.getCount-base
////      println("Graph refined "+(i+1)+" times. Q1 traversals given 3 executions :" + refinedQ1Count)
////
////      base = partitionedNeoGraph.getCount
////
////      for(j <- 0 until 3) { q2 }
////      val refinedQ2Count = partitionedNeoGraph.getCount-base
////      println("Graph refined "+(i+1)+" times. Q2 traversals given 3 executions :" + refinedQ2Count)
////
////      base = partitionedNeoGraph.getCount
//
//      for(j <- 0 until 5) { q1 }
//      val refinedQ1Count = partitionedNeoGraph.getCount-base
//      println("Graph refined "+(i+1)+" times. Q1 traversals given 5 executions :" + refinedQ1Count)
//
////      for(j <- 0 until 3) { q4 }
////      val refinedQ4Count = partitionedNeoGraph.getCount-base
////      println("Graph refined "+(i+1)+" times. Q4 traversals given 3 executions :" + refinedQ4Count)
//
//      partitionedNeoGraph.getPartitions.foreach { p => println(s"Partitions comm count ${p.getCount}") }
//
//    }
//
//
//    base = partitionedNeoGraph.getCount
//    for(j <- 0 until 4) { q1 }
//    q3
//    println(s"20% delta New number of traversals: ${partitionedNeoGraph.getCount-base}")
//
//    base = partitionedNeoGraph.getCount
//    for(j <- 0 until 3) { q1 }
//    for(j <- 0 until 2) { q3 }
//    println(s"40% delta New number of traversals: ${partitionedNeoGraph.getCount-base}")
//
//    base = partitionedNeoGraph.getCount
//    for(j <- 0 until 2) { q1 }
//    for(j <- 0 until 3) { q3 }
//    println(s"60% delta New number of traversals: ${partitionedNeoGraph.getCount-base}")
//
//    base = partitionedNeoGraph.getCount
//    q1
//    for(j <- 0 until 4) { q3 }
//    println(s"80% delta New number of traversals: ${partitionedNeoGraph.getCount-base}")
//
//    base = partitionedNeoGraph.getCount
//    for(j <- 0 until 5) { q3 }
//    println(s"100% delta New number of traversals: ${partitionedNeoGraph.getCount-base}")
//    //No comparing to final result when using random() filter on queries as not deterministic
//    //val q2ResultRefined = q2
//    //assert(q2Result == q2ResultRefined)



    partitionedNeoGraph.shutdown()
    FileUtils.removeAll("target/provgenRefined")
  }

  private def runMETIS(summary: TraversalPatternSummary): Unit = {
    val originalNeoGraph = new Neo4j2Graph("/Users/hugofirth/Desktop/Data/provgen/graph.db", config.asJava)

    //No comparing to final result when using random() filter on queries as not deterministic
    //val q2Result = q2(originalNeoGraph)

    val plan = new FileInputStream("/Users/hugofirth/Desktop/Data/provgen/provgen.metis.part.8")
    val strategy = METISPartitionStrategy({ location: String => new TinkerGraph(location) }, Map("output.directory" -> "provgenMetis"), plan, Some(neoLabelOf))
    implicit val partitionedNeoGraph: PartitionedGraph with Countable = PartitionedGraph(originalNeoGraph, strategy, 8, summary)
    originalNeoGraph.shutdown()

    var base = partitionedNeoGraph.getCount
    for(j <- 0 until 3) { q1 }
    val refinedQ1Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q1 traversals given 3 executions :" + refinedQ1Count)

    base = partitionedNeoGraph.getCount

    for(j <- 0 until 3) { q2 }
    val refinedQ2Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q2 traversals given 3 executions :" + refinedQ2Count)

    base = partitionedNeoGraph.getCount

    for(j <- 0 until 3) { q3 }
    val refinedQ3Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q3 traversals given 3 executions :" + refinedQ3Count)

    for(j <- 0 until 3) { q4 }
    val refinedQ4Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q4 traversals given 3 executions :" + refinedQ4Count)

    for(i <- 0 to 8) {
      partitionedNeoGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }

      partitionedNeoGraph.getPartitions.foreach(_.refine(maxIntro = introversionThreshold))

      base = partitionedNeoGraph.getCount

      for(j <- 0 until 3) { q1 }
      val refinedQ1Count = partitionedNeoGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q1 traversals given 3 executions :" + refinedQ1Count)

      base = partitionedNeoGraph.getCount

      for(j <- 0 until 3) { q2 }
      val refinedQ2Count = partitionedNeoGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q2 traversals given 3 executions :" + refinedQ2Count)

      base = partitionedNeoGraph.getCount

      for(j <- 0 until 3) { q3 }
      val refinedQ3Count = partitionedNeoGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q3 traversals given 3 executions :" + refinedQ3Count)

      for(j <- 0 until 3) { q4 }
      val refinedQ4Count = partitionedNeoGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q4 traversals given 3 executions :" + refinedQ4Count)
    }

    //No comparing to final result when using random() filter on queries as not deterministic
    //val q2ResultRefined = q2
    //assert(q2Result == q2ResultRefined)
    println("Finished the experiment!")

    partitionedNeoGraph.shutdown()
    FileUtils.removeAll("target/provgenMetis")
  }
}
