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

import java.io.BufferedInputStream

import com.tinkerpop.blueprints.{Graph => BlueprintsGraph, Direction, Edge}
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.gremlin.scala._
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader
import org.gdget.experimental.graph.TraversalPatternSummary
import org.gdget.experimental.graph.big.BigGraph
import org.gdget.experimental.graph.partition.{METISPartitionStrategy, PartitionedGraph, HashPartitionStrategy}
import org.gdget.util.{FileUtils, Countable}

/** Description of Class
  *
  * @author hugofirth
  */
object GratefulDeadExperiment extends Experiment {

  val originalGdGraph = new TinkerGraph()
  val input = new BufferedInputStream( this.getClass.getResourceAsStream("/graph-example-2.xml") )
  GraphMLReader.inputGraph(originalGdGraph, input)
  val frequencies = Map("Q1" -> 10, "Q2" -> 10)
  val summary = new TraversalPatternSummary(frequencies, 4)
  summary.trie addBinding(Seq("song", "artist"), "Q1")
  summary.trie addBinding(Seq("artist", "song", "song", "artist"), "Q2")

  def q1(start: Long)(implicit graph: BlueprintsGraph) = graph.v(start).startPipe.out("written_by").map { _("name") }.toSet()

  def q2(implicit graph: BlueprintsGraph) = graph.V.has("type", "artist").in("written_by").outE("followed_by").dedup.order({
    (left: Edge, right: Edge) =>
      val weight = right.getProperty[Int]("weight").compareTo(left.getProperty[Int]("weight"))
      if(weight == 0) {
        right.getVertex(Direction.IN).getProperty[String]("name").compareTo(left.getVertex(Direction.IN).getProperty[String]("name"))
      } else weight
  }).range(0,5).inV.out("written_by").property[String]("name").toList()

  val q2Result = q2(originalGdGraph)

  val introversions = Vector(0.05F, 0.1F, 0.15F, 0.2F, 0.25F, 0.3F, 0.35F, 0.4F, 0.45F)

  override def run(output: String): Unit = {
    runTAPR()
    runMETIS()
  }

  private def runTAPR(): Unit = {
    val strategy = HashPartitionStrategy(
      { location: String => BigGraph(validKeys = Set("__globalId", "__label", "__external")) },
      Map("vertex.labelKey" -> "type", "output.directory" -> "gratefulRefined")
    )
    implicit val partitionedGdGraph: PartitionedGraph with Countable = PartitionedGraph(originalGdGraph, strategy, 3, summary)
    var base = partitionedGdGraph.getCount
    //for(j <- 0 until 10) { q1(j) }
    val refinedQ1Count = partitionedGdGraph.getCount-base
    println("Graph refined 0 times. Q1 traversals given 10 executions :" + refinedQ1Count)

    base = partitionedGdGraph.getCount

    //for(k <- 0 until 10) { q2 }
    val refinedQ2Count = partitionedGdGraph.getCount-base
    println("Graph refined 0 times. Q2 traversals given 10 executions :" + refinedQ2Count)

    for(i <- 0 to 8) {
      partitionedGdGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }

      partitionedGdGraph.getPartitions.foreach(_.refine(introversions(i)))

      base = partitionedGdGraph.getCount

      for(j <- 0 until 10) { q1(j) }
      val refinedQ1Count = partitionedGdGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q1 traversals given 10 executions :" + refinedQ1Count)

      base = partitionedGdGraph.getCount

      for(k <- 0 until 10) { q2 }
      val refinedQ2Count = partitionedGdGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q2 traversals given 10 executions :" + refinedQ2Count)
    }

    val q2ResultRefined = q2

    assert(q2Result == q2ResultRefined)
    originalGdGraph.shutdown()
    partitionedGdGraph.shutdown()
    FileUtils.removeAll("target/gratefulRefined")
  }

  private def runMETIS(): Unit = {
    val plan = this.getClass.getResourceAsStream("/plan-example.metis.part.3")
    val strategy = METISPartitionStrategy(
      { location: String => BigGraph(validKeys = Set("__globalId", "__label", "__external"))},
      Map("vertex.labelKey" -> "type", "output.directory" -> "gratefulMetis"), plan
    )
    implicit val partitionedGdGraph: PartitionedGraph with Countable = PartitionedGraph(originalGdGraph, strategy, 3, summary)
    var base = partitionedGdGraph.getCount
    for(j <- 0 until 10) { q1(j) }
    val refinedQ1Count = partitionedGdGraph.getCount-base
    println("Graph partitioned with METIS. Q1 traversals given 10 executions :" + refinedQ1Count)

    base = partitionedGdGraph.getCount

    for(k <- 0 until 10) { q2 }
    val refinedQ2Count = partitionedGdGraph.getCount-base
    println("Graph partitioned with METIS. Q2 traversals given 10 executions :" + refinedQ2Count)

    for(i <- 0 to 8) {
      partitionedGdGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }

      partitionedGdGraph.getPartitions.foreach(_.refine(introversions(i)))

      base = partitionedGdGraph.getCount

      for(j <- 0 until 10) { q1(j) }
      val refinedQ1Count = partitionedGdGraph.getCount-base
      println("METIS Graph refined "+(i+1)+" times. Q1 traversals given 10 executions :" + refinedQ1Count)

      base = partitionedGdGraph.getCount

      for(k <- 0 until 10) { q2 }
      val refinedQ2Count = partitionedGdGraph.getCount-base
      println("METIS Graph refined "+(i+1)+" times. Q2 traversals given 10 executions :" + refinedQ2Count)
    }

    val q2ResultRefined = q2

    assert(q2Result == q2ResultRefined)
    originalGdGraph.shutdown()
    partitionedGdGraph.shutdown()
    FileUtils.removeAll("target/gratefulMetis")
  }
}
