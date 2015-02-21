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

import com.tinkerpop.blueprints.{Direction, Edge}
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader
import com.tinkerpop.gremlin.scala._
import org.gdget.experimental.graph.TraversalPatternSummary
import org.gdget.experimental.graph.partition.{Partition, HashPartitionStrategy, PartitionedGraph}
import org.gdget.util.{Identifier, FileUtils, Counting}

/** Description of Class
  *
  * @author hugofirth
  */
object Main extends App{

  gratefulDeadTest()

  def gratefulDeadTest(): Unit = {
    val originalGdGraph = new TinkerGraph()
    val input = new BufferedInputStream( this.getClass.getResourceAsStream("/graph-example-2.xml") )
    GraphMLReader.inputGraph(originalGdGraph, input)
    var partitionedGdGraph: PartitionedGraph with Counting = null
    val frequencies = Map("Q1" -> 10, "Q2" -> 10)
    val summary = new TraversalPatternSummary(frequencies, 4)
    summary.trie addBinding(Seq("song", "artist"), "Q1")
    summary.trie addBinding(Seq("artist", "song", "song", "artist"), "Q2")
    val strategy = HashPartitionStrategy("vertex.labelKey" -> "type", "output.directory" -> "gratefulRefined")
    partitionedGdGraph = PartitionedGraph(originalGdGraph, strategy, 3, summary)

    partitionedGdGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }
    var base = partitionedGdGraph.getCount

    for(i <- 0 until 10) { partitionedGdGraph.v(i).startPipe.out("written_by").map { _("name") }.toSet() }
    val unrefinedQ1Count = partitionedGdGraph.getCount-base
    println("Unrefined Q1 traversals given 10 executions :" + unrefinedQ1Count)

    base = partitionedGdGraph.getCount

    for(i <- 0 until 10) {
      partitionedGdGraph.V.has("type", "artist").in("written_by").outE("followed_by").dedup.order({
        (left: Edge, right: Edge) =>
          val weight = right.getProperty[Int]("weight").compareTo(left.getProperty[Int]("weight"))
          if(weight == 0) {
            right.getVertex(Direction.IN).getProperty[String]("name").compareTo(left.getVertex(Direction.IN).getProperty[String]("name"))
          } else weight
      }).range(0,5).inV.out("written_by").property[String]("name").toList()
    }
    val unrefinedQ2Count = partitionedGdGraph.getCount-base
    println("Unrefined Q2 traversals given 10 executions :" + unrefinedQ2Count)

    //4 erroneously crops up for the first time after 0 has run its swaps to 1. In this small window the only code which
    //ever changes the graph (and it must be changing the graph) is the receive method. Look here.
    partitionedGdGraph.getPartitions.foreach{ p: Partition =>
      val pq = p.getPotentialOutcastVertices(minToBeIn = 0.001F, maxIntroversion = 0.2F)
      pq.dequeueAll.map { case (vertex, introversion, probability) =>
        p.attemptSwap(vertex, probability, p.getPotentialDestPartitions(vertex))
      }
    }

    base = partitionedGdGraph.getCount

    for(i <- 0 until 10) { partitionedGdGraph.v(i).startPipe.out("written_by").map { _("name") }.toSet() }
    val refinedQ1Count = partitionedGdGraph.getCount-base
    println("Once Refined Q1 traversals given 100 executions :" + refinedQ1Count)

    base = partitionedGdGraph.getCount

    println("base is "+base)

    for(i <- 0 until 10) {
      partitionedGdGraph.V.has("type", "artist").in("written_by").outE("followed_by").dedup.order({
        (left: Edge, right: Edge) =>
          val weight = right.getProperty[Int]("weight").compareTo(left.getProperty[Int]("weight"))
          if(weight == 0) {
            right.getVertex(Direction.IN).getProperty[String]("name").compareTo(left.getVertex(Direction.IN).getProperty[String]("name"))
          } else weight
      }).range(0,5).inV.out("written_by").property[String]("name").toList()
    }
    val refinedQ2Count = partitionedGdGraph.getCount-base
    println("Once Refined Q2 traversals given 10 executions :" + refinedQ2Count)
    println("total is "+partitionedGdGraph.getCount)

    partitionedGdGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }

    partitionedGdGraph.getPartitions.foreach{ p: Partition =>
      val pq = p.getPotentialOutcastVertices(minToBeIn = 0.001F, maxIntroversion = 0.2F)
      pq.dequeueAll.map { case (vertex, introversion, probability) =>
        p.attemptSwap(vertex, probability, p.getPotentialDestPartitions(vertex))
      }
    }

    base = partitionedGdGraph.getCount

    for(i <- 0 until 10) { partitionedGdGraph.v(i).startPipe.out("written_by").map { _("name") }.toSet() }
    val refinedQ1Count2 = partitionedGdGraph.getCount-base
    println("Twice Refined Q1 traversals given 100 executions :" + refinedQ1Count2)

    base = partitionedGdGraph.getCount

    println("base is "+base)

    for(i <- 0 until 10) {
      partitionedGdGraph.V.has("type", "artist").in("written_by").outE("followed_by").dedup.order({
        (left: Edge, right: Edge) =>
          val weight = right.getProperty[Int]("weight").compareTo(left.getProperty[Int]("weight"))
          if(weight == 0) {
            right.getVertex(Direction.IN).getProperty[String]("name").compareTo(left.getVertex(Direction.IN).getProperty[String]("name"))
          } else weight
      }).range(0,5).inV.out("written_by").property[String]("name").toList()
    }
    val refinedQ2Count2 = partitionedGdGraph.getCount-base
    println("Twice Refined Q2 traversals given 10 executions :" + refinedQ2Count2)
    println("total is "+partitionedGdGraph.getCount)

    partitionedGdGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }

    originalGdGraph.shutdown()
    partitionedGdGraph.shutdown()
    FileUtils.removeAll("target/gratefulRefined")
  }
}
