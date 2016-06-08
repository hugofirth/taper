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
import com.tinkerpop.blueprints.{Graph => BlueprintsGraph, Vertex}
import com.tinkerpop.blueprints.impls.neo4j2.{Neo4j2Vertex, Neo4j2Graph}
import org.gdget.experimental.graph.TraversalPatternSummary
import org.gdget.experimental.graph.big.BigGraph
import org.gdget.experimental.graph.partition.{METISPartitionStrategy, PartitionedGraph, HashPartitionStrategy}
import org.gdget.util.{SystemUtils, FileUtils, Countable}

import scala.collection.JavaConverters._

/** Description of Class
  *
  * @author hugofirth
  */
object MusicBrainzExperiment extends Experiment {

  val config = Map(
    "neostore.nodestore.db.mapped_memory" -> "1000M",
    "neostore.relationshipstore.db.mapped_memory" -> "2500M",
    "neostore.relationshipgroupstore.db.mapped_memory" -> "10M",
    "neostore.propertystore.db.mapped_memory" -> "4500M",
    "neostore.propertystore.db.strings.mapped_memory" -> "800M",
    "neostore.propertystore.db.arrays.mapped_memory" -> "5M"
  )
  val frequencies = Map("Q1" -> 3, "Q2" -> 4, "Q3" -> 3)
  val summary = new TraversalPatternSummary(frequencies, 5)
  summary.trie addBinding(Seq("Area", "Artist", "Area"), "Q1")
  summary.trie addBinding(Seq("Area", "Artist", "Label", "Area"), "Q1")
  summary.trie addBinding(Seq("Arists", "ArtistCredit", "Track", "ArtistCredit", "Artist"), "Q2")
  summary.trie addBinding(Seq("Arists", "ArtistCredit", "Track", "ArtistCredit", "Artist"), "Q2")
  summary.trie addBinding(Seq("Artist", "ArtistCredit", "Track", "Medium"), "Q3")

  val neoLabelOf: (Vertex) => String =
    (v: Vertex) => v.asInstanceOf[Neo4j2Vertex].getRawVertex.getLabels.asScala.headOption match {
      case Some(label) => label.name
      case None => "Other"
    }

  //Does giving a specific id as here cripple my approach?
  def q1(implicit graph: BlueprintsGraph) = {
    val emmigrate = ->[Vertex].in("STARTED_IN").out("ENDED_IN")
    val foreignLabel = ->[Vertex].in("FROM_AREA").out("RECORDING_CONTRACT").out("FROM_AREA")
    graph.V.has("__label", "Area").or(emmigrate, foreignLabel).toList()
  }

  def q2(implicit graph: BlueprintsGraph) = {
    //If this doesn't work we can use a buffer + aggregate/retain
    graph.V.has("__label", "Artist").random(0.01).out("CREDITED_AS").out("CREDITED_ON").in("CREDITED_ON")
      .in("CREDITED_AS").toList()
  }

  def q3(implicit graph: BlueprintsGraph) = {
    graph.V.has("__label", "Artist").random(0.01).out("CREDITED_AS").out("CREDITED_ON").out("APPEARS_ON").toList()
  }

  val introversions = Vector(0.05F, 0.1F, 0.15F, 0.2F, 0.25F, 0.3F, 0.35F, 0.4F, 0.45F)
  val introversionThreshold = 0.1F

  override def run(output: String): Unit = {
    runTAPR()
//    runMETIS()
  }


  private def runTAPR(): Unit = {

    val originalNeoGraph = new Neo4j2Graph("/Users/hugofirth/Desktop/Data/musicbrainz/graph.db", config.asJava)

    //No comparing to final result when using random() filter on queries as not deterministic
    //val q2Result = q2(originalNeoGraph)

    val strategy = HashPartitionStrategy(
      { location: String => BigGraph(validKeys = Set("__globalId", "__label", "__external")) },
      Map("output.directory" -> "mbRefined"), Some(neoLabelOf)
    )

    implicit val partitionedNeoGraph: PartitionedGraph with Countable = PartitionedGraph(originalNeoGraph, strategy, 8, summary)
    originalNeoGraph.shutdown()


    var base = partitionedNeoGraph.getCount
    for(j <- 0 until 3) { q1 }
    val refinedQ1Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q1 traversals given 3 executions :" + refinedQ1Count)

    base = partitionedNeoGraph.getCount

    for(j <- 0 until 4) { q2 }
    val refinedQ2Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q2 traversals given 4 executions :" + refinedQ2Count)

    base = partitionedNeoGraph.getCount

    for(j <- 0 until 3) { q3 }
    val refinedQ3Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q3 traversals given 3 executions :" + refinedQ3Count)

    partitionedNeoGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }

//    for(i <- 0 until 8) {
//      partitionedNeoGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }
//
//      partitionedNeoGraph.getPartitions.foreach { p => p.refine(maxIntro = introversionThreshold) }
//
//      base = partitionedNeoGraph.getCount
//
//      for(j <- 0 until 3) { q1 }
//      val refinedQ1Count = partitionedNeoGraph.getCount-base
//      logger.debug("Graph refined "+(i+1)+" times. Q1 traversals given 3 executions :" + refinedQ1Count)
//
//      base = partitionedNeoGraph.getCount
//
//      for(j <- 0 until 4) { q2 }
//      val refinedQ2Count = partitionedNeoGraph.getCount-base
//      logger.debug("Graph refined "+(i+1)+" times. Q2 traversals given 4 executions :" + refinedQ2Count)
//
//      base = partitionedNeoGraph.getCount
//
//      for(j <- 0 until 3) { q3 }
//      val refinedQ3Count = partitionedNeoGraph.getCount-base
//      logger.debug("Graph refined "+(i+1)+" times. Q3 traversals given 3 executions :" + refinedQ3Count)
//
//      partitionedNeoGraph.getPartitions.foreach { p => println(s"Partitions comm count ${p.getCount}") }
//    }

    //No comparing to final result when using random() filter on queries as not deterministic
    //val q2ResultRefined = q2
    //assert(q2Result == q2ResultRefined)

    partitionedNeoGraph.shutdown()
//    FileUtils.removeAll("target/mbRefined")
  }

  private def runMETIS(): Unit = {

    val originalNeoGraph = new Neo4j2Graph("/Users/hugofirth/Desktop/Data/musicbrainz/graph.db", config.asJava)

    //No comparing to final result when using random() filter on queries as not deterministic
    //val q2Result = q2(originalNeoGraph)
    println("Finished loading the graph." + sys.props("line.separator") + SystemUtils.heapStats)
    val plan = new FileInputStream("/Users/hugofirth/Desktop/Data/musicbrainz/musicbrainz.metis.part.8")

    val strategy = METISPartitionStrategy(
      { location: String => BigGraph(validKeys = Set("__globalId", "__label", "__external")) },
      Map("output.directory" -> "mbMetis"), plan, Some(neoLabelOf))

    implicit val partitionedNeoGraph: PartitionedGraph with Countable = PartitionedGraph(originalNeoGraph, strategy, 8, summary)
    originalNeoGraph.shutdown()

    println("Finished partitioning the graph." + sys.props("line.separator") + SystemUtils.heapStats)

    var base = partitionedNeoGraph.getCount
    for(j <- 0 until 3) { q1 }
    val refinedQ1Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q1 traversals given 3 executions :" + refinedQ1Count)

    base = partitionedNeoGraph.getCount

    for(j <- 0 until 4) { q2 }
    val refinedQ2Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q2 traversals given 4 executions :" + refinedQ2Count)

    base = partitionedNeoGraph.getCount

    for(j <- 0 until 3) { q3 }
    val refinedQ3Count = partitionedNeoGraph.getCount-base
    println("Graph refined 0 times. Q3 traversals given 3 executions :" + refinedQ3Count)

    for(i <- 0 to 8) {
      partitionedNeoGraph.getPartitions.foreach { p => println("Partition "+p.id+" has size "+p.getVertices.size) }

      partitionedNeoGraph.getPartitions.foreach(_.refine(maxIntro = introversionThreshold))

      base = partitionedNeoGraph.getCount

      for(j <- 0 until 3) { q1 }
      val refinedQ1Count = partitionedNeoGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q1 traversals given 3 executions :" + refinedQ1Count)

      base = partitionedNeoGraph.getCount

      for(j <- 0 until 4) { q2 }
      val refinedQ2Count = partitionedNeoGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q2 traversals given 4 executions :" + refinedQ2Count)

      base = partitionedNeoGraph.getCount

      for(j <- 0 until 3) { q3 }
      val refinedQ3Count = partitionedNeoGraph.getCount-base
      println("Graph refined "+(i+1)+" times. Q3 traversals given 3 executions :" + refinedQ3Count)

      partitionedNeoGraph.getPartitions.foreach { p => println(s"Partitions comm count ${p.getCount}") }

    }

    //No comparing to final result when using random() filter on queries as not deterministic
    //val q2ResultRefined = q2
    //assert(q2Result == q2ResultRefined)

    partitionedNeoGraph.shutdown()

    //No files
    // FileUtils.removeAll("target/mbMetis")
  }


}
