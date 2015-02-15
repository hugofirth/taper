/** contextual-stability
  *
  * Copyright (c) 2014 Hugo Firth
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
package org.gdget.experimental.graph.partition

import java.io.BufferedInputStream
import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph
import com.tinkerpop.blueprints.util.io.graphml.{GraphMigrator, GraphMLReader}
import com.tinkerpop.blueprints.{Edge, Direction, Vertex}
import com.tinkerpop.blueprints.impls.tg.{TinkerGraph, TinkerGraphFactory}
import com.tinkerpop.gremlin.scala._
import org.gdget.UnitSpec
import org.gdget.experimental.graph.{TraversalPatternSummary, DatabaseSpec}
import org.gdget.util.{FileUtils, Counting}
import org.junit.runner.RunWith
import org.scalatest.{Suites, DoNotDiscover}
import org.scalatest.junit.JUnitRunner

class PartitionedGraphTestSuite extends Suites(new PartitionedNeoGraphSpec, new PartitionedTinkerGraphSpec) {}

/** Description of Class
  *
  * @author hugofirth
  */
@DoNotDiscover
class PartitionedTinkerGraphSpec extends UnitSpec with DatabaseSpec {

  val originalGraph = TinkerGraphFactory.createTinkerGraph()
  var partitionedGraph: PartitionedGraph with Counting = null

  val originalGdGraph = new TinkerGraph()
  val input = new BufferedInputStream( this.getClass.getResourceAsStream("/graph-example-2.xml") )
  GraphMLReader.inputGraph(originalGdGraph, input)
  var partitionedGdGraph: PartitionedGraph with Counting = null
  addDatabase(originalGdGraph, originalGraph)

  before {
    partitionedGraph = PartitionedGraph(originalGraph, HashPartitionStrategy("output.directory" -> "ptgs_tinker"), 2,
      TraversalPatternSummary(Map.empty[String, Int], 4))
    partitionedGdGraph = PartitionedGraph(originalGdGraph, HashPartitionStrategy("output.directory" -> "ptgs_grateful"), 3,
      TraversalPatternSummary(Map.empty[String, Int], 4))
  }

  after {
    partitionedGdGraph.shutdown()
    partitionedGraph.shutdown()
    FileUtils.removeAll("target/ptgs_tinker")
    FileUtils.removeAll("target/ptgs_grateful")
  }

  "A PartitionedTinkerGraph" should "contain a number of disjoint subgraphs equal to numPartitions " in {
    val partitions = partitionedGraph.getPartitions
    partitions.hasDefiniteSize should be (true)
    partitions.size should be (2)
  }

  it should "have an initial Element id greater than any in the original Graph " in {
    val nextId = partitionedGraph.getNextElementId
    nextId should be > 12L
  }

  it should "be able to return a vertex by global (original) id " in {
    val vertex = partitionedGraph.getVertex(1L)
    vertex.getProperty[String]("name") should be ("marko")
    vertex.getProperty[Int]("age") should be (29)
  }

  //Should be able to return a collection of vertices by property key and value

  it should "be able to return an edge by global (original) id " in {
    val edge = partitionedGraph.getEdge(7L)
    edge.getLabel should be ("knows")
    edge.getProperty[Float]("weight") should equal (0.5F)
  }

  //Should be abe to return a collection of edges by property key and value

  it should "be able to remove a vertex " in {
    val vertex = partitionedGraph.getVertex(1L)
    vertex should not be (null)
    partitionedGraph.removeVertex(vertex)
    partitionedGraph.getVertex(1L) should be (null)
  }

  it should "be able to remove an edge " in {
    val edge = partitionedGraph.getEdge(10L)
    edge should not be (null)
    partitionedGraph.removeEdge(edge)
    partitionedGraph.getEdge(10L) should be (null)
  }

  it should "be able to add a vertex " in {
    val l = partitionedGraph.addVertex(null)
    val r = partitionedGraph.getVertex(l.getId)
    l should equal (r)
  }

  it should "be able to add an edge " in {
    val vertex1 = partitionedGraph.getVertex(1L)
    val vertex2 = partitionedGraph.getVertex(2L)
    val l = partitionedGraph.addEdge(null, vertex1, vertex2, "test")
    val r = partitionedGraph.getEdge(l.getId)
    l should equal (r)
  }

  it should "be able to correctly answer a simple Gremlin query" in {
    val result = partitionedGraph.v(1L).out("knows").map { _("name") }.toSet
    val traversals = partitionedGraph.getCount

    result should equal (Set("vadas", "josh"))
  }

  it should "be able to correctly answer a complex Gremlin query" in {
    val result = partitionedGraph.V.as("person")
      .in("knows")
      .out("knows")
      .out("created")
      .has("lang", "java")
      .simplePath
      .dedup
      .back("person").property[String]("name").toSet()
    val traversals = partitionedGraph.getCount
    result.size should be (1)
    result should equal (Set("vadas"))
  }

  it should "be able to partition a larger, more complex graph" in {
    val partitions = partitionedGdGraph.getPartitions
    partitions.hasDefiniteSize should be (true)
    partitions.size should be (3)
  }

  it should "be able to correctly answer a simple Gremlin query over a complex graph" in {
    val result = partitionedGdGraph.v(1L).startPipe.out("written_by").map { _("name") }.toSet()
    val traversals = partitionedGdGraph.count
    result should equal (Set("Bo_Diddley"))
  }

  it should "be able to correctly answer a complex Gremlin query over a complex graph" in {
    val expected = originalGdGraph.V.has("name", "Bo_Diddley").in("written_by").outE("followed_by").dedup.order({
      (left: Edge, right: Edge) =>
        val weight = right.getProperty[Int]("weight").compareTo(left.getProperty[Int]("weight"))
        if(weight == 0) {
          right.getVertex(Direction.IN).getProperty[String]("name").compareTo(left.getVertex(Direction.IN).getProperty[String]("name"))
        } else weight
    }).range(0,5).inV.out("written_by").property[String]("name").toList()
    val result = partitionedGdGraph.V.has("name", "Bo_Diddley").in("written_by").outE("followed_by").dedup.order({
      (left: Edge, right: Edge) =>
        val weight = right.getProperty[Int]("weight").compareTo(left.getProperty[Int]("weight"))
        if(weight == 0) {
          right.getVertex(Direction.IN).getProperty[String]("name").compareTo(left.getVertex(Direction.IN).getProperty[String]("name"))
        } else weight
    }).range(0,5).inV.out("written_by").property[String]("name").toList()
    val traversals = partitionedGdGraph.count
    result should equal (expected)
  }

//  it should "work for me please" in {
//    val frequencies = Map("Q1" -> 4, "Q2" -> 4)
//    val summary = new TraversalPatternSummary(frequencies, 4)
//    summary.trie addBinding(Seq("song", "artist"), "Q1")
//    summary.trie addBinding(Seq("artist", "song", "song", "artist"), "Q2")
//    val strategy = HashPartitionStrategy("vertex.labelKey" -> "type", "output.directory" -> "gratefulNew")
//    partitionedGdGraph = PartitionedGraph(originalGdGraph, strategy, 3, summary)
//    val partition = partitionedGdGraph.getPartitionById(0)
//    val test = partition.map(_.getPotentialOutcastVertices)
//    val longPaths = partition.get.visitorMatrix.withPrefix(Seq.empty[PartitionVertex]).filter { case (path, probability) => path.size>2 }
//    val internalArtists = partition.get.getVertices.filter { v =>
//      (v.getProperty[String]("__label") == "artist") && (v.getProperty[String]("__external") == null)
//    }
//    assert(true)
//  }


}

@DoNotDiscover
class PartitionedNeoGraphSpec extends UnitSpec with DatabaseSpec {

  val originalGraph = TinkerGraphFactory.createTinkerGraph()
  val originalGdGraph = new TinkerGraph()
  val input = new BufferedInputStream( this.getClass.getResourceAsStream("/graph-example-2.xml") )
  GraphMLReader.inputGraph(originalGdGraph, input)

  val originalNeoGraph = new Neo4j2Graph("target/neo/pngs_tinker")
  GraphMigrator.migrateGraph(originalGraph, originalNeoGraph)
  val originalNeoGdGraph = new Neo4j2Graph("target/neo/pngs_grateful")
  GraphMigrator.migrateGraph(originalGdGraph, originalNeoGdGraph)

  var partitionedGraph: PartitionedGraph with Counting =
    PartitionedGraph(originalNeoGraph, HashPartitionStrategy("output.directory" -> "pngs_tinker"), 2, TraversalPatternSummary(Map.empty[String, Int], 4))
  var partitionedGdGraph: PartitionedGraph with Counting =
    PartitionedGraph(originalNeoGdGraph, HashPartitionStrategy("output.directory" -> "pngs_grateful"), 3, TraversalPatternSummary(Map.empty[String, Int], 4))

  addDatabase(originalGraph, originalGdGraph, originalNeoGraph, originalNeoGdGraph, partitionedGraph, partitionedGdGraph)
  shouldClear("target/neo", "target/pngs_tinker", "target/pngs_grateful")

  "A PartitionedNeoGraph" should "contain a number of disjoint subgraphs equal to numPartitions " in {
    val partitions = partitionedGraph.getPartitions
    partitions.hasDefiniteSize should be (true)
    partitions.size should be (2)
  }

  it should "have an initial Element id greater than any in the original Graph " in {
    val nextId = partitionedGraph.getNextElementId
    nextId should be >= 6L
  }


  it should "be able to correctly answer a simple Gremlin query" in {
    val result = partitionedGraph.V.has("name", "marko").out("knows").map( _("name") ).toSet()
    val traversals = partitionedGraph.getCount

    result should equal (Set("vadas", "josh"))
  }

  it should "be able to correctly answer a complex Gremlin query" in {
    val result = partitionedGraph.V.as("person")
      .in("knows")
      .out("knows")
      .out("created")
      .has("lang", "java")
      .simplePath
      .dedup
      .back("person").property[String]("name").toSet()
    val traversals = partitionedGraph.getCount
    result.size should be (1)
    result should equal (Set("vadas"))
  }

  it should "be able to partition a larger, more complex graph" in {
    val partitions = partitionedGdGraph.getPartitions
    partitions.hasDefiniteSize should be (true)
    partitions.size should be (3)
  }

  it should "be able to correctly answer a simple Gremlin query over a complex graph" in {
    val result = partitionedGdGraph.V.has("name", "HEY BO DIDDLEY").out("written_by").map( _("name") ).toSet()
    val traversals = partitionedGdGraph.count()
    result should equal (Set("Bo_Diddley"))
  }

  it should "be able to correctly answer a complex Gremlin query over a complex graph" in {
    val expected = originalGdGraph.V.has("name", "Bo_Diddley").in("written_by").outE("followed_by").dedup.order({
      (left: Edge, right: Edge) =>
        val weight = right.getProperty[Int]("weight").compareTo(left.getProperty[Int]("weight"))
        if(weight == 0) {
          right.getVertex(Direction.IN).getProperty[String]("name").compareTo(left.getVertex(Direction.IN).getProperty[String]("name"))
        } else weight
    }).range(0,5).inV.out("written_by").property[String]("name").toList()
    val result = partitionedGdGraph.V.has("name", "Bo_Diddley").in("written_by").outE("followed_by").dedup.order({
      (left: Edge, right: Edge) =>
        val weight = right.getProperty[Int]("weight").compareTo(left.getProperty[Int]("weight"))
        if(weight == 0) {
          right.getVertex(Direction.IN).getProperty[String]("name").compareTo(left.getVertex(Direction.IN).getProperty[String]("name"))
        } else weight
    }).range(0,5).inV.out("written_by").property[String]("name").toList()
    val traversals = partitionedGdGraph.count()
    result should equal (expected)
  }

  it should "be able to add an edge " in {
    val marko = partitionedGraph.V.has("name", "marko").next()
    val peter = partitionedGraph.V.has("name", "peter").next()
    val l = partitionedGraph.addEdge(null, marko, peter, "knows")
    val r = partitionedGraph.getEdge(l.getId)
    l should equal (r)
  }

  it should "be able to add a vertex " in {
    val l = partitionedGraph.addVertex(null)
    val r = partitionedGraph.getVertex(l.getId)
    l should equal (r)
  }

  it should "be able to remove an edge " in {
    val edge = partitionedGraph.V.has("name", "vadas").bothE("knows").next()
    edge should not be (null)
    partitionedGraph.removeEdge(edge)
    partitionedGraph.getEdge(edge.getId) should be (null)
  }

  it should "be able to remove a vertex " in {
    val vertex = partitionedGraph.V.has("name", "marko").next()
    vertex should not be (null)
    partitionedGraph.removeVertex(vertex)
    partitionedGraph.getVertex(vertex.getId) should be (null)
  }

}




