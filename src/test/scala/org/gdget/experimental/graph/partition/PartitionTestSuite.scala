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

import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.util.GraphHelper
import org.gdget.UnitSpec
import org.gdget.experimental.graph.{TraversalPatternSummary, DatabaseSpec}
import org.gdget.util.{Identifier, FileUtils}
import org.scalatest.{DoNotDiscover, Suites}

/** Description of Class
  *
  * @author hugofirth
  */
class PartitionTestSuite extends Suites(new PartitionSpec) {}

@DoNotDiscover
class PartitionSpec extends UnitSpec with DatabaseSpec {

  val originalGraph = new TinkerGraph()
  val summary = TraversalPatternSummary(Map("Q1" -> 4, "Q2" -> 4), 3)
  var graph: Graph = _
  var partitionedGraph: PartitionedGraph = _
  var partition: Partition = _

  summary.trie addBinding(Seq("a","b","d"), "Q1")
  summary.trie addBinding(Seq("a","c","d"), "Q1")
  summary.trie addBinding(Seq("a","c","a"), "Q2")
  summary.trie addBinding(Seq("c","c","a"), "Q2")

  val vertices = Map(
    1 -> originalGraph.addVertex(1L),
    2 -> originalGraph.addVertex(2L),
    3 -> originalGraph.addVertex(3L),
    4 -> originalGraph.addVertex(4L),
    5 -> originalGraph.addVertex(5L),
    6 -> originalGraph.addVertex(6L)
  )

  vertices get 1 foreach( _.setProperty("__label", "a") )
  vertices get 2 foreach( _.setProperty("__label", "b") )
  vertices get 3 foreach( _.setProperty("__label", "c") )
  vertices get 4 foreach( _.setProperty("__label", "d") )
  vertices get 5 foreach( _.setProperty("__label", "c") )
  vertices get 6 foreach( _.setProperty("__label", "a") )

  originalGraph.addEdge(1L, vertices(1), vertices(2), "foo")
  originalGraph.addEdge(2L, vertices(1), vertices(4), "bar")
  originalGraph.addEdge(3L, vertices(2), vertices(3), "baz")
  originalGraph.addEdge(4L, vertices(2), vertices(4), "foo")
  originalGraph.addEdge(5L, vertices(2), vertices(5), "bar")
  originalGraph.addEdge(6L, vertices(3), vertices(4), "baz")
  originalGraph.addEdge(7L, vertices(3), vertices(5), "foo")
  originalGraph.addEdge(8L, vertices(3), vertices(6), "bar")
  originalGraph.addEdge(9L, vertices(4), vertices(5), "baz")
  originalGraph.addEdge(10L, vertices(5), vertices(6), "foo")



  before {
    graph = new TinkerGraph()
    GraphHelper.copyGraph(originalGraph, graph)
    partitionedGraph = PartitionedGraph(graph, HashPartitionStrategy({ location: String => new TinkerGraph(location) },
      Map("output.directory" -> "ps_tinker")), 2, summary)
    partition = partitionedGraph.getPartitions.headOption.getOrElse( fail("No partitions found") )
  }

  after {
    partitionedGraph.shutdown()
    FileUtils.removeAll("target/ps_tinker")
  }

  "A Partition" should "be able to return a priority Queue of 'extraverted' vertices to eject" in {
    val pq = partition.getPotentialOutcastVertices(minToBeIn = 0.001F, maxIntro = 0.75F)
    pq should have size 1
    pq.dequeue()._1 should equal (Identifier(1L))
  }

  it should "be able to return variable length priority Queues depending on provided parameters" in {
    val pq = partition.getPotentialOutcastVertices(minToBeIn = 0.001F, maxIntro = 0.5F)
    pq should have size 0
  }

  it should "be able to succesfully attempt a swap of an extraverted vertex with another partition" in {
    val pq = partition.getPotentialOutcastVertices(minToBeIn = 0.001F, maxIntro = 0.75F)
    val (vertexId, introversion, probability) = pq.dequeue()
    val vertex = partition.getVertex(vertexId).getOrElse { fail("Partition should contain potential ejection candidate.") }
    val destinations = partition.getPotentialDestPartitions(vertex)
    partition.getInternalVertices should contain (vertex)
    partition.attemptSwap(vertex, probability, destinations, 0.5F)
    partition.getInternalVertices should not contain (vertex)
  }


}
