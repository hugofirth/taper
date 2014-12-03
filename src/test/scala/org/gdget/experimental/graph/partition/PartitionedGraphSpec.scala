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

import com.tinkerpop.blueprints.impls.tg.TinkerGraphFactory
import org.gdget.UnitSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Description of Class
  *
  * @author hugofirth
  */
@RunWith(classOf[JUnitRunner])
class PartitionedGraphSpec extends UnitSpec {

  val originalGraph = TinkerGraphFactory.createTinkerGraph()
  val numPartitions = 2
  var partitionedGraph: PartitionedGraph = null

  before {
    partitionedGraph = PartitionedGraph(originalGraph, HashPartition, numPartitions)
  }

  "A PartitionedGraph" should "contain a number of disjoint subgraphs equal to numPartitions " in {
    val partitions = partitionedGraph.getPartitions
    partitions.hasDefiniteSize should be (true)
    partitions.size should be (2)
  }

  it should "have an initial Element id greater than any in the original Graph " in {
    val nextId = partitionedGraph.getNextElementId
    nextId should be > 13L
  }

  it should "be able to return a vertex by global (original) id " in {
    val vertex = partitionedGraph.getVertex(1L)
    vertex.getProperty[String]("name") should be ("marko")
    vertex.getProperty[Int]("age") should be (29)
  }

  //Should be able to return a collection of vertices by property key and value

  //Should be able to return an edge by global (original) id

  //Should be abe to return a collection of edges by property key and value

  //Should be able to remove a vertex

  //Should be able to remove an edge

  //Should be able to correctly answer Gremlin queries which cross partitions
}
