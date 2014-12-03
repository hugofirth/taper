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

import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.util.ElementHelper
import com.tinkerpop.blueprints.{Direction, Graph}
import org.gdget.util.Identifier

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Defines the contract for a partition strategy object
  *
  * @author hugofirth
  */
trait PartitionStrategy {
  def execute(graph: Graph,
              numPartitions: Int,
              parent: PartitionedGraph): (Map[Int, Partition], mutable.MultiMap[Identifier, Int], Long)
  def createEmptySubGraph(graph: Graph): Graph = graph match {
    case graph: TinkerGraph => new TinkerGraph()
    case graph: Neo4j2Graph => ???
    case _ => ??? //Throw unsupported implementation exception or something
  }

}

/** The PartitionStrategy object for creating a simple, "Hash-based", graph partition.
  *
  * @author hugofirth
  */
object HashPartition extends PartitionStrategy {

  /** Simplistic early implementation of Hash partitioning algorithm. Relies upon all sub graphs fitting into memory.
    *
    * @todo Adapt the partition to periodically persist the partitions to free up memory.
    * @todo Generalise graph implementation choice - perhaps with dependency injection or a factory?
    * @todo Make and return partitions from here, keep counter of elements created and pass that onto PartitionedGraph
    *
    * @param graph The parent graph to be sub divided.
    * @param numPartitions The number of sub graphs to create
    * @return The set of created sub graphs along with associated vertex/edge  global -> local Id maps
    */
  override def execute(graph: Graph,
                       numPartitions: Int,
                       parent: PartitionedGraph): (Map[Int, Partition], mutable.MultiMap[Identifier, Int], Long) = {

    val subGraphs = for(i <- 0 until numPartitions ) yield createEmptySubGraph(graph)
    val vertexIdMaps = for(i <- 0 until numPartitions) yield mutable.Map[Identifier, Identifier]()
    val edgeIdMaps = for(i <- 0 until numPartitions) yield mutable.Map[Identifier, Identifier]()

    val vertices = graph.getVertices.asScala
    val partitionKeys = Iterator.continually((0 until numPartitions).toIterator).flatten
    val edgesSeen = new mutable.HashMap[Identifier, mutable.Set[Int]] with mutable.MultiMap[Identifier, Int]
    var maxId: Identifier = 0L

    vertices.foreach { v =>
      val key = partitionKeys.next()
      val newVertex = subGraphs(key).addVertex(null)

      maxId = if(maxId < v.getId) v.getId else maxId
      vertexIdMaps(key)(v.getId) = newVertex.getId
      ElementHelper.copyProperties(v, newVertex)
      newVertex.setProperty("__globalId", v.getId)
      v.getEdges(Direction.BOTH).asScala.foreach { e =>
        if(!edgesSeen.entryExists(e.getId, _ == key)) {
          //If Edge doesn't already exist locally then do
          val newEdge = subGraphs(key).addEdge(null, e.getVertex(Direction.OUT), e.getVertex(Direction.IN), e.getLabel)
          maxId = if(maxId < e.getId) e.getId else maxId
          edgeIdMaps(key)(e.getId) = newEdge.getId
          ElementHelper.copyProperties(e, newEdge)
          edgesSeen.addBinding(e.getId, key)
        }
      }
    }

    val partitions = for(i <- 0 until numPartitions) yield {
      (i, Partition(subGraphs(i), parent, vertexIdMaps(i), edgeIdMaps(i), i))
    }
    (partitions.toMap, edgesSeen, maxId)
  }
}

/** The PartitionStrategy object for creating a graph partition which minimises ''edge-cut'' using spectral graph
  * methods (which rely upon Algebraic graph theory and the Feidler vector).
  *
  * @author hugofirth
  */
object SpectralPartition extends PartitionStrategy {
  override def execute(graph: Graph,
                       numPartitions: Int,
                       parent: PartitionedGraph): (Map[Int, Partition], mutable.MultiMap[Identifier, Int], Long) = ???
}
