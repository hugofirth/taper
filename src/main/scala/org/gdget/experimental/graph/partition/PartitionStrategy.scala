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

import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.util.ElementHelper
import com.tinkerpop.blueprints.{Direction, Graph, Vertex}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Defines the contract for a partition strategy object
  *
  * @author hugofirth
  */
trait PartitionStrategy {
  def execute(graph: Graph, numPartitions: Int): List[(Graph, mutable.Map[Long, Long], mutable.Map[Long, Long])]
}

/** The PartitionStrategy object for creating a simple, "Hash-based", graph partition.
  *
  * @author hugofirth
  */
case object HashPartition extends PartitionStrategy {

  /** Simplistic early implementation of Hash partitioning algorithm. Relies upon all sub graphs fitting into memory.
    *
    * @todo Adapt the partition to periodically persist the partitions to free up memory.
    * @todo Generalise graph implementation choice - perhaps with dependency injection or a factory?
    *
    * @param graph The parent graph to be sub divided.
    * @param numPartitions The number of sub graphs to create
    * @return The set of created sub graphs along with associated vertex/edge  global -> local Id maps
    */
  override def execute(graph: Graph, numPartitions: Int): List[(Graph, mutable.Map[Long, Long], mutable.Map[Long, Long])] = {
    val partitionComponents: List[(Graph, mutable.Map[Long, Long], mutable.Map[Long, Long])] =
      (for (i <- 0 to numPartitions) yield {
        ( new TinkerGraph(), mutable.Map[Long, Long](), mutable.Map[Long, Long]() )
      }).toList
    val vertices: Iterable[Vertex] = graph.getVertices.asScala
    val assignedVertices: Iterable[(Vertex, Int)] = vertices.zip(Stream.continually((0 until numPartitions).toStream).flatten)
    val edgesSeen = new mutable.HashMap[AnyRef, mutable.Set[Int]] with mutable.MultiMap[AnyRef, Int]

    assignedVertices.foreach {
      case (v:Vertex, i:Int) =>
        val newVertex = partitionComponents(i)._1.addVertex(v.getId)
        partitionComponents(i)._2(v.getId.asInstanceOf[Long]) = newVertex.getId.asInstanceOf[Long]
        ElementHelper.copyProperties(v,newVertex)
        v.getEdges(Direction.BOTH).asScala.foreach(e => {
          if(!edgesSeen.entryExists(e.getId, _ == i)) {
            //If Edge doesn't already exist locally then do
            val newEdge = partitionComponents(i)._1.addEdge(e.getId, e.getVertex(Direction.OUT), e.getVertex(Direction.IN), e.getLabel)
            partitionComponents(i)._3(e.getId.asInstanceOf[Long]) = newEdge.getId.asInstanceOf[Long]
            ElementHelper.copyProperties(e, newEdge)
            edgesSeen.addBinding(e.getId, i)
          }
        })
    }
    partitionComponents
  }
}

/** The PartitionStrategy object for creating a graph partition which minimises ''edge-cut'' using spectral graph
  * methods (which rely upon Algebraic graph theory and the Feidler vector).
  *
  * @author hugofirth
  */
case object SpectralPartition extends PartitionStrategy {
  override def execute(graph: Graph, numPartitions: Int): List[(Graph, mutable.Map[Long, Long], mutable.Map[Long, Long])] = ???
}
