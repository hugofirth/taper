/**
  * contextual-stability
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

import com.tinkerpop.blueprints.util.{ExceptionFactory, ElementHelper}
import com.tinkerpop.blueprints.{Edge, Graph, Vertex}

import scala.collection.mutable
import scala.collection.JavaConverters._

object Partition {
  def apply(subGraph: Graph,
            parent: => PartitionedGraph,
            vertexIdMap: mutable.Map[Long, Long],
            edgeIdMap: mutable.Map[Long, Long],
            id: Int): Partition = {
    new Partition(subGraph, parent, vertexIdMap, edgeIdMap, id)
  }
}

/** Description of Class
  *
  * @author hugofirth
  */
class Partition private (private[this] val subGraph: Graph,
                         parentGraph: => PartitionedGraph,
                         private[partition] val vertexIdMap: mutable.Map[Long, Long],
                         private[partition] val edgeIdMap: mutable.Map[Long, Long],
                         val id: Int) {

  lazy private[partition] val parent = parentGraph

  def getPotentialDestPartitions(v: Vertex): List[Partition] = ???

  def getPartialInteractionOrder: List[Vertex] = ???

  def shouldAccept(potential: Vertex, previousScore: Int): Boolean = ???

  def shutdown(): Unit = this.subGraph.shutdown()

  /** Takes a global vertex id - looks it up locally, and then retrieves and returns the associated vertex
    *
    * @todo prevent local id from leaking in any way.
    *
    * @param id the Long id associated with the desired vertex
    * @return the vertex specified by Long id
    */
  def getVertex(id: Long): Vertex = {
    val vertex = this.subGraph.getVertex(this.vertexIdMap.get(id))
    if(vertex != null) PartitionVertex(vertex, id) else null
  }

  /**
    * @todo Transform return into an Iterable of PartitionVertex
    *
    * @return
    */
  def getVertices: Iterable[Vertex] = this.subGraph.getVertices.asScala

  def removeVertex(vertex: Vertex): Unit = {
    val idAsLong = vertex.getId.asInstanceOf[Long]
    if(this.vertexIdMap.get(idAsLong).isDefined) throw ExceptionFactory.vertexWithIdDoesNotExist(idAsLong)
    this.vertexIdMap.remove(idAsLong)
    this.subGraph.removeVertex(vertex)
  }

  /**
    * @todo Transform return into a PartitionVertex
    *
    * @param id
    * @return
    */
  def addVertex(id: Long): Vertex = this.subGraph.addVertex(null)

  /**
   * @todo Transform return into a PartitionVertex
   *
   * @param vertex
   * @return
   */
  def addVertex(vertex: Vertex): Vertex = {
    val idAsLong = vertex.getId.asInstanceOf[Long]
    if(vertexIdMap.get(idAsLong).isDefined) throw ExceptionFactory.vertexWithIdAlreadyExists()
    val newVertex = this.subGraph.addVertex(null)
    ElementHelper.copyProperties(vertex, newVertex)
    vertexIdMap.put(idAsLong, newVertex.getId.asInstanceOf[Long])
    PartitionVertex(newVertex, idAsLong)
  }

  /**
   * @todo Transform return into a PartitionEdge
   *
   * @param id
   * @return
   */
  def getEdge(id: Long) = subGraph.getEdge(id)

  /**
   * @todo Transform return into an Iterable of PartitionEdge
   *
   * @return
   */
  def getEdges: Iterable[Edge] = this.subGraph.getEdges.asScala

  def removeEdge(edge: Edge): Unit = this.subGraph.removeEdge(edge)

  /** @todo Transform return into a PartitionEdge */
  def addEdge(id: Long, out: Vertex, in: Vertex, label: String, external: Option[(Vertex, Int)] = None): Edge = {
    external match {
      case Some((v,i)) =>
        val newVertex = this.subGraph.addVertex(null)
        val outVertex = if(v == out) {
          ElementHelper.copyProperties(out, newVertex)
          newVertex.setProperty("__external", i)
          newVertex
        } else out
        val inVertex = if(v == in) {
          ElementHelper.copyProperties(in, newVertex)
          newVertex.setProperty("__external", i)
          newVertex
        } else in
        this.subGraph.addEdge(id, outVertex, inVertex, label)
      case None =>
        this.subGraph.addEdge(id, out, in, label)
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Partition]

  override def equals(other: Any): Boolean = other match {
    case that: Partition =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
