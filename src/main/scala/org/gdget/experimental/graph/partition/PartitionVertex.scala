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

import java.lang.{Iterable => JIterable}
import java.util.{Set => JSet}

import com.tinkerpop.blueprints.util.DefaultVertexQuery
import org.gdget.experimental.graph.util.Labellable
import org.gdget.util.{Countable, Identifier}

import scala.collection.JavaConverters._

import com.tinkerpop.blueprints._

sealed trait Internal extends PartitionVertex {}

sealed trait External extends PartitionVertex {

  override def getEdges(direction: Direction, labels: String*): JIterable[Edge] = {
    this.logTraversal()
    this.partition.parent.getVertex(this.getId).getEdges(direction, labels: _*)
  }

  override def query(): VertexQuery = {
    this.logTraversal()
    this.partition.parent.getVertex(this.getId).query()
  }

  override def getVertices(direction: Direction, labels: String*): JIterable[Vertex] = {
    this.logTraversal()
    this.partition.parent.getVertex(this.getId).getVertices(direction, labels: _*)
  }

  private[partition] def getLocalVertices = verticesView(Direction.BOTH)
  
  private[partition] def getLocalEdges = edgesView(Direction.BOTH)

  private def logTraversal() = this.partition.parent match {
    case graph : Countable => graph.count()
    case _ =>
  }
}

object PartitionVertex {
  def apply(toBeWrapped: Vertex, globalId: Identifier, partition: Partition) = toBeWrapped match {
    case v: PartitionVertex => v
    case v if Option(v.getProperty[Int]("__external")).isDefined => new PartitionVertex(v, globalId, partition) with External
    case v => new PartitionVertex(v, globalId, partition) with Internal
  }
  def unapply(v: PartitionVertex): Option[Vertex] = Some(v.wrapped)
  def unwrap(v: Vertex): Vertex = v match {
    case PartitionVertex(vertex) => vertex
    case _ => v
  }
}

/** Description of Class
  *
  *
  * @author hugofirth
  */
class PartitionVertex private (val wrapped: Vertex, val globalId: Identifier, val partition: Partition) extends Vertex with Labellable {

  //Set globalId property to provided value - convert to Long because some Graph vendors limit property types
  wrapped.setProperty("__globalId", globalId.toLong)

  override def getEdges(direction: Direction, labels: String*): JIterable[Edge] = {
    val edges: Iterable[Edge] = edgesView(direction, labels: _*)
    edges.asJava
  }

  private[partition] def getPartitionEdges(direction: Direction, labels: String*): Iterable[PartitionEdge] = 
    edgesView(direction, labels: _*)

  override def addEdge(label: String, other: Vertex): PartitionEdge = {
    val newEdge = wrapped.addEdge(label, other)
    val globalId = partition.getNextId
    newEdge.setProperty("__globalId", globalId)
    PartitionEdge(newEdge, globalId, partition)
  }
  
  protected def edgesView(direction: Direction, labels: String*): Iterable[PartitionEdge] = 
    wrapped.getEdges(direction, labels: _*).asScala.view.map { e => 
      PartitionEdge(e, e.getProperty[Any]("__globalId"), partition) 
    }

  override def query(): VertexQuery = new DefaultVertexQuery(this)

  protected def verticesView(direction: Direction, labels: String*): Iterable[PartitionVertex] =
    wrapped.getVertices(direction, labels: _*).asScala.view.map { v => 
      PartitionVertex(v, v.getProperty[Any]("__globalId"), partition) 
    }

  override def getVertices(direction: Direction, labels: String*): JIterable[Vertex] = {
    val vertices: Iterable[Vertex] = verticesView(direction, labels: _*)
    vertices.asJava
  }

  def getPartitionVertices(direction: Direction, labels: String*): Iterable[PartitionVertex] =
    verticesView(direction, labels: _*)

  private[partition] def intVertices(direction: Direction, labels: String*): Iterable[Internal] = {
    verticesView(direction, labels:_*).collect { case i: Internal => i }
  }

  private[partition] def extVertices(direction: Direction, labels: String*): Iterable[External] = {
    verticesView(direction, labels:_*).collect { case e: External => e }
  }

  override def getProperty[T](propertyKey: String): T = wrapped.getProperty[T](propertyKey)

  override def getId: Identifier = wrapped.getProperty[Any]("__globalId")

  override def setProperty(propertyKey: String, propertyValue: scala.Any): Unit =
    wrapped.setProperty(propertyKey, propertyValue)

  override def getPropertyKeys: JSet[String] = wrapped.getPropertyKeys


  override def remove(): Unit = wrapped.remove()

  override def removeProperty[T](propertyKey: String): T = wrapped.removeProperty[T](propertyKey)

  override def equals(other: Any): Boolean = other match {
    case that: Vertex =>
      other.isInstanceOf[Vertex] &&
        this.getId == Identifier(that.getId)
    case _ => false
  }

  override def hashCode = this.getId.hashCode

  override def toString = s"v[$getLabel, gId:$getId, lId:${wrapped.getId}, {$getPropertyKeys}]"
}
