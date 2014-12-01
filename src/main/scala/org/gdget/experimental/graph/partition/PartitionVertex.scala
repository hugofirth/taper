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

import java.lang.Iterable
import java.util

import scala.collection.JavaConverters._

import com.tinkerpop.blueprints.{Edge, VertexQuery, Direction, Vertex}

object PartitionVertex {
  def apply(toBeWrapped: Vertex, globalId: Long, parent: Partition) = new PartitionVertex(toBeWrapped, globalId, parent)
}

/** Description of Class
  *
  * @author hugofirth
  */
class PartitionVertex private (wrapped: Vertex, globalId: Long, parent: Partition) extends Vertex {



  override def getEdges(direction: Direction, labels: String*): Iterable[Edge] = {
    val edgesView = wrapped.getEdges(direction, labels: _*).asScala.view.map { e =>
      PartitionEdge(e, e.getProperty[Long]("__globalId"), parent).asInstanceOf[Edge]
    }
    edgesView.asJava
  }


  override def addEdge(label: String, other: Vertex): Edge = {
    val newEdge = wrapped.addEdge(label, other)
    val globalId = parent.getNextId
    newEdge.setProperty("__globalId", globalId)
    PartitionEdge(newEdge, globalId, parent)
  }

  override def query(): VertexQuery = wrapped.query()

  override def getVertices(direction: Direction, labels: String*): Iterable[Vertex] = {
    val verticesView =  wrapped.getVertices(direction, labels: _*).asScala.view.map { v =>
      PartitionVertex(v, v.getProperty[Long]("__globalId"), parent).asInstanceOf[Vertex]
    }
    verticesView.asJava
  }

  override def getProperty[T](propertyKey: String): T = wrapped.getProperty[T](propertyKey)

  override def getId: AnyRef = globalId

  override def setProperty(propertyKey: String, propertyValue: scala.Any): Unit =
    wrapped.setProperty(propertyKey, propertyValue)

  override def getPropertyKeys: util.Set[String] = wrapped.getPropertyKeys

  override def remove(): Unit = wrapped.remove()

  override def removeProperty[T](propertyKey: String): T = wrapped.removeProperty[T](propertyKey)
}
