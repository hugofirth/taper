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

import com.tinkerpop.blueprints.{Edge, VertexQuery, Direction, Vertex}

object PartitionVertex {
  def apply(toBeWrapped: Vertex, globalId: Long) = new PartitionVertex(toBeWrapped, globalId)
}

/** Description of Class
  *
  * @author hugofirth
  */
class PartitionVertex private (wrapped: Vertex, globalId: Long) extends Vertex {


  override def getEdges(direction: Direction, strings: String*): Iterable[Edge] =
    wrapped.getEdges(direction, strings: _*)

  override def addEdge(s: String, other: Vertex): Edge = wrapped.addEdge(s, other)

  override def query(): VertexQuery = wrapped.query()

  override def getVertices(direction: Direction, strings: String*): Iterable[Vertex] =
    wrapped.getVertices(direction, strings: _*)

  override def getProperty[T](propertyKey: String): T = wrapped.getProperty[T](propertyKey)

  override def getId: AnyRef = globalId

  override def setProperty(propertyKey: String, propertyValue: scala.Any): Unit =
    wrapped.setProperty(propertyKey, propertyValue)

  override def getPropertyKeys: util.Set[String] = wrapped.getPropertyKeys

  override def remove(): Unit = wrapped.remove()

  override def removeProperty[T](propertyKey: String): T = wrapped.removeProperty[T](propertyKey)
}
