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
package org.gdget.experimental.graph.big

import java.lang.{Iterable => JIterable}


import org.gdget.experimental.graph.util.Labellable

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.language.postfixOps

import com.tinkerpop.blueprints.util.{VerticesFromEdgesIterable, DefaultVertexQuery}
import com.tinkerpop.blueprints.{Edge, VertexQuery, Direction, Vertex, Graph => BlueprintsGraph}

import org.gdget.collection.CompressedMultiMap

/** Description of Class
  *
  *
  * @author hugofirth
  */
case class BigVertex(id: Long, private val parent: BlueprintsGraph) extends Vertex with BigElement with Labellable {

  //TODO: Get rid of this horror in refactor
  require(parent.isInstanceOf[BigGraph], "BigVertex parent graph must be of type BigGraph. Found " +
    parent.getClass.getCanonicalName)

  override protected val graph = parent.asInstanceOf[BigGraph]

  private val outEdges = new mutable.HashMap[String, Set[Edge]]() with CompressedMultiMap[String, Edge]
  private val inEdges = new mutable.HashMap[String, Set[Edge]]() with CompressedMultiMap[String, Edge]

  override def addEdge(label: String, vertex: Vertex): Edge = graph.addEdge(id, this, vertex, label)

  override def getEdges(direction: Direction, labels: String*): JIterable[Edge] = labels match {
    case Nil => direction match {
      case Direction.OUT => outEdges.values.flatten.asJava
      case Direction.IN => inEdges.values.flatten.asJava
      case Direction.BOTH => (outEdges.values.flatten ++ inEdges.values.flatten).asJava
    }
    case _ => direction match {
      case Direction.OUT => labels.flatMap(outEdges.get).flatten.asJava
      case Direction.IN => labels.flatMap(inEdges.get).flatten.asJava
      case Direction.BOTH => (labels.flatMap(outEdges.get).flatten ++ labels.flatMap(inEdges.get).flatten).asJava
    }
  }

  override def query(): VertexQuery = new DefaultVertexQuery(this)

  override def getVertices(direction: Direction, labels: String*): JIterable[Vertex] =
    new VerticesFromEdgesIterable(this, direction, labels:_*)

  private[big] def addOutEdge(label:String, edge: Edge) = outEdges.addBinding(label, edge)

  private[big] def addInEdge(label:String, edge: Edge) = inEdges.addBinding(label, edge)

  private[big] def removeOutEdge(edge: Edge) = outEdges.removeBinding(edge.getLabel, edge)

  private[big] def removeInEdge(edge: Edge) = inEdges.removeBinding(edge.getLabel, edge)

  override def remove() = graph.removeVertex(this)
}
