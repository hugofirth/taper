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

import com.tinkerpop.blueprints.{GraphQuery, Direction, Features, Vertex, Edge, Graph => BlueprintsGraph}
import com.tinkerpop.blueprints.util.{DefaultGraphQuery, PropertyFilteredIterable, ExceptionFactory}
import org.gdget.experimental.graph._
import org.gdget.util.Identifier

import scala.collection.mutable
import scala.collection.JavaConverters._


/** Description of Class
  *
  * @author hugofirth
  */
class BigGraph private(val validKeys: Set[String],
               protected val edgeFactory: (Any, Vertex, Vertex, EdgeLabel) => BlueprintsGraph => BigEdge,
               protected val vertexFactory: (Any) => BlueprintsGraph => BigVertex) extends Graph with PropertyRestrictedGraph {

  private val currentId: Iterator[Long] = Iterator.from(0).map(_.toLong)
  private val vertices: mutable.Map[Long, BigVertex] = mutable.Map.empty[Long, BigVertex]
  private val edges: mutable.Map[Long, BigEdge] = mutable.Map.empty[Long, BigEdge]

  override def getEdge(id: scala.Any): BigEdge = {
    if(Option(id).isEmpty)
      throw ExceptionFactory.edgeIdCanNotBeNull()

    this.edges.getOrElse(Identifier(id), { throw EdgeWithIdDoesNotExistException(Some(id)) })
  }

  override def shutdown(): Unit = ()

  override def getVertex(id: scala.Any): BigVertex = {
    if(Option(id).isEmpty)
      throw ExceptionFactory.vertexIdCanNotBeNull()

    this.vertices.getOrElse(Identifier(id), { throw ExceptionFactory.vertexWithIdDoesNotExist(id) })
  }

  override def addEdge(id: scala.Any, outVertex: Vertex, inVertex: Vertex, label: String): Edge = {
    if(Option(label).isEmpty)
      ExceptionFactory.edgeLabelCanNotBeNull()

    val newId = Option(id) match {
      case Some(ident) if edges.get(Identifier(ident)).isDefined => throw ExceptionFactory.edgeWithIdAlreadyExist(ident)
      case Some(ident) => Identifier(ident)
      case None => Identifier(currentId.next())
    }

    val out = outVertex.asInstanceOf[BigVertex]
    val in = inVertex.asInstanceOf[BigVertex]

    val edge = edgeFactory(newId, out, in, label)(this)
    out.addOutEdge(label, edge)
    in.addInEdge(label, edge)
    edges.put(newId, edge)
    edge
  }

  override def getFeatures: Features = BigGraph.features

  override def removeVertex(vertex: Vertex): Unit = {
    vertices.getOrElse(Identifier(vertex.getId), { throw ExceptionFactory.vertexWithIdDoesNotExist(vertex.getId) })

    for(e <- vertex.getEdges(Direction.BOTH).asScala)
      removeEdge(e)

    vertices.remove(Identifier(vertex.getId))
  }

  override def addVertex(id: scala.Any): Vertex = {
    val newId = Option(id) match {
      case Some(ident) if vertices.get(Identifier(ident)).isDefined => throw ExceptionFactory.vertexWithIdAlreadyExists(id)
      case Some(ident) => Identifier(ident)
      case None => Identifier(currentId.next())
    }

    val vertex = vertexFactory(newId)(this)
    vertices.put(newId, vertex)
    vertex
  }

  override def getEdges: JIterable[Edge] = {
    val jEdges: Iterable[Edge] = edges.values.toSeq
    jEdges.asJava
  }

  override def getEdges(key: String, value: scala.Any): JIterable[Edge] =
    new PropertyFilteredIterable[Edge](key, value, getEdges)
//    if(edges.nonEmpty) new PropertyFilteredIterable[Edge](key, value, getEdges) else Iterable.empty[Edge].asJava
  //Fix for potential Scala bug, empty valuesIterator from Map throws NPE on .next(), should be NoSuchElementException



  override def removeEdge(edge: Edge): Unit = {
    val out = edge.getVertex(Direction.OUT)
    val in = edge.getVertex(Direction.IN)
    out.asInstanceOf[BigVertex].removeOutEdge(edge)
    in.asInstanceOf[BigVertex].removeInEdge(edge)
    edges.remove(Identifier(edge.getId))
  }

  override def query(): GraphQuery = new DefaultGraphQuery(this)

  override def getVertices: JIterable[Vertex] = {
    val jVertices: Iterable[Vertex] = vertices.values.toSeq
    jVertices.asJava
  }

  override def getVertices(key: String, value: scala.Any): JIterable[Vertex] =
    new PropertyFilteredIterable[Vertex](key, value, getVertices)
//    if(vertices.nonEmpty) new PropertyFilteredIterable[Vertex](key, value, getVertices) else Iterable.empty[Vertex].asJava
  //Fix for potential Scala bug, empty valuesIterator from Map throws NPE on .next(), should be NoSuchElementException
}

object BigGraph {

  def apply(validKeys: Set[String]): BigGraph = new BigGraph(validKeys,
    (id: Any, out: Vertex, in: Vertex , label: EdgeLabel) => new BigEdge(Identifier(id), out, in, label, _:BlueprintsGraph),
    (id: Any) => new BigVertex(Identifier(id), _:BlueprintsGraph)
  )

  private val features: Features = new Features()

  features.supportsDuplicateEdges = true
  features.supportsSelfLoops = true
  features.supportsSerializableObjectProperty = true
  features.supportsBooleanProperty = true
  features.supportsDoubleProperty = true
  features.supportsFloatProperty = true
  features.supportsIntegerProperty = true
  features.supportsPrimitiveArrayProperty = true
  features.supportsUniformListProperty = true
  features.supportsMixedListProperty = true
  features.supportsLongProperty = true
  features.supportsMapProperty = true
  features.supportsStringProperty = true

  features.ignoresSuppliedIds = false
  features.isPersistent = false
  features.isWrapper = false

  features.supportsIndices = false
  features.supportsKeyIndices = false
  features.supportsVertexKeyIndex = false
  features.supportsEdgeKeyIndex = false
  features.supportsVertexIndex = false
  features.supportsEdgeIndex = false
  features.supportsTransactions = false
  features.supportsVertexIteration = true
  features.supportsEdgeIteration = true
  features.supportsEdgeRetrieval = true
  features.supportsEdgeProperties = true
  features.supportsThreadedTransactions = false
  features.supportsThreadIsolatedTransactions = false
}

