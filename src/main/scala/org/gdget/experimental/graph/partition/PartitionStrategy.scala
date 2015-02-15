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
import com.tinkerpop.blueprints.util.{ExceptionFactory, ElementHelper}
import com.tinkerpop.blueprints.{Vertex, Direction, Graph}
import org.gdget.experimental.graph.{UnsupportedImplException, UnsupportedIdFormatException, EdgeWithIdDoesNotExistException}
import org.gdget.util.Identifier

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Defines the contract for a partition strategy object
  *
  * @author hugofirth
  */
sealed trait PartitionStrategy {

  protected var subGraphCounter: Int = 0
  private val defaults: Map[String, String] = Map(
    "output.root" -> "target",
    "output.directory" -> ""
  )
  private val properties: mutable.Map[String, String] = mutable.Map()

  def getProps(key: String): Option[String] = properties.get(key).orElse(defaults.get(key))

  protected def props(props: (String, String)*): mutable.Map[String, String] = this.properties ++= props

  def incrementAndGetSubGraphLocation(): String = {
    subGraphCounter += 1
    //Safe to use .get here as I know these are either defaults or user specified values
    getProps("output.root").get+"/"+getProps("output.directory").get+"/"+"graph-"+subGraphCounter
  }

  def execute(graph: Graph,
              numPartitions: Int,
              parent: PartitionedGraph): (Map[Int, Partition], Long)
  //TODO: implement proper Neo4j support rather than just importing Neo4j to Tinker
  def createEmptySubGraph(graph: Graph): Graph = graph match {
    case g: TinkerGraph => new TinkerGraph(incrementAndGetSubGraphLocation())
    case g: Neo4j2Graph => new TinkerGraph(incrementAndGetSubGraphLocation()) //new Neo4j2Graph(getSubGraphLocation())
    case g => throw new UnsupportedImplException(Some(graph.getClass.getSimpleName))
  }

  def createExternalVertex(g: Graph, v: Vertex, ext: Int): Vertex = {
    val newVertex = g.addVertex(null)
    ElementHelper.copyProperties(v, newVertex)
    newVertex.setProperty("__external", ext)
    newVertex
  }
}

object HashPartitionStrategy {
  def apply(props: (String, String)*): HashPartitionStrategy = {
    val strategy = new HashPartitionStrategy
    strategy.props(props:_*)
    strategy
  }
}

/** The PartitionStrategy class for creating a simple, "Hash-based", graph partition.
  *
  * @author hugofirth
  */
class HashPartitionStrategy extends PartitionStrategy {


  /** Simplistic early implementation of Hash partitioning algorithm. Relies upon all sub graphs fitting into memory.
    *
    *
    *
    * @param graph The parent graph to be sub divided.
    * @param numPartitions The number of sub graphs to create
    * @return The set of created sub graphs along with associated vertex/edge  global -> local Id maps
    */
  //TODO: Adapt the partition to periodically persist the partitions to free up memory?
  //TODO: Adapt to produce min ID count for each partition and pass it on during construction
  override def execute(graph: Graph,
                       numPartitions: Int,
                       parent: PartitionedGraph): (Map[Int, Partition], Long) = {

    subGraphCounter = 0

    val subGraphs = for(i <- 0 until numPartitions ) yield createEmptySubGraph(graph)
    val vertexIdMaps = for(i <- 0 until numPartitions) yield mutable.Map[Identifier, Identifier]()
    val edgeIdMaps = for(i <- 0 until numPartitions) yield mutable.Map[Identifier, Identifier]()
    val extVertexIdMaps = for(i <- 0 until numPartitions) yield mutable.Map[Identifier, Identifier]()

    val vertices = graph.getVertices.asScala
    val partitionKeys = Iterator.continually((0 until numPartitions).toIterator).flatten
    var maxId: Identifier = 0L

    vertices.foreach { v =>
      val subGraphIdx = partitionKeys.next()
      val newVertex = subGraphs(subGraphIdx).addVertex(null)

      maxId = if(maxId < v.getId) v.getId else maxId
      vertexIdMaps(subGraphIdx)(v.getId) = newVertex.getId
      ElementHelper.copyProperties(v, newVertex)
      newVertex.setProperty("__globalId", v.getId)
      val label = v.getProperty[String](getProps("vertex.labelKey").getOrElse("__label"))
      newVertex.setProperty("__label", if(label == null) { "NA" } else { label })
      //TODO: Handle labels for more than just Tinker. Maybe use allow a partial function to do some transformation for each?
    }

    val edges = graph.getEdges.asScala

    //TODO: cleanup this horrible horrible hash partitioning code. Make more "scala-like" and comment at least!
    edges.foreach { e =>
      //Work out which sub graph outgoing vertex is in and add edge to that
      val globalOutVertexId = e.getVertex(Direction.OUT).getId
      val globalInVertexId = e.getVertex(Direction.IN).getId
      val outSubGraphIdx = vertexIdMaps.zipWithIndex.find( _._1.contains(globalOutVertexId) )
        .getOrElse( throw ExceptionFactory.vertexWithIdDoesNotExist(globalOutVertexId) )._2
      val inSubGraphIdx = vertexIdMaps.zipWithIndex.find( _._1.contains(globalInVertexId) )
        .getOrElse( throw ExceptionFactory.vertexWithIdDoesNotExist(globalInVertexId) )._2
      val localOutVertexId = vertexIdMaps(outSubGraphIdx)(globalOutVertexId)
      val localInVertexId = vertexIdMaps(inSubGraphIdx)(globalInVertexId)

      if(outSubGraphIdx == inSubGraphIdx) {
        //Lookup vertex ids local to their containing subgraphs
        val subGraph = subGraphs(outSubGraphIdx)
        val newEdge = subGraph.addEdge(null, subGraph.getVertex(localOutVertexId), subGraph.getVertex(localInVertexId), e.getLabel)
        maxId = if(maxId < e.getId) e.getId else maxId
        edgeIdMaps(outSubGraphIdx)(e.getId) = newEdge.getId
        newEdge.setProperty("__globalId", e.getId)
        ElementHelper.copyProperties(e, newEdge)
      } else {
        //If edge is cross partition create 2 edges and an external vertex
        val outSubGraph = subGraphs(outSubGraphIdx)
        val inSubGraph = subGraphs(inSubGraphIdx)
        val extOutVertexId = extVertexIdMaps(inSubGraphIdx).get(globalOutVertexId)
        val extOutVertex = if(extOutVertexId.isDefined) {
          inSubGraph.getVertex(extOutVertexId.get)
        } else {
          val newExtVertex = createExternalVertex(inSubGraph, outSubGraph.getVertex(localOutVertexId), outSubGraphIdx)
          extVertexIdMaps(inSubGraphIdx)(globalOutVertexId) = newExtVertex.getId
          newExtVertex
        }
        val extInVertexId = extVertexIdMaps(outSubGraphIdx).get(globalInVertexId)
        val extInVertex = if(extInVertexId.isDefined) {
          outSubGraph.getVertex(extInVertexId.get)
        } else {
          val newExtVertex = createExternalVertex(outSubGraph, inSubGraph.getVertex(localInVertexId), inSubGraphIdx)
          extVertexIdMaps(outSubGraphIdx)(globalInVertexId) = newExtVertex.getId
          newExtVertex
        }
        val newOutEdge = outSubGraph.addEdge(null, outSubGraph.getVertex(localOutVertexId), extInVertex, e.getLabel)
        val newInEdge = inSubGraph.addEdge(null, extOutVertex, inSubGraph.getVertex(localInVertexId), e.getLabel)
        maxId = if(maxId < e.getId) e.getId else maxId
        edgeIdMaps(outSubGraphIdx)(e.getId) = newOutEdge.getId
        edgeIdMaps(inSubGraphIdx)(e.getId) = newInEdge.getId
        ElementHelper.copyProperties(e, newOutEdge)
        ElementHelper.copyProperties(e, newInEdge)
        newInEdge.setProperty("__globalId", e.getId)
        newOutEdge.setProperty("__globalId", e.getId)
      }

    }


    val partitions = for(i <- 0 until numPartitions) yield {
      (i, Partition(subGraphs(i), parent, vertexIdMaps(i), edgeIdMaps(i), i))
    }
    (partitions.toMap, maxId)
  }
}

/** The PartitionStrategy object for creating a graph partition which minimises ''edge-cut'' using spectral graph
  * methods (which rely upon Algebraic graph theory and the Feidler vector).
  *
  * @author hugofirth
  */
class SpectralPartitionStrategy extends PartitionStrategy {
  override def execute(graph: Graph,
                       numPartitions: Int,
                       parent: PartitionedGraph): (Map[Int, Partition], Long) = ???
}
