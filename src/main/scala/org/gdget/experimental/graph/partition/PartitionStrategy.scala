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

import java.io.FileNotFoundException

import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.util.{ExceptionFactory, ElementHelper}
import com.tinkerpop.blueprints.{Edge, Vertex, Direction, Graph}
import org.gdget.experimental.graph.UnsupportedImplException
import org.gdget.util.Identifier

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.Try

/** Defines the contract for a partition strategy object
  *
  * TODO: Clean up this whole class. Its a horrible mess
  *
  * @author hugofirth
  */
sealed trait PartitionStrategy {


  def graph: Graph
  def numPartitions: Int
  def parent: PartitionedGraph
  def props: Map[String, String]

  def labelOf: (Vertex) => String = (v: Vertex) =>
    properties("vertex.labelKey").flatMap({ labelKey => Option(v.getProperty[String](labelKey))}).getOrElse("NA")

  protected val subGraphs = for(i <- 0 until numPartitions ) yield createEmptySubGraph(graph)
  protected val vertexIdMaps = for(i <- 0 until numPartitions) yield mutable.Map[Identifier, Identifier]()
  protected val edgeIdMaps = for(i <- 0 until numPartitions) yield mutable.Map[Identifier, Identifier]()
  protected val extVertexIdMaps = for(i <- 0 until numPartitions) yield mutable.Map[Identifier, Identifier]()

  protected var maxId: Identifier = 0L
  protected var subGraphCounter: Int = 0
  def defaults: Map[String, String] = Map(
    "output.root" -> "target",
    "output.directory" -> "",
    "vertex.labelKey" -> "__label"
  )

  protected lazy val partitions = {
    val partitions = for(i <- 0 until numPartitions) yield {
      (i, Partition(subGraphs(i), parent, vertexIdMaps(i), edgeIdMaps(i), extVertexIdMaps(i), i))
    }
    (partitions.toMap, maxId)
  }


  private def properties(key: String): Option[String] = props.get(key).orElse(defaults.get(key))

  private def incrementAndGetSubGraphLocation(): String = {
    subGraphCounter += 1
    //Safe to use .get here as I know these are either defaults or user specified values
    properties("output.root").get+"/"+properties("output.directory").get+"/"+"graph-"+subGraphCounter
  }

  //TODO: implement proper Neo4j support rather than just importing Neo4j to Tinker
  private def createEmptySubGraph(graph: Graph): Graph = graph match {
    case g: TinkerGraph => new TinkerGraph(incrementAndGetSubGraphLocation())
    case g: Neo4j2Graph => new TinkerGraph(incrementAndGetSubGraphLocation()) //new Neo4j2Graph(getSubGraphLocation())
    case g => throw new UnsupportedImplException(Some(graph.getClass.getSimpleName))
  }

  protected def createExternalVertex(partitionIdx: Int, v: Vertex, ext: Int): Vertex = {
    val newVertex = subGraphs(partitionIdx).addVertex(null)
    ElementHelper.copyProperties(v, newVertex)
    newVertex.setProperty("__external", ext)
    newVertex
  }

  protected def createVertex(partitionIdx: Int, vertex: Vertex) {
    val newVertex = subGraphs(partitionIdx).addVertex(null)
    maxId = if(maxId < vertex.getId) vertex.getId else maxId
    vertexIdMaps(partitionIdx)(vertex.getId) = newVertex.getId
    ElementHelper.copyProperties(vertex, newVertex)
    newVertex.setProperty("__globalId", vertex.getId)
    newVertex.setProperty("__label", labelOf(vertex))
  }

  protected def createEdge(e: Edge) {
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
        val newExtVertex = createExternalVertex(inSubGraphIdx, outSubGraph.getVertex(localOutVertexId), outSubGraphIdx)
        extVertexIdMaps(inSubGraphIdx)(globalOutVertexId) = newExtVertex.getId
        newExtVertex
      }
      val extInVertexId = extVertexIdMaps(outSubGraphIdx).get(globalInVertexId)
      val extInVertex = if(extInVertexId.isDefined) {
        outSubGraph.getVertex(extInVertexId.get)
      } else {
        val newExtVertex = createExternalVertex(outSubGraphIdx, inSubGraph.getVertex(localInVertexId), inSubGraphIdx)
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

  def execute(): (Map[Int, Partition], Identifier)
}


/** The PartitionStrategy class for creating a simple, "Hash-based", graph partition.
  *
  * @author hugofirth
  */
case class HashPartitionStrategy(override val graph: Graph,
                                 override val numPartitions: Int,
                                 override val parent: PartitionedGraph,
                                 override val props: Map[String, String]) extends PartitionStrategy {


  /** Simplistic early implementation of Hash partitioning algorithm. Relies upon all sub graphs fitting into memory.
    *
    * @return The set of created sub graphs along with associated vertex/edge  global -> local Id maps
    */
  //TODO: Adapt the partition to periodically persist the partitions to free up memory?
  //TODO: Adapt to produce min ID count for each partition and pass it on during construction
  override def execute(): (Map[Int, Partition], Identifier) = {

    val partitionKeys = Iterator.continually((0 until numPartitions).toIterator).flatten
    graph.getVertices.asScala.foreach(createVertex(partitionKeys.next(), _))
    graph.getEdges.asScala.foreach(createEdge)
    partitions
  }
}

object HashPartitionStrategy {
  def apply(props: Map[String, String]): (Graph, Int, PartitionedGraph) => PartitionStrategy =
    HashPartitionStrategy(_: Graph, _: Int, _: PartitionedGraph, props)
}

/** The PartitionStrategy class for creating a graph partition which minimises ''edge-cut'' using spectral graph
  * methods (which rely upon Algebraic graph theory and the Feidler vector).
  *
  * @author hugofirth
  */
case class SpectralPartitionStrategy(graph: Graph,
                                     numPartitions: Int,
                                     parent: PartitionedGraph,
                                     props: Map[String, String]) extends PartitionStrategy {

  override def execute(): (Map[Int, Partition], Identifier) = ???
}

/** The PartitionStrategy class for creating a graph partition with minimal ''edge-cut'' by using an external
  * partitioning plan generated using the popular tool METIS: http://glaros.dtc.umn.edu/gkhome/metis/metis/download
  *
  * @author hugofirth
  */
case class METISPartitionStrategy(override val graph: Graph,
                                  override val numPartitions: Int,
                                  override val parent: PartitionedGraph,
                                  override val props: Map[String, String],
                                  planURI: String) extends PartitionStrategy {

  override def execute(): (Map[Int, Partition], Identifier) = {

    val buffer = Try(Source.fromFile(planURI)) recover {
      case e: FileNotFoundException =>
        Console.err.println("[ERROR]: Specified file ("+planURI+") does not exist!")
        Console.err.println(e.getMessage)
        Source.fromIterable(Iterable.empty[Char])
      case e: Exception =>
        Console.err.println("[ERROR]: An unexpected error occurred when trying to read the file "+planURI)
        Console.err.println(e.getMessage)
        Source.fromIterable(Iterable.empty[Char])
    }

    val mappings = for {
      (line, idx) <- buffer.get.getLines().zipWithIndex
      subGraphIdx = line.toInt if subGraphs.isDefinedAt(line.toInt)
      vertex = Option(graph.getVertex(idx)) if vertex.isDefined
    } yield vertex.get -> subGraphIdx

    mappings.foreach { case (vertex, subGraphIdx) => createVertex(subGraphIdx, vertex) }
    graph.getEdges.asScala.foreach(createEdge)

    buffer.get.close()
    partitions
  }
}

