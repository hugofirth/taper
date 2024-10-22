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

import java.io.{InputStream, FileNotFoundException}

import com.tinkerpop.blueprints.util.{ExceptionFactory, ElementHelper}
import com.tinkerpop.blueprints.{Edge, Vertex, Direction, Graph => BlueprintsGraph}
import com.typesafe.scalalogging.slf4j.LazyLogging


import org.gdget.experimental.graph.{Transaction, UnsupportedImplException}
import org.gdget.util.{SystemUtils, Identifier}


import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

/** Defines the contract for a partition strategy object
  *
  * TODO: Clean up this whole class. Its a horrible mess. Please don't judge ;)
  *
  * @author hugofirth
  */
sealed trait PartitionStrategy extends LazyLogging {

  def graph: BlueprintsGraph
  def subGraphFactory: (String) => BlueprintsGraph
  def numPartitions: Int
  def parent: PartitionedGraph
  def props: Map[String, String]
  def label: Option[(Vertex) => String]

  protected def labelOf = label.getOrElse(defaultLabelOf)
  protected def defaultLabelOf: (Vertex) => String = (v: Vertex) =>
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

  private def createEmptySubGraph(graph: BlueprintsGraph): BlueprintsGraph = subGraphFactory(incrementAndGetSubGraphLocation())

  protected def createExternalVertex(partitionIdx: Int, v: Vertex, ext: Int): Vertex = {
    val newVertex = subGraphs(partitionIdx).addVertex(null)
    ElementHelper.copyProperties(v, newVertex)
    newVertex.setProperty("__external", ext)
    newVertex
  }

  protected def createVertex(partitionIdx: Int, vertex: Vertex) {
    val newVertex = subGraphs(partitionIdx).addVertex(null)
    val vId = Identifier(vertex.getId)
    maxId = if(maxId < vId) vId else maxId
    vertexIdMaps(partitionIdx)(vId) = newVertex.getId
    //ElementHelper.copyProperties(vertex, newVertex)
    newVertex.setProperty("__globalId", vId.toLong)
    newVertex.setProperty("__label", labelOf(vertex))
  }

  protected def createEdge(e: Edge ) {
    //Work out which sub graph outgoing vertex is in and add edge to that
    val globalOutVertexId = e.getVertex(Direction.OUT).getId
    val globalInVertexId = e.getVertex(Direction.IN).getId
    val outSubGraphIdx = vertexIdMaps.zipWithIndex.find( _._1.contains(globalOutVertexId) )
      .getOrElse( throw ExceptionFactory.vertexWithIdDoesNotExist(globalOutVertexId) )._2
    val inSubGraphIdx = vertexIdMaps.zipWithIndex.find( _._1.contains(globalInVertexId) )
      .getOrElse( throw ExceptionFactory.vertexWithIdDoesNotExist(globalInVertexId) )._2
    val localOutVertexId = vertexIdMaps(outSubGraphIdx)(globalOutVertexId)
    val localInVertexId = vertexIdMaps(inSubGraphIdx)(globalInVertexId)

    val eId = Identifier(e.getId)

    if(outSubGraphIdx == inSubGraphIdx) {
      //Lookup vertex ids local to their containing subgraphs
      val subGraph = subGraphs(outSubGraphIdx)
      val newEdge = subGraph.addEdge(null, subGraph.getVertex(localOutVertexId), subGraph.getVertex(localInVertexId), e.getLabel)
      maxId = if(maxId < eId) eId else maxId
      edgeIdMaps(outSubGraphIdx)(eId) = newEdge.getId
      newEdge.setProperty("__globalId", eId.toLong)
      //ElementHelper.copyProperties(e, newEdge)
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
      maxId = if(maxId < eId) eId else maxId
      edgeIdMaps(outSubGraphIdx)(eId) = newOutEdge.getId
      edgeIdMaps(inSubGraphIdx)(eId) = newInEdge.getId
//      ElementHelper.copyProperties(e, newOutEdge)
//      ElementHelper.copyProperties(e, newInEdge)
      newInEdge.setProperty("__globalId", eId.toLong)
      newOutEdge.setProperty("__globalId", eId.toLong)
    }
  }

  def execute(): (Map[Int, Partition], Identifier)
}

/** The PartitionStrategy class for creating a simple, "Hash-based", graph partition.
  *
  * @author hugofirth
  */
case class HashPartitionStrategy(override val graph: BlueprintsGraph,
                                 override val subGraphFactory: (String) => BlueprintsGraph,
                                 override val numPartitions: Int,
                                 override val parent: PartitionedGraph,
                                 override val props: Map[String, String],
                                 override val label: Option[(Vertex) => String] = None) extends PartitionStrategy {


  /** Simplistic early implementation of Hash partitioning algorithm. Relies upon all sub graphs fitting into memory.
    *
    * @return The set of created sub graphs along with associated vertex/edge  global -> local Id maps
    */
  //TODO: Adapt the partition to periodically persist the partitions to free up memory?
  //TODO: Adapt to produce min ID count for each partition and pass it on during construction
  override def execute(): (Map[Int, Partition], Identifier) = {
    def createElements() = {
      val partitionKeys = Iterator.continually((0 until numPartitions).toIterator).flatten
      //graph.getVertices.asScala.foreach(createVertex(partitionKeys.next(), _))
      for((vertex, id) <- graph.getVertices.asScala.zipWithIndex) {
        createVertex(partitionKeys.next(), vertex)
        if(id%10000 == 0)
          logger.debug(s"Created $id vertices.")
          if(id%1000000 == 0)
            logger.debug(SystemUtils.heapStats)
      }
      //graph.getEdges.asScala.foreach(createEdge)
      for((edge, id) <- graph.getEdges.asScala.zipWithIndex) {
        createEdge(edge)
        if(id%10000 == 0)
          logger.debug(s"Created $id edges.")
          if(id%1000000 == 0)
            logger.debug(SystemUtils.heapStats)
      }
    }

    val tx = Transaction.forGraph(graph)

    Try(createElements()) match {
      case Success(_) => tx.foreach(_.commit())
      case Failure(e) =>
        logger.error("[ERROR]: An unexpected error occurred when trying to Hash partition the graph!")
        logger.error(e.toString)
        tx.foreach(_.rollback())
    }

    partitions
  }
}

object HashPartitionStrategy {
  def apply(subGraphFactory: (String) => BlueprintsGraph,
            props: Map[String, String],
            label: Option[(Vertex) => String]): (BlueprintsGraph, Int, PartitionedGraph) => PartitionStrategy =
    HashPartitionStrategy(_: BlueprintsGraph, subGraphFactory, _: Int, _: PartitionedGraph, props, label)

  def apply(subGraphFactory: (String) => BlueprintsGraph,
            props: Map[String, String]): (BlueprintsGraph, Int, PartitionedGraph) => PartitionStrategy =
    HashPartitionStrategy(_: BlueprintsGraph, subGraphFactory, _: Int, _: PartitionedGraph, props)
}

/** The PartitionStrategy class for creating a graph partition which minimises ''edge-cut'' using spectral graph
  * methods (which rely upon Algebraic graph theory and the Feidler vector).
  *
  * @author hugofirth
  */
case class SpectralPartitionStrategy(override val graph: BlueprintsGraph,
                                     override val subGraphFactory: (String) => BlueprintsGraph,
                                     override val numPartitions: Int,
                                     override val parent: PartitionedGraph,
                                     override val props: Map[String, String],
                                     override val label: Option[(Vertex) => String] = None) extends PartitionStrategy {

  override def execute(): (Map[Int, Partition], Identifier) = ???
}

/** The PartitionStrategy class for creating a graph partition with minimal ''edge-cut'' by using an external
  * partitioning plan generated using the popular tool METIS: http://glaros.dtc.umn.edu/gkhome/metis/metis/download
  *
  * TODO: Fix to accept String again. What did I mean by this?
  *
  * @author hugofirth
  */
case class METISPartitionStrategy(override val graph: BlueprintsGraph,
                                  override val subGraphFactory: (String) => BlueprintsGraph,
                                  override val numPartitions: Int,
                                  override val parent: PartitionedGraph,
                                  override val props: Map[String, String],
                                  planURI: InputStream,
                                  override val label: Option[(Vertex) => String] = None) extends PartitionStrategy {

  override def execute(): (Map[Int, Partition], Identifier) = {

    def createElementsFrom(src: Source) = {
      val partitionBuffers = for(i <- 0 until numPartitions) yield mutable.ListBuffer.empty[Vertex]

      for {
        (p, v) <- src.getLines()
          .zip(graph.getVertices.asScala.iterator)
        pId = p.toInt if subGraphs.isDefinedAt(p.toInt)
      } partitionBuffers(pId) += v

      logger.debug("Finished adding vertices to partition buckets.")
      logger.debug(SystemUtils.heapStats)

      var i = 0
      for {
        (vertices, idx) <- partitionBuffers.zipWithIndex
        vertex <- vertices
      } { createVertex(idx, vertex); i+=1; if(i%10000 == 0) logger.debug(s"Created $i vertices.") }

      partitionBuffers.foreach(_.clear())
      logger.debug("Finished creating partition vertices.")
      logger.debug(SystemUtils.heapStats)

      i = 0
      for(edge <- graph.getEdges.asScala) { createEdge(edge); i+=1; if(i%10000 == 0) logger.debug(s"Created $i edges.") }
      logger.debug("Finished creating partition edges.")
    }

    logger.debug("Starting to partition the graph")

    val buffer = Try(Source.fromInputStream(planURI)) recover {
      case e: FileNotFoundException =>
        logger.error("[ERROR]: Specified file ("+planURI+") does not exist!")
        logger.error(e.getMessage)
        Source.fromIterable(Iterable.empty[Char])
      case e: Exception =>
        logger.error("[ERROR]: An unexpected error occurred when trying to read the file "+planURI)
        logger.error(e.getMessage)
        Source.fromIterable(Iterable.empty[Char])
    }

    val tx = Transaction.forGraph(graph)

    Try(createElementsFrom(buffer.get)) match {
      case Success(_) => tx.foreach(_.commit())
      case Failure(e) =>
        Console.err.println("[ERROR]: An unexpected error occurred when trying to METIS partition the graph!")
        Console.err.println(e.getMessage)
        tx.foreach(_.rollback())
        System.exit(1)
    }

    buffer.get.close()
    partitions
  }
}

object METISPartitionStrategy {

  def apply(subGraphFactory: (String) => BlueprintsGraph,
            props: Map[String, String],
            planURI: InputStream,
            label: Option[(Vertex) => String]): (BlueprintsGraph, Int, PartitionedGraph) => PartitionStrategy =
    METISPartitionStrategy(_: BlueprintsGraph, subGraphFactory, _: Int, _: PartitionedGraph, props, planURI, label)

  def apply(subGraphFactory: (String) => BlueprintsGraph,
            props: Map[String, String],
            planURI: InputStream): (BlueprintsGraph, Int, PartitionedGraph) => PartitionStrategy =
    METISPartitionStrategy(_: BlueprintsGraph, subGraphFactory, _: Int, _: PartitionedGraph, props, planURI)
}