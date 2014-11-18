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

import com.tinkerpop.blueprints._
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.util.{DefaultGraphQuery, ExceptionFactory}

import scala.collection.mutable


sealed trait PartitionedGraph extends Graph {
  /** Returns a list of all Partitions in the Graph.
    *
    * In a LocalPartitionedGraph this will return all the partitions in the graph.
    * In a DistributedPartitionedGraph this will return all the partitions available locally.
    *
    * @return an `Iterable` containing [[org.gdget.experimental.graph.partition.Partition]] objects
    */
  def getPartitions: Iterable[Partition]

  /** Returns an Option for a [[org.gdget.experimental.graph.partition.Partition]] object specified by an `Int` id.
    *
    * @param partitionId the `Int` id of the partition to be returned
    * @return the `Option` object representing the partition with id partitionId, or `None`
    */
  def getPartitionById(partitionId: Int): Option[Partition]
}

/** Factory for [[org.gdget.experimental.graph.partition.PartitionedGraph]] instances. */
object PartitionedGraph {
  /** Creates a newly partitioned graph from a provided original.
    *
    * The original must be either a Neo4jGraph or a TinkerGraph.
    *
    * @todo Use generic graph and allow passing of ENUM to specify graph implementation.
    *
    * @param graphLocation a `String` filesystem location for the original graph
    * @param strategy [[org.gdget.experimental.graph.partition.PartitionStrategy]] object defining the method of
    *                subdividing the graph
    * @param numPartitions an `Int` number of partitions to be created
    * @param distributed a `Boolean` which represents whether the graph will be distributed across machines
    * @return The created PartitionedGraph object
    */
  def apply(graphLocation: String, strategy: PartitionStrategy, numPartitions: Int, distributed: Boolean = false) = {
    if(distributed) {
      ???
    } else {
      val graph: Graph = new TinkerGraph(graphLocation)
      //Get the list of subgraphs and create partitions out of them
      var partitions: Map[Int, Partition] = Map()
      val partitionedGraph = new LocalPartitionedGraph(graph, partitions)
      val partitionsList = strategy.execute(graph, numPartitions).zipWithIndex.map { case (components, i) =>
          val partition = Partition(subGraph = components._1, parent = partitionedGraph, vertexIdMap = components._2,
            edgeIdMap = components._3, id = i)
          (i, partition)
      }
      //Add Partitions to a map Int => Partition
      partitions = partitionsList.toMap
      //Return PartitionedGraph
      partitionedGraph
    }
  }
}

/** Description of Class.
  *
  * @constructor creates a new locally partitioned graph from a base graph, a map of partitions and a map of edges
  * @param graph the base graph
  * @param partitionMap a map of partitions to their integer identifiers
  * @param edgeMap a map of global edge identifiers to the set of partition identifiers representing where they may be found
  * @author hugofirth
  */
class LocalPartitionedGraph private (graph: Graph,
                                     partitionMap: => Map[Int, Partition],
                                     edgeMap: mutable.MultiMap[AnyRef, Int]) extends PartitionedGraph {

  private lazy val partitions = partitionMap
  private val features = graph.getFeatures

  override def getPartitions: Iterable[Partition] = partitions.values

  override def getPartitionById(partitionId: Int): Option[Partition] = partitions.get(partitionId)

  private def getEdgeFromPartition(partitionId: Int, edgeId: Long): Edge = {
    getPartitionById(partitionId) match {
      case Some(p) => p.getEdge(edgeId)
      case None => throw PartitionDoesNotExistException(partitionId)
    }
  }

  override def getFeatures: Features = features

  override def getEdge(id: scala.Any): Edge = {
    if(id == null) throw ExceptionFactory.edgeIdCanNotBeNull()
    val idAsLong: Long = id.asInstanceOf[Long]
    edgeMap.get(idAsLong) match {
      case Some(p: Set[Int]) => getEdgeFromPartition(p.head, idAsLong)
      case None => null //If edge not in map, return null as per reference implementation
    }
  }

  override def shutdown(): Unit = partitions.values.foreach(p => p.shutdown())

  override def getVertex(id: scala.Any): Vertex = {
    if(id == null) throw ExceptionFactory.vertexIdCanNotBeNull()
    val idAsLong: Long = id.asInstanceOf[Long]
    val partition = this.partitions.values.find(p => p.getVertex(idAsLong) != null)
    partition match {
      case Some(p) => p.getVertex(idAsLong)
      case None => null //If vertex not in graph, return null as per reference implementation
    }
  }

  override def addEdge(id: scala.Any, outVertex: Vertex, inVertex: Vertex, label: String): Edge = {
    if(label == null) throw ExceptionFactory.edgeLabelCanNotBeNull()
    //NOTE: UNSAFE CASTS... Here there be dragons...
    val outVertexPartition = this.partitions.values.find(p => p.getVertex(outVertex.getId.asInstanceOf[Long]) != null).get
    val inVertexPartition = this.partitions.values.find(p => p.getVertex(inVertex.getId.asInstanceOf[Long]) != null).get
    val idAsLong = id.asInstanceOf[Long]
    val edge = if(outVertexPartition == inVertexPartition) {
      outVertexPartition.addEdge(idAsLong, outVertex, inVertex, label)
    } else {
      outVertexPartition.addEdge(idAsLong, outVertex, inVertex, label, Some(inVertex, inVertexPartition.id))
      inVertexPartition.addEdge(idAsLong, outVertex, inVertex, label, Some(outVertex, outVertexPartition.id))
    }
    edge
  }


  override def removeVertex(vertex: Vertex): Unit = {
    val idAsLong: Long = vertex.getId.asInstanceOf[Long]
    val partition = this.partitions.values.find(p => p.getVertex(idAsLong) != null)
    partition match {
      case Some(p) => p.removeVertex(vertex)
      case None => throw ExceptionFactory.vertexWithIdDoesNotExist(idAsLong)
    }
  }

  override def addVertex(id: scala.Any): Vertex = this.partitions.values.head.addVertex(id.asInstanceOf[Long])

  override def getEdges: Iterable[Edge] = {
    //NOTE: May contain duplicates
    val edgeIterables: Iterable[Iterable[Edge]] = this.partitions.values.map(p => p.getEdges)
    edgeIterables.foldLeft(Iterable.empty[Edge])( (accum, e) => accum ++ e )
  }

  override def getEdges(s: String, o: scala.Any): Iterable[Edge] = ???

  override def removeEdge(edge: Edge): Unit = {
    val idAsLong: Long = edge.getId.asInstanceOf[Long]
    val edgePartitionIndices = edgeMap.get(idAsLong)
    if(edgePartitionIndices.isDefined) {
      edgePartitionIndices.get.foreach { i =>
        getPartitionById(i) match {
          case Some(p) => p.removeEdge(edge)
          case None => throw PartitionDoesNotExistException(i)
        }
      }
    }
  }

  override def query(): GraphQuery = new DefaultGraphQuery(this)

  override def getVertices: Iterable[Vertex] = {
    val vertexIterables: Iterable[Iterable[Vertex]] = partitions.values.map(p => p.getVertices)
    vertexIterables.foldLeft(Iterable.empty[Vertex])( (accum, v) => accum ++ v )
  }

  override def getVertices(s: String, o: scala.Any): Iterable[Vertex] = ???
}
