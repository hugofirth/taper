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

import java.util.concurrent.atomic.AtomicLong
import java.lang.{Iterable => JIterable}

import com.tinkerpop.blueprints._
import com.tinkerpop.blueprints.util.{DefaultGraphQuery, ExceptionFactory}
import org.gdget.experimental.graph.TraversalPatternSummary
import org.gdget.util.{Counting, Identifier}

import scala.collection.JavaConverters._



sealed trait PartitionedGraph extends Graph {
  /** Returns a list of all Partitions in the Graph.
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

  /** Returns the next Long id for an element in the graph.
    *
    * @return a `Long` to act as an [[com.tinkerpop.blueprints.Element]] object id
    */
  private[partition] def getNextElementId: Long

  /** Returns a summary of the common traversal operations and their frequencies
    *
    * @return a `TraversalPatternSummary` object
    */
  def traversalSummary: TraversalPatternSummary


  /** Override [[com.tinkerpop.blueprints.Graph]] methods without implementation in PartitionedGraph trait to declare
    * covariant return types.
    */
  override def getEdge(id: scala.Any): PartitionEdge
  override def getVertex(id: scala.Any): PartitionVertex
  override def addEdge(id: scala.Any, outVertex: Vertex, inVertex: Vertex, label: String): PartitionEdge
  override def addVertex(id: scala.Any): PartitionVertex

}

/** Factory for [[org.gdget.experimental.graph.partition.PartitionedGraph]] instances. */
object PartitionedGraph {
  /** Creates a newly partitioned graph from a provided original.
    *
    * The original must be either a Neo4jGraph or a TinkerGraph.
    *
    * @param graph the original graph
    * @param strategy [[org.gdget.experimental.graph.partition.PartitionStrategy]] object defining the method of
    *                subdividing the graph
    * @param numPartitions an `Int` number of partitions to be created
    * @param trie a `TraversalPatternSummary` which summarises frequencies associated with sequences of traversals
    * @param distributed a `Boolean` which represents whether the graph will be distributed across machines
    * @return The created PartitionedGraph object
    */
  def apply(graph: Graph,
            strategy: PartitionStrategy,
            numPartitions: Int,
            trie: TraversalPatternSummary,
            distributed: Boolean = false) = {
    if(distributed) {
      ???
    } else {
      //Get the list of subgraphs and create partitions out of them
      var partitions: Map[Int, Partition] = Map()
      var initialId = 0L
      val partitionedGraph = new LocalPartitionedGraph(graph, partitions, trie, initialId) with Counting
      val partitionComponents = strategy.execute(graph, numPartitions, partitionedGraph)
      //Add Partitions to a map Int => Partition
      partitions = partitionComponents._1
      initialId = partitionComponents._2
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
  * @author hugofirth
  */
//TODO: update ids to be globalId case class at top level (Int, Identifier)
//TODO: update use of Pattern matching to resolve Options to a .map() call instead
class LocalPartitionedGraph private[partition] (graph: Graph,
                                     partitionMap: => Map[Int, Partition],
                                     val traversalSummary: TraversalPatternSummary,
                                     initialId: => Long = 0) extends PartitionedGraph {

  private lazy val partitions = partitionMap
  private val features = graph.getFeatures
  private lazy val counter = new AtomicLong(initialId)

  override def getPartitions: Iterable[Partition] = partitions.values

  override def getPartitionById(partitionId: Int): Option[Partition] = partitions.get(partitionId)

  override def getFeatures: Features = features

  override def getEdge(id: scala.Any): PartitionEdge = {
    if(id == null) throw ExceptionFactory.edgeIdCanNotBeNull()
    this.getEdgePartitionById(id).flatMap( _.getEdge(id) ).orNull
    //If edge not in map, return null as per reference implementation
  }

  override def shutdown(): Unit = partitions.values.foreach( _.shutdown() )

  override def getVertex(id: scala.Any): PartitionVertex = {
    if(id == null) throw ExceptionFactory.vertexIdCanNotBeNull()
    this.getVertexPartitionById(id).flatMap( _.getVertex(id) ).orNull
    //If vertex not in map, return null as per reference implementation
  }

  override def addEdge(id: scala.Any, outVertex: Vertex, inVertex: Vertex, label: String): PartitionEdge = {
    val parOutVertex = outVertex.asInstanceOf[PartitionVertex]
    val parInVertex = inVertex.asInstanceOf[PartitionVertex]

    if(label == null) throw ExceptionFactory.edgeLabelCanNotBeNull()
    val outVertexPartition = this.getVertexPartition(parOutVertex).getOrElse {
      throw ExceptionFactory.vertexWithIdDoesNotExist(parOutVertex.getId)
    }
    val inVertexPartition = this.getVertexPartition(parInVertex).getOrElse {
      throw ExceptionFactory.vertexWithIdDoesNotExist(parInVertex.getId)
    }
    if(outVertexPartition == inVertexPartition) {
      outVertexPartition.addEdge(parOutVertex, parInVertex, label)
    } else {
      inVertexPartition.addEdge(parOutVertex, parInVertex, label, Some(parOutVertex, outVertexPartition.id))
      outVertexPartition.addEdge(parOutVertex, parInVertex, label, Some(parInVertex, inVertexPartition.id))
    }
  }


  override def removeVertex(vertex: Vertex): Unit = {
    //Unsafe Cast - here there be dragons
    val parVertex = vertex.asInstanceOf[PartitionVertex]
    val partitions = this.getVertexPartitions(parVertex)
    if(partitions.isEmpty) {
      //If vertex not found, throw Exception as per reference implementation
      throw ExceptionFactory.vertexWithIdDoesNotExist(vertex.getId)
    }
    partitions.map( _.removeVertex(parVertex) )
  }

  //TODO: Create a sensible and balanced approach for adding vertices to partitions
  override def addVertex(id: scala.Any): PartitionVertex = this.partitions.values.head.addVertex()

  override def getEdges: JIterable[Edge] = {
    //TODO: Work out if this can ever contain duplicates or if we are truly edge disjoint?
    val edgeIterables: Iterable[Iterable[Edge]] = this.partitions.values.map( _.getEdges )
    edgeIterables.foldLeft(Iterable.empty[Edge])( (accum, e) => accum ++ e ).asJava
  }

  override def getEdges(s: String, o: scala.Any): JIterable[Edge] = ???

  override def removeEdge(edge: Edge): Unit = {
    //Unsafe Cast - here there be dragons
    val parEdge = edge.asInstanceOf[PartitionEdge]
    this.getEdgePartitions(parEdge).map( _.removeEdge(parEdge) )
  }

  override def query(): GraphQuery = new DefaultGraphQuery(this)

  override def getVertices: JIterable[Vertex] = {
    //May contain duplicates
    val vertexIterables: Iterable[Iterable[Vertex]] = partitions.values.map( _.getVertices )
    vertexIterables.foldLeft(Iterable.empty[Vertex])( (accum, v) => accum ++ v ).asJava
  }

  override def getVertices(s: String, o: scala.Any): JIterable[Vertex] = ???

  override def getNextElementId = counter.incrementAndGet()

  //TODO: When checking partitions for Elements, if external copies are found then use those to link to the real element quicker
  private def getVertexPartition(vertex: PartitionVertex): Option[Partition] = this.getVertexPartitionById(vertex.getId)

  private def getVertexPartitions(vertex: PartitionVertex): Iterable[Partition] = this.getVertexPartitionsById(vertex.getId)

  private def getEdgePartition(edge: PartitionEdge): Option[Partition] = this.getEdgePartitionById(edge.getId)

  private def getEdgePartitions(edge: PartitionEdge): Iterable[Partition] = this.getEdgePartitionsById(edge.getId)

  private def getVertexPartitionById(id: Identifier): Option[Partition] = this.partitions.values.find( _.getVertex(id).isDefined )

  private def getVertexPartitionsById(id: Identifier): Iterable[Partition] = this.partitions.values.filter( _.getVertex(id).isDefined )

  private def getEdgePartitionById(id: Identifier): Option[Partition] = this.partitions.values.find( _.getEdge(id).isDefined )

  private def getEdgePartitionsById(id: Identifier): Iterable[Partition] = this.partitions.values.filter( _.getEdge(id).isDefined )

}
