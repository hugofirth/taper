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
package org.gdget.experimental.graph.util.io


import java.io.{PrintWriter, FileWriter, FileNotFoundException, FileOutputStream}

import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph
import com.tinkerpop.blueprints.{Direction, Vertex, Graph}
import org.gdget.util.Identifier
import org.neo4j.graphdb.{Direction => neoDirection, GraphDatabaseService, Node}
import org.neo4j.tooling.GlobalGraphOperations

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/** Description of Class
  *
  * @author hugofirth
  */
case object METISGraphWriter extends GraphWriter {

  implicit val vertexOrdering = Ordering.by[Vertex, Identifier](vertex => vertex.getId)

  override def write(graph: Graph, path: String): Unit = {
    def neighbours(v: Vertex) = v.getVertices(Direction.BOTH).asScala.map(_.getId)

    val pq = mutable.PriorityQueue[Vertex]()(vertexOrdering.reverse)
    graph.getVertices.asScala.foreach(pq.enqueue(_))

    val out = Try(Option(new FileWriter(path))) recover {
      case e: FileNotFoundException =>
        Console.err.println("[ERROR]: Specified file path ("+path+") does not exist and could not be created!")
        Console.err.println(e.getMessage)
        None
      case e: Exception =>
        Console.err.println("[ERROR]: An unexpected error occurred when trying to write to the file "+path)
        Console.err.println(e.getMessage)
        None
    }

    out.get match {
      case Some(output) =>
        val writer = new PrintWriter(output)
        //METIS requires that vertices be numbered from 1, not 0
        val idMap = pq.clone().dequeueAll.iterator.map(_.getId).zip(Iterator from 1).toMap
        val lines = pq.dequeueAll.map(neighbours(_).flatMap(idMap.get))
        val numVertices = lines.size
        val numRelationships = lines.foldLeft(0) { case (accum, n) => accum + n.size }
        writer.println(numVertices+" "+(numRelationships/2))
        lines.foreach( n => writer.println(n.mkString(" ")))
        writer.close()
      case None =>
    }
  }
}

case object METISNeo4jGraphWriter extends GraphWriter {

  implicit val nodeOrdering = Ordering.by[Node, Identifier] { case vertex => -vertex.getId }

  //TODO: Rewrite this method. Look at loaner pattern.
  override def write(graph: Graph, path: String): Unit = {
    def neighbours(n: Node) = n.getRelationships(neoDirection.BOTH).asScala.map(_.getEndNode.getId)

    require(graph.isInstanceOf[Neo4j2Graph], "This graph type is not supported by METISNeo4jGraphWriter")
    val neoGraph = graph.asInstanceOf[Neo4j2Graph].getRawGraph
    val tx = neoGraph.beginTx()
    val pq = mutable.PriorityQueue[Node]()
    try {
      GlobalGraphOperations.at(neoGraph).getAllNodes.asScala.foreach(pq.enqueue(_))
      val writer = new PrintWriter(path)
      //METIS requires that vertices be numbered from 1, not 0
      val idMap = pq.clone().dequeueAll.iterator.map(_.getId).zip(Iterator from 1).toMap
      val lines = pq.dequeueAll.map(neighbours(_).flatMap(idMap.get))
      val numVertices = lines.size
      val numRelationships = lines.foldLeft(0) { case (accum, n) => accum + n.size }
      writer.println(numVertices+" "+(numRelationships/2))
      lines.foreach( n => writer.println(n.mkString(" ")))
      tx.success()
      writer.close()
    } catch {
      case e: FileNotFoundException =>
        Console.err.println("[ERROR]: Specified file path ("+path+") does not exist and could not be created!")
        Console.err.println(e.getMessage)
        tx.failure()
      case e: Exception =>
        Console.err.println("[ERROR]: An unexpected error occurred when trying to write to the file "+path)
        Console.err.println(e.getMessage)
        tx.failure()
    }
    tx.close()
  }
}
