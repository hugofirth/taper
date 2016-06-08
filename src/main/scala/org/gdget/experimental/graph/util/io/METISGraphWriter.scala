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


import java.io.{PrintWriter, FileWriter, FileNotFoundException}


import com.tinkerpop.blueprints.{Direction, Vertex, Graph => BlueprintsGraph}
import org.gdget.experimental.graph.Transaction
import org.gdget.util.Identifier

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/** Description of Class
  *
  * @author hugofirth
  */
case object METISGraphWriter extends GraphWriter {

  implicit val vertexOrdering = Ordering.by[Vertex, Identifier](vertex => vertex.getId)

  override def write(graph: BlueprintsGraph, path: String): Unit = {
    def neighbours(v: Vertex) = v.getVertices(Direction.BOTH).asScala.map(_.getId)

    val tx = Transaction.forGraph(graph)

    val pq = mutable.PriorityQueue[Vertex]()(vertexOrdering.reverse)
    for(v <- graph.getVertices.asScala)
      pq.enqueue(v)

    val out = Try(Option(new FileWriter(path))) recover {
      case e: FileNotFoundException =>
        logger.error("[ERROR]: Specified file path ("+path+") does not exist and could not be created!")
        logger.error(e.getMessage)
        tx.foreach(_.rollback())
        None
      case e: Exception =>
        logger.error("[ERROR]: An unexpected error occurred when trying to write to the file "+path)
        logger.error(e.getMessage)
        tx.foreach(_.rollback())
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
        for( (n,i) <- lines.zipWithIndex ) {
          writer.println(n.mkString(" "))
          if (i % 1000 == 0) logger.debug(s"Written out $i nodes.")
        }
        tx.foreach(_.commit())
        writer.close()
      case None => tx.foreach(_.rollback())
    }
  }
}

//case object METISNeo4jGraphWriter extends GraphWriter {
//
//  implicit val nodeOrdering = Ordering.by[Node, Identifier] { case vertex => -vertex.getId }
//
//  //TODO: Rewrite this method. Look at loaner pattern.
//  override def write(graph: Graph, path: String): Unit = {
//    def neighbours(n: Node) = n.getRelationships(neoDirection.INCOMING).asScala.map(_.getStartNode.getId) ++
//      n.getRelationships(neoDirection.OUTGOING).asScala.map(_.getEndNode.getId)
//
//    require(graph.isInstanceOf[Neo4j2Graph], "This graph type is not supported by METISNeo4jGraphWriter")
//    val neoGraph = graph.asInstanceOf[Neo4j2Graph].getRawGraph
//    val idItr = Iterator from 1
//    val tx = neoGraph.beginTx()
//    val idMap = mutable.Map[Int, Int]()
//    var lines = mutable.ArrayBuffer[Iterable[Int]]()
//    var numRelationships = 0
//    try {
//      println("Starting to read in the graph")
//      GlobalGraphOperations.at(neoGraph).getAllNodes.asScala.foreach { n =>
//        val i = idItr.next()
//        if(i%1000==0) println("Processed "+i+" nodes...")
//        val rels = neighbours(n)
//        idMap += ((n.getId.toInt, i))
//        numRelationships += rels.size
//        lines += rels.map(_.toInt)
//      }
//      tx.success()
//    } catch {
//      case e: Exception =>
//        Console.err.println("[ERROR]: An unexpected error occurred when trying to parse the graph")
//        Console.err.println(e.getMessage)
//        tx.failure()
//    }
//    tx.close()
//    println("Finished reading in the graph. Starting to transform the lines.")
//    lines = lines.map( line => line.flatMap(idMap.get) )
//    try {
//      val writer = new PrintWriter(path)
//      val numVertices = lines.size
//      writer.println(numVertices+" "+(numRelationships/2))
//      lines.foreach( n => writer.println(n.mkString(" ")))
//      writer.close()
//    } catch {
//      case e: FileNotFoundException =>
//        Console.err.println("[ERROR]: Specified file path ("+path+") does not exist and could not be created!")
//        Console.err.println(e.getMessage)
//      case e: Exception =>
//        Console.err.println("[ERROR]: An unexpected error occurred when trying to write to the file "+path)
//        Console.err.println(e.getMessage)
//    }
//
//  }
//}
