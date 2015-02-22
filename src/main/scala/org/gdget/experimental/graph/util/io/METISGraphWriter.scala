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

import com.tinkerpop.blueprints.{Direction, Vertex, Graph}
import org.gdget.util.Identifier

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/** Description of Class
  *
  * @author hugofirth
  */
case object METISGraphWriter extends GraphWriter {
  override def write(graph: Graph, path: String): Unit = {

    val pq = mutable.PriorityQueue[Vertex]()(Ordering[Identifier].on { case vertex => vertex.getId })
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
        val pw = new PrintWriter(output)
        pq.dequeueAll.foreach { v => pw.println(v.getVertices(Direction.OUT).asScala.map(_.getId).mkString(" ")) }
        pw.close()
      case None =>
    }
  }
}
