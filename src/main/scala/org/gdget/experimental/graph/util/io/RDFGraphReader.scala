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

import java.io.{FileNotFoundException, FileInputStream}

import com.tinkerpop.blueprints.{Graph => BlueprintsGraph}

import scala.util.Try

/** Description of Class
  *
  * @author hugofirth
  */
case object RDFGraphReader extends GraphReader{
  override def read(graph: BlueprintsGraph, path: String): BlueprintsGraph = {
    val input = Try(Option(new FileInputStream(path))) recover {
      case e: FileNotFoundException =>
        Console.err.println("[ERROR]: Specified file path ("+path+") does not exist and could not be read.")
        Console.err.println(e.getMessage)
        None
      case e: Exception =>
        Console.err.println("[ERROR]: An unexpected error occurred when trying to read from the file "+path)
        Console.err.println(e.getMessage)
        None
    }

    ???
  }
}
