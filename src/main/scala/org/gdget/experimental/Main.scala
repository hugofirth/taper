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
package org.gdget.experimental

import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph
import org.gdget.experimental.graph.util.io.METISGraphWriter
import scala.collection.JavaConverters._


/** Description of Class
  *
  * @author hugofirth
  */
object Main extends App {

  // GratefulDeadExperiment.run("")
  // ProvGenExperiment1.run("")
  ProvGenExperiment2.run("")
  // MusicBrainzExperiment.run("")

//  val config = Map(
//    "neostore.nodestore.db.mapped_memory" -> "1000M",
//    "neostore.relationshipstore.db.mapped_memory" -> "2500M",
//    "neostore.relationshipgroupstore.db.mapped_memory" -> "10M",
//    "neostore.propertystore.db.mapped_memory" -> "4500M",
//    "neostore.propertystore.db.strings.mapped_memory" -> "800M",
//    "neostore.propertystore.db.arrays.mapped_memory" -> "5M"
//  )
//
//  val graph = new Neo4j2Graph("/Users/hugofirth/Desktop/Data/musicbrainz/graph.db", config.asJava)
//  METISGraphWriter.write(graph, "/Users/hugofirth/Desktop/Data/musicbrainz/musicbrainz.metis")



}
