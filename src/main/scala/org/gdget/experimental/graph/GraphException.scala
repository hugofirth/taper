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
package org.gdget.experimental.graph

/** Marker trait for exceptions thrown from the [[org.gdget.experimental.graph]] package. */
sealed trait GraphException extends RuntimeException
object GraphException {
  def unapply(e: GraphException): Option[(String, Throwable)] = Some(e.getMessage -> e.getCause)
}

/**
  *
  * @param id
  * @param cause
  */
case class UnsupportedIdFormatException(id: Option[Any] = None, cause: Option[Throwable] = None)
  extends RuntimeException("The id: "+ id.getOrElse("N/A") +" is not of an accepted format (String, Int or Long)!", cause.orNull)
  with GraphException

/**
  *
  * @param graphType
  * @param cause
  */
case class UnsupportedImplException(graphType: Option[String] = None, cause: Option[Throwable] = None)
  extends RuntimeException("The graph database: " + graphType + " is not a supported underlying implementation for " +
    "PartitionedGraph.", cause.orNull) with GraphException


/**
  *
  * @param id
  * @param cause
  */
case class EdgeWithIdDoesNotExistException(id: Option[Any] = None, cause: Option[Throwable] = None)
  extends RuntimeException("Edge with id does not exist: "+ id.getOrElse("N/A"), cause.orNull) with GraphException

