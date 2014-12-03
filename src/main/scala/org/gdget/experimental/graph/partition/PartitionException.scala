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

import org.gdget.util.Identifier

/** Marker trait for exceptions thrown from the [[org.gdget.experimental.graph.partition]] package. */
sealed trait PartitionException extends RuntimeException
object PartitionException {
  def unapply(e: PartitionException): Option[(String, Throwable)] = Some(e.getMessage -> e.getCause)
}

/**
  *
  * @param partitionId
  * @param cause
  */
case class PartitionDoesNotExistException(partitionId: Option[Int] = None, cause: Option[Throwable] = None)
  extends RuntimeException("The partition with id "+partitionId.getOrElse("N/A")+" could not be found!", cause.orNull)
  with PartitionException

/**
  *
  * @param edgeId
  * @param partitionId
  * @param cause
  */
case class EdgeDoesNotExistException(edgeId: Option[Identifier] = None,
                                     partitionId: Option[Int] = None,
                                     cause: Option[Throwable] = None)
  extends RuntimeException("The edge with id "+edgeId.getOrElse("N/A")+" could not be found in the partition with" +
    " id "+partitionId.getOrElse("N/A")+"!", cause.orNull) with PartitionException