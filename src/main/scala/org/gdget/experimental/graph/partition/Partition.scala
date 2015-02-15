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

import com.tinkerpop.blueprints.util.{ExceptionFactory, ElementHelper}
import com.tinkerpop.blueprints.{Direction, Graph}
import org.gdget.experimental.graph.EdgeWithIdDoesNotExistException
import org.gdget.util.Identifier
import org.gdget.util.collection.MutableTrieMap

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.JavaConverters._

/** */
object Partition {
  def apply(subGraph: Graph,
            parent: => PartitionedGraph,
            vertexIdMap: mutable.Map[Identifier, Identifier],
            edgeIdMap: mutable.Map[Identifier, Identifier],
            id: Int): Partition = {
    new Partition(subGraph, parent, vertexIdMap, edgeIdMap, id)
  }

}

/** Description of Class
  *
  * @author hugofirth
  */
class Partition private (private[this] val subGraph: Graph,
                         parentGraph: => PartitionedGraph,
                         private[partition] val vertexIdMap: mutable.Map[Identifier, Identifier],
                         private[partition] val edgeIdMap: mutable.Map[Identifier, Identifier],
                         val id: Int) {

  lazy private[partition] val parent = parentGraph
  private[partition] val visitorMatrix = MutableTrieMap[PartitionVertex, Float](Seq.empty[PartitionVertex] -> 1F)
  private val minToBeInThreshold = 0.001F
  private val maxIntroversionThreshold = 0.2F
  private val patterns = parent.traversalSummary
  private val labelCounts = mutable.Map.empty[String, Int]

  //TODO: Replace the following with proper akka code
  //TODO: Check we're not returning "internal" vertices from external partitions?
  //  In most cases this can be easily achieved by making sure that we only get non-external neighbours from internal
  //  vertices. In this way external vertices will act as a "barrier" between partitions.
  //  A better long term solution would be to insure true vertex-cut partitioning. Its semantically clearer than what
  //  we have now.  I think we actually already have this and I am an idiot ... ='( long day
  @tailrec
  final def attemptSwap(potential: PartitionVertex, loss: Float, destinations: List[Partition]): Unit = destinations match {
    case head :: tail if head.shouldAccept(potential, loss) => //Handle vertex having left
      //Check if any external vertices that potential was connected to now have no Internal (to this partition!) neighbours
      //If any exist, then they should be deleted.
      potential.getPartitionVertices(Direction.BOTH)
        .collect({ case e: External if e.getPartitionVertices(Direction.BOTH).size < 2 => e }).map(_.remove())
      //Mark the moved vertex as "external".
      potential.setProperty("__external", head.id)
    case head :: tail => attemptSwap(potential, loss, tail) //If partition doesn't want it then try next best bet
    case Nil => println("Swap failed completely")
  }

  def getPotentialDestPartitions(v: PartitionVertex): List[Partition] = {
    //Get vertex's neighbourhood of partitions
    val neighbourhood = v.getPartitionVertices(Direction.BOTH).collect({ case e: External => e })
      .groupBy( _.getProperty[Int]("__external") ).mapValues(_.size)
    val preference: List[(Int,Int)] = neighbourhood.toList.sortBy({ case (partitionId, incidence) => incidence })
    preference.flatMap { case (partitionId, incidence) => parent.getPartitionById(partitionId) }
  }

  def getPotentialOutcastVertices(minToBeIn: Float = minToBeInThreshold,
                                  maxIntroversion: Float = maxIntroversionThreshold): mutable.PriorityQueue[(PartitionVertex, Float, Float)] = {

    //Reverse order by percentage, then by reverse likelihood to be in
    val potential = mutable.PriorityQueue[(PartitionVertex, Float, Float)]()(
      Ordering[(Float, Float)].on { case(vertex, introversion, probability) => (-introversion/probability, -probability) }
    )
    potential ++= (for{
      v <- getInternalVertices
      //We want both Internal and External neighbours here!
      neighbours = v.getPartitionVertices(Direction.BOTH)
      neighboursByLabel = neighbours.groupBy[String](_.getProperty[String]("__label"))
      introversion = calculateIntroversion(List(Seq(v)))(neighboursByLabel, maxIntroversion)
      if introversion._1/introversion._2 < maxIntroversion
    } yield { (v, introversion._1, introversion._2) })

    //TODO: Work on Map Index of external vertices to enable "outside-in" heuristic
    //TODO: As we are dealing with a read-only graph, would threading be part of an answer here?
  }

  @tailrec
  private def calculateIntroversion(paths: List[Seq[PartitionVertex]],
                                    introversion: Float = 0F,
                                    totalPathProbabilities: Float = 0F)
                                   (vNeighboursByLabel: Map[String, Iterable[PartitionVertex]],
                                    maxIntroversion: Float = maxIntroversionThreshold): (Float, Float) = {
    var newPaths = List.empty[Seq[PartitionVertex]]
    var totalIntroversion = introversion
    var totalProbability = totalPathProbabilities
    for(path <- paths if path.size <= patterns.pathLength) {
      //Get the path's label sequence
      val pathLabels = path.map(_.getProperty[String]("__label"))
      //If patterns contains the label sequence and a prefix
      if(patterns.trie.contains(pathLabels)) {
        //Get the likelihood associated with path
        val pathLikelihood = getLikelihoodOfPath(path)
        //Get the likelihoods associated with each possible subsequent label (internal and external) in the pathLabels sequence
        val labelLikelihoods = patterns.getLikelihoodsFromPattern(pathLabels)
        val absentLabels = labelLikelihoods.keySet &~ vNeighboursByLabel.keySet
        //Uniformly distribute the likelihood of each label across each of the neighbours of v (internal and external) with that label
        val transitionsFromPath: Map[PartitionVertex, Float] = for {
          (label, labelledVertices) <- vNeighboursByLabel
          vIntNeighbour <- labelledVertices if vIntNeighbour.getProperty[String]("__external") == null
          likelihood = labelLikelihoods.getOrElse(label, 0F) / labelledVertices.size
          effectiveLikelihood = likelihood*pathLikelihood
        } yield { (vIntNeighbour, effectiveLikelihood) }
        val remainder: Float = (absentLabels.toList flatMap labelLikelihoods.get).sum*pathLikelihood
        totalIntroversion += transitionsFromPath.values.sum + remainder
        totalProbability += pathLikelihood
      }
      //We want Internal Neighbours here?
      //TODO: work out if want both Internal and External here instead
      newPaths ++= { for(neighbour <- path.head.getPartitionVertices(Direction.BOTH).collect({case i: Internal => i}).toList) yield neighbour+:path }
    }

    //Check Introversion percentage threshold after the paths have reached a third of their total length.
    //This is non-optimal for two reasons 1) we might drop a vertex too early 2) we might do wasted calculations
    //On the lookout for a better solution
    val perIntro = if(totalProbability>0) { totalIntroversion/totalProbability } else { 0F }
    if(newPaths.nonEmpty && (newPaths.head.size<=(patterns.pathLength+3)/3 && perIntro<maxIntroversion)) {
      calculateIntroversion(newPaths, totalIntroversion, totalProbability)(vNeighboursByLabel, maxIntroversion)
    } else { (totalIntroversion, totalProbability) }
  }

  private def getLikelihoodOfPath(path: Seq[PartitionVertex], minToBeIn: Float = minToBeInThreshold) = visitorMatrix.get(path).getOrElse {
    @tailrec
    def traverseLikelihood(mutual: Seq[PartitionVertex], subTrie: MutableTrieMap[PartitionVertex, Float],
                           remaining: Seq[PartitionVertex]): Float = {
      remaining match {
        case head::tail if mutual.nonEmpty =>
          //We want both Internal and External vertices here!
          val neighbourLabels = mutual.last.getPartitionVertices(Direction.BOTH).groupBy( _.getProperty[String]("__label") )
          val labelLikelihoods = patterns.getLikelihoodsFromPattern(mutual.map( _.getProperty[String]("__label") ))
          val nextLikelihood = {
            subTrie.value.getOrElse(0F) *
              (labelLikelihoods(head.getProperty[String]("__label")) /
                neighbourLabels(head.getProperty[String]("__label")).size)
          }
          subTrie += (Seq(head) -> nextLikelihood)
          if(nextLikelihood > minToBeIn) {
            traverseLikelihood(mutual:+head, subTrie.withPrefix(Seq(head)), tail)
          } else{ 0F }
        case head::tail =>
          val labelLikelihoods = patterns.getLikelihoodsFromPattern(mutual.map( _.getProperty[String]("__label") ))
          //special casing the first step. Starting to think I shouldn't.
          val nextLikelihood = {
            subTrie.value.getOrElse(0F) *
              (labelLikelihoods(head.getProperty[String]("__label")) /
                labelCounts.getOrElseUpdate(head.getProperty[String]("__label"),
                  getInternalVertices(head.getProperty[String]("__label")).size))
          }
          subTrie += (Seq(head) -> nextLikelihood)
          if(nextLikelihood > minToBeIn) {
            traverseLikelihood(mutual:+head, subTrie.withPrefix(Seq(head)), tail)
          } else{ 0F }
        case Nil =>
          subTrie.value.getOrElse(0F)
      }
    }

    //Find the maximal prefix in path which does exist in the visitorMatrix
    val maxMutualPrefix = visitorMatrix.maximumMutualPrefix(path)
    val remaining = path.takeRight(path.size-maxMutualPrefix.size)
    traverseLikelihood(maxMutualPrefix, visitorMatrix.withPrefix(maxMutualPrefix), remaining)
    //TODO: Work out if we are properly accounting for a minimum probability to be in a path here.
  }

  //TODO: Add checking of internal neighbours of v to see if any of them come with us, work out how that will work.
  // At the moment this is kind of a flood fill approach, but what is the threshold and why?
  // This is really tricky - I have spent some time trying to think of a solution, will do more soon.
  private def getFamily(v: PartitionVertex): Set[PartitionVertex] = {
    val internalNeighbours = v.getPartitionVertices(Direction.BOTH).collect { case i: Internal => i }
    val n2VLikelihoods = internalNeighbours.map(_ -> 0F).toMap
    // pass 0 up the line for each neighbour, up to path limit. As it accrues probability check if it passes threshold.
    // If it does then it is part of the family. The first generation family is the input to the second generation.
    ???
  }

  //TODO: Calculate gain *after* we have updated neighbours (somehow) otherwise its always the same value.
  def shouldAccept(potential: PartitionVertex, loss: Float): Boolean = {
    val neighbours = potential.getPartitionVertices(Direction.BOTH)
    val neighboursByLabel = neighbours.groupBy[String](_.getProperty[String]("__label"))
    val introversion = calculateIntroversion(List(Seq(potential)))(neighboursByLabel)
    val gain = introversion._1/introversion._2
    if(gain > loss) {
      //Find the external copy of this vertex
      val prevExt = getVertex(potential.getId)
      //Remove the external property and re-wrap the vertex as Internal
      val previousPartition = prevExt.removeProperty[Int]("__external")
      val newVertex = PartitionVertex(PartitionVertex.unwrap(prevExt), prevExt.getId, this)
      //Get neighbours of potential and do Set disjoint with neighbours of it's external counterpart.
      //The resulting set are new "external" neighbours. Create them (with properties etc...)
      val newOutExternalsEdges = potential.getPartitionEdges(Direction.OUT).toSet &~ newVertex.getPartitionEdges(Direction.OUT).toSet
      newOutExternalsEdges.foreach { e: PartitionEdge =>
        //addEdge should handle vertex copying and externalisation for us.
        val newEdge = addEdge(newVertex, e.in, e.getLabel, Some((e.in, previousPartition)))
        ElementHelper.copyProperties(e, newEdge)
      }
      val newInExternalsEdges = potential.getPartitionEdges(Direction.IN).toSet &~ newVertex.getPartitionEdges(Direction.IN).toSet
      newInExternalsEdges.foreach { e: PartitionEdge =>
        //addEdge should handle vertex copying and externalisation for us.
        val newEdge = addEdge(e.out, newVertex, e.getLabel, Some((e.out, previousPartition)))
        ElementHelper.copyProperties(e, newEdge)
      }
      true
    } else { false }
  }

  def shutdown(): Unit = this.subGraph.shutdown()

  /** Takes a global vertex id - looks it up locally, and then retrieves and returns the associated vertex
    *
    *
    * @param globalId the global Long id associated with the desired vertex
    * @return the vertex specified by Long id
    */
  def getVertex(globalId: Identifier): PartitionVertex = {
    val localId = this.vertexIdMap.get(globalId)
    val vertex = localId.map(this.subGraph.getVertex(_))
    if(vertex.isDefined) { PartitionVertex(vertex.get, globalId, this) } else { null }
  }

  /**
    *
    *
    * @return
    */
  def getVertices: Iterable[PartitionVertex] = this.subGraph.getVertices.asScala.view.map { v =>
    PartitionVertex(v, v.getProperty[Any]("__globalId"), this)
  }

  /**
    *
    * @param label
    * @return
    */
  def getVertices(label: String): Iterable[PartitionVertex] = this.subGraph.getVertices("__label", label).asScala.view.map { v =>
    PartitionVertex(v, v.getProperty[Any]("__globalId"), this)
  }

  /**
    *
    *
    * @return
    */
  def getInternalVertices: Iterable[PartitionVertex] = this.getVertices.collect{ case i: Internal => i }

  /**
    *
    * @param label
    * @return
    */
  def getInternalVertices(label: String): Iterable[PartitionVertex] = this.getVertices(label).collect{ case i: Internal => i }

  /**
   *
   *
   * @return
   */
  def getExternalVertices: Iterable[PartitionVertex] = this.getVertices.collect{ case e: External => e }

  /**
   *
   * @param label
   * @return
   */
  def getExternalVertices(label: String): Iterable[PartitionVertex] = this.getVertices(label).collect{ case e: External => e }

  /**
    *
    * @param vertex
    */
  def removeVertex(vertex: PartitionVertex): Unit = {
    if(this.vertexIdMap.get(vertex.getId).isEmpty) { throw ExceptionFactory.vertexWithIdDoesNotExist(vertex.getId) }
    this.vertexIdMap.remove(vertex.getId)
    this.subGraph.removeVertex(PartitionVertex.unwrap(vertex))
  }

  /**
    *
    * @return
    */
  def addVertex(): PartitionVertex = {
    val globalId = this.getNextId
    val newVertex = this.subGraph.addVertex(null)
    vertexIdMap.put(globalId, newVertex.getId)
    PartitionVertex(newVertex, globalId, this)
  }

  /**
    *
    * @param vertex
    * @return
    */
  def addVertex(vertex: PartitionVertex): PartitionVertex = {
    if(vertexIdMap.get(vertex.getId).isDefined) { throw ExceptionFactory.vertexWithIdAlreadyExists() }
    val newVertex = this.subGraph.addVertex(null)
    ElementHelper.copyProperties(vertex, newVertex)
    vertexIdMap.put(vertex.getId, newVertex.getId)
    PartitionVertex(newVertex, vertex.getId, this)
  }

  /**
   *
   * @param globalId
   * @return
   */
  def getEdge(globalId: Identifier): PartitionEdge = {
    val localId = this.edgeIdMap.get(globalId)
    val edge = localId.map(this.subGraph.getEdge(_))
    if(edge.isDefined) { PartitionEdge(edge.get, globalId, this) } else { null }
  }

  /**
    *
    * @return
    */
  def getEdges: Iterable[PartitionEdge] = this.subGraph.getEdges.asScala.view.map { e =>
    PartitionEdge(e, e.getProperty[Any]("__globalId"), this)
  }

  /**
    *
    * @param edge
    */
  def removeEdge(edge: PartitionEdge): Unit = {
    if(this.edgeIdMap.get(edge.getId).isEmpty) { throw EdgeWithIdDoesNotExistException(Some(edge.getId)) }
    this.edgeIdMap.remove(edge.getId)
    this.subGraph.removeEdge(PartitionEdge.unwrap(edge))
  }

  /**
    *
    * @param out
    * @param in
    * @param label
    * @param external
    * @return
    */
  def addEdge(out: PartitionVertex, in: PartitionVertex, label: String, external: Option[(PartitionVertex, Int)] = None): PartitionEdge = {
    val e = external match {
      case Some((v,i)) =>
        val newVertex = this.subGraph.addVertex(null)
        val outVertex = if(v == out) {
          ElementHelper.copyProperties(out, newVertex)
          newVertex.setProperty("__external", i)
          newVertex
        } else { out }
        val inVertex = if(v == in) {
          ElementHelper.copyProperties(in, newVertex)
          newVertex.setProperty("__external", i)
          newVertex
        } else { in }
        this.subGraph.addEdge(null, PartitionVertex.unwrap(outVertex), PartitionVertex.unwrap(inVertex), label)
      case None =>
        this.subGraph.addEdge(null, PartitionVertex.unwrap(out), PartitionVertex.unwrap(in), label)
    }
    val globalId = this.getNextId
    edgeIdMap(globalId) = e.getId
    PartitionEdge(e, globalId, this)
  }

  def getNextId: Long = { parent.getNextElementId }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Partition]

  override def equals(other: Any): Boolean = other match {
    case that: Partition =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = this.id.hashCode
}
