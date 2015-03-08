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
import org.gdget.experimental.graph.Path._
import org.gdget.util.Identifier
import org.gdget.collection.MutableTrieMap

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.JavaConverters._

//TODO: Reintroduce magic number in receiving calulcateIntroversion check (0.5) and see what effect it has.
//TODO: Try higher introversions in general.
//TODO: Find pathological case for METIS getting it wrong.

sealed trait Refinable {
  this: Partition with Dynamic =>

  private[partition] val visitorMatrix = MutableTrieMap[PartitionVertex, Float](Seq.empty[PartitionVertex] -> 1F)
  private val probThreshold = 0.001F
  private val introThreshold = 0.2F
  private lazy val patterns = parent.traversalSummary
  private val labelCounts = mutable.Map.empty[String, Int]

  def refine(minProb: Float = probThreshold, maxIntro: Float = introThreshold) = {
    val pq = getPotentialOutcastVertices(minProb, maxIntro)
    pq.dequeueAll.map { case (vertexId, introversion, probability) =>
      getVertex(vertexId) map { vertex =>
        attemptSwap(vertex, probability, getPotentialDestPartitions(vertex), maxIntro)
      }
    }
  }

  //TODO: Replace the following with proper akka code
  @tailrec private[partition] final def attemptSwap(potential: PartitionVertex,
                                   prob: Float,
                                   destinations: List[Partition],
                                   maxIntro: Float): Unit = destinations match {
    case head :: tail if head.shouldAccept(potential, prob-getLikelihoodOfPath(Seq(potential)), maxIntro) =>
      getFamily(potential).foreach { v => send(v, head); head.receive(v) }
    case head :: tail => attemptSwap(potential, prob, tail, maxIntro) //If partition doesn't want it then try next best bet
    case Nil => //Swap failed entirely - should do some logging here.
  }

  def getPotentialDestPartitions(v: PartitionVertex): List[Partition] = {
    //Get vertex's neighbourhood of partitions
    val neighbourhood = v.extVertices(Direction.BOTH).groupBy( _.getProperty[Int]("__external") ).mapValues(_.size)
    val preference: List[(Int,Int)] = neighbourhood.toList.sortBy({ case (partitionId, incidence) => -incidence })
    preference.flatMap { case (partitionId, incidence) => parent.getPartitionById(partitionId) }
  }

  private[partition] def getPotentialOutcastVertices(minToBeIn: Float = probThreshold,
                                  maxIntro: Float = introThreshold): mutable.PriorityQueue[(Identifier, Float, Float)] = {

    //Reverse order by percentage, then by reverse likelihood to be in
    val potential = mutable.PriorityQueue[(Identifier, Float, Float)]()(
      Ordering[(Float, Float)].on { case(vertexId, introversion, probability) => (-introversion/probability, -probability) }
    )
    potential ++= (for{
      v <- getInternalVertices
      //We want both Internal and External neighbours here!
      neighbours = v.partitionVertices(Direction.BOTH)
      neighboursByLabel = neighbours.groupBy[String](_.getLabel).mapValues(vItr => (vItr.collect({ case i: Internal => i }), vItr.size))
      introversion = calculateIntroversion(List(Seq(v)))(neighboursByLabel, maxIntro)
      if introversion._1/introversion._2 < maxIntro
    } yield { (v.getId, introversion._1, introversion._2) })

    //TODO: Work on Map Index of external vertices to enable "outside-in" heuristic
    //TODO: As we are dealing with a read-only graph, would threading be part of an answer here?
  }


  @tailrec private def calculateIntroversion(paths: List[Seq[PartitionVertex]],
                                    introversion: Float = 0F,
                                    totalPathProbabilities: Float = 0F)
                                   (vNeighboursByLabel: Map[String, (Iterable[PartitionVertex], Int)],
                                    maxIntro: Float = introThreshold): (Float, Float) = {
    var newPaths = List.empty[Seq[PartitionVertex]]
    var totalIntroversion = introversion
    var totalProbability = totalPathProbabilities
    for(path <- paths if path.size <= patterns.pathLength) {
      //Get the path's label sequence
      val pathLabels = path.toLabels
      //If patterns contains the label sequence and a prefix
      if(patterns.trie.contains(pathLabels)) {
        //Get the likelihood associated with path
        val pathLikelihood = getLikelihoodOfPath(path)
        //Get the likelihoods associated with each possible subsequent label (internal and external) in the pathLabels sequence
        val labelLikelihoods = patterns.getLikelihoodsFromPattern(pathLabels)
        val absentLabels = labelLikelihoods.keySet &~ vNeighboursByLabel.keySet
        //Uniformly distribute the likelihood of each label across each of the neighbours of v (internal and external) with that label
        //Then assign their appropriate share to internal labels.
        val transitionsFromPath: Map[PartitionVertex, Float] = for {
          (label, (labelledVertices, numLabel)) <- vNeighboursByLabel
          vIntNeighbour <- labelledVertices
          likelihood = labelLikelihoods.getOrElse(label, 0F) / numLabel
          effectiveLikelihood = likelihood*pathLikelihood
        } yield { (vIntNeighbour, effectiveLikelihood) }
        val remainder: Float = (absentLabels.toList flatMap labelLikelihoods.get).sum*pathLikelihood
        totalIntroversion += transitionsFromPath.values.sum + remainder
        totalProbability += pathLikelihood
      }
      //We want Internal Neighbours here
      //TODO: work out if want both Internal and External here instead
      newPaths ++= { for(neighbour <- path.head.intVertices(Direction.BOTH).toList) yield neighbour+:path }
    }

    //Check Introversion percentage threshold after the paths have reached a third of their total length.
    //This is non-optimal for two reasons 1) we might drop a vertex too early 2) we might do wasted calculations
    //On the lookout for a better solution
    val perIntro = if(totalProbability>0) { totalIntroversion/totalProbability } else { 0F }
    if(newPaths.nonEmpty && (newPaths.head.size<=(patterns.pathLength+3)/3 && perIntro<maxIntro)) {
      calculateIntroversion(newPaths, totalIntroversion, totalProbability)(vNeighboursByLabel, maxIntro)
    } else { (totalIntroversion, totalProbability) }
  }

  private def getLikelihoodOfPath(path: Seq[PartitionVertex], minToBeIn: Float = probThreshold) = visitorMatrix.get(path).getOrElse {

    @tailrec def traverseLikelihood(mutual: Seq[PartitionVertex],
                                    subTrie: MutableTrieMap[PartitionVertex, Float],
                                    remaining: Seq[PartitionVertex]): Float = remaining match {
      case head::tail if mutual.nonEmpty =>
        //We want both Internal and External vertices here!
        val neighbourLabels = mutual.last.partitionVertices(Direction.BOTH).groupBy( _.getLabel )
        val labelLikelihoods = patterns.getLikelihoodsFromPattern(mutual.toLabels)
        val nextLikelihood = {
          subTrie.value.getOrElse(0F) * (labelLikelihoods.getOrElse(head.getLabel, 0F) / neighbourLabels(head.getLabel).size)
        }
        subTrie += (Seq(head) -> nextLikelihood)
        if(nextLikelihood > minToBeIn) {
          traverseLikelihood(mutual:+head, subTrie.withPrefix(Seq(head)), tail)
        } else{ 0F }
      case head::tail =>
        val labelLikelihoods = patterns.getLikelihoodsFromPattern(Seq.empty[String])
        //special casing the first step. Starting to think I shouldn't.
        val nextLikelihood = {
          subTrie.value.getOrElse(0F) * (labelLikelihoods.getOrElse(head.getLabel, 0F) /
            labelCounts.getOrElseUpdate(head.getLabel, getInternalVertices(head.getLabel).size))
        }
        subTrie += (Seq(head) -> nextLikelihood)
        if(nextLikelihood > minToBeIn) {
          traverseLikelihood(mutual:+head, subTrie.withPrefix(Seq(head)), tail)
        } else{ 0F }
      case Nil =>
        subTrie.value.getOrElse(0F)
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

    @tailrec def getCandidates(candidates: Set[PartitionVertex]): Set[PartitionVertex] = {
      val newCandidates = candidates flatMap { candidate =>
        val neighbours = candidate.intVertices(Direction.BOTH).filterNot(candidates.contains)
        neighbours.filter { n =>
          val (i, o) = n.intVertices(Direction.BOTH, candidate.getLabel).partition(candidates.contains)
          i.size > o.size
        }
      }
      if(newCandidates.nonEmpty) { getCandidates( candidates ++ newCandidates ) } else { candidates }
    }

//    @tailrec def likeLihoodThrough(paths: List[Seq[PartitionVertex]],
//                                   probabilities: (Float, Float) = (0,0))
//                                  (implicit candidates: Set[PartitionVertex]): Float = {
//      var newPaths = List.empty[Seq[PartitionVertex]]
//      var (pathProb, vProb) = probabilities
//      for(path <- paths if path.size <= patterns.pathLength) {
//        if(patterns.trie.contains(path.toLabels)) {
//          vProb += getLikelihoodOfPath(path)
//          pathProb += getLikelihoodOfPath(path.init)
//        }
//        newPaths ++= { for(neighbour <- path.head.intVertices(Direction.BOTH).toList) yield neighbour+:path }
//      }
//      if(newPaths.nonEmpty && pProportion < 0.5F) { likeLihoodThrough(paths, pProportion) } else { pProportion }
//    }

    //get candidate family members for v
    var family = Set(v)
    //TODO: Fix with proper getFamily
    family = family ++ getCandidates(family)
    //If a vertex only has neighbours already in family, then add it to family.
    //TODO: Do better with some kind of closed loop detection (hard)
    family ++ (family flatMap(_.intVertices(Direction.BOTH).filter(_.intVertices(Direction.BOTH).forall(family.contains))))

  }

  private def shouldAccept(potential: PartitionVertex, loss: Float, maxIntro: Float): Boolean = getExternalVertex(potential.getId) match {
    case Some(external) =>
      val neighboursByLabel = external.partitionVertices(Direction.BOTH).groupBy[String](_.getLabel)
      val internalNeighboursByLabel = neighboursByLabel.mapValues { vItr =>
        (vItr.filter(_.getProperty[Int]("__external") == id), vItr.size)
      }
      val introversion = calculateIntroversion(List(Seq(external)))(internalNeighboursByLabel, maxIntro)
      //We're comparing the introversion lost in the old partition, to the introversion gained in the new.
      introversion._1 > loss
    case _ => false
  }

}

sealed trait Dynamic {
  this: Partition =>

  private[partition] def send(vertex: PartitionVertex, destination: Partition): Unit = {

    //Handle vertex having left
    //Check if any external vertices that potential was connected to now have no Internal (to this partition!) neighbours
    //If any exist, then they should be deleted.
    vertex.partitionVertices(Direction.BOTH).partition(n => Option(n.getProperty[Int]("__external")).isEmpty) match {
      case (Nil, Nil) => removeVertex(vertex)
      case (Nil, externals) =>
        removeVertex(vertex)
        //If an external neighbour no longer has any neighbours here, delete it.
        externals.filter(_.wrapped.getVertices(Direction.BOTH).asScala.isEmpty).foreach(removeExternalVertex)
      case (internals, externals) =>
        //If an external neighbour only has one neighbour here (vertex), delete it.
        externals.filter(_.wrapped.getVertices(Direction.BOTH).asScala.toSet.size == 1).foreach { v =>
          removeExternalVertex(v)
        }
        vertex.setProperty("__external", destination.id)
        vertexIdMap.remove(vertex.getId)
        extVertexIdMap(vertex.getId) = vertex.wrapped.getId
    }
  }

  //TODO: Run through again to catch dup Id. Run again with if check for id swap, work out when vertex gets missed for cleanup.
  private[partition] def receive(v: PartitionVertex): PartitionVertex = {
    val (newVertex, newExternals) = getExternalVertex(v.getId) match {
      case Some(external) =>
        //Remove the external property and re-wrap the vertex as Internal
        external.removeProperty[Int]("__external")
        val ex = PartitionVertex(PartitionVertex.unwrap(external), external.getId, this)
        extVertexIdMap.remove(external.getId)
        vertexIdMap(ex.getId) = ex.wrapped.getId
        //Get neighbours of potential and do Set disjoint with neighbours of it's external counterpart.
        //The resulting set are new "external" neighbours. Create them (with properties etc...)
        val out = (v.getPartitionEdges(Direction.OUT).toSet &~ external.getPartitionEdges(Direction.OUT).toSet).map((_, Direction.OUT))
        val in = (v.getPartitionEdges(Direction.IN).toSet &~ external.getPartitionEdges(Direction.IN).toSet).map((_, Direction.IN))
        (ex, in ++ out)
      case None =>
        val newV = addVertex(v)
        newV.removeProperty[Int]("__external")
        (newV , (v.getPartitionEdges(Direction.OUT).map((_, Direction.OUT)) ++
          v.getPartitionEdges(Direction.IN).map((_, Direction.IN))).toSet )
    }
    //TODO: replace with implicit "from" param
    val from = v.partition.id
    newExternals.foreach { case (edge, direction) =>
      //addEdge should handle vertex copying and externalisation for us.
      val newEdge = if (direction == Direction.OUT) {
        val (inV, ext) = getExternalVertex(edge.in.getId).map((_, None)).getOrElse((edge.in, Some((edge.in, from))))
        addEdge(newVertex, inV, edge.getLabel, ext)
      } else {
        val (outV, ext) = getExternalVertex(edge.out.getId).map((_,None)).getOrElse((edge.out, Some((edge.out, from))))
        addEdge(outV, newVertex, edge.getLabel, ext)
      }
      ElementHelper.copyProperties(edge, newEdge)
    }
    newVertex
  }

}

/** */
object Partition {
  def apply(subGraph: Graph,
            parent: => PartitionedGraph,
            vertexIdMap: mutable.Map[Identifier, Identifier],
            edgeIdMap: mutable.Map[Identifier, Identifier],
            extVertexIdMap: mutable.Map[Identifier, Identifier],
            id: Int): Partition = {
    new Partition(subGraph, parent, vertexIdMap, edgeIdMap, extVertexIdMap, id)
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
                         val extVertexIdMap: mutable.Map[Identifier, Identifier],
                         val id: Int) extends Dynamic with Refinable {

  lazy private[partition] val parent = parentGraph

  def shutdown(): Unit = this.subGraph.shutdown()

  /** Takes a global vertex id - looks it up locally, and then retrieves and returns the associated vertex
    *
    *
    * @param globalId the global Long id associated with the desired vertex
    * @return the vertex specified by Long id
    */
  def getVertex(globalId: Identifier): Option[PartitionVertex] = {
    val localId = this.vertexIdMap.get(globalId)
    val vertex = localId.flatMap { vId => Option(this.subGraph.getVertex(vId)) }
    if(vertex.isDefined) { Some(PartitionVertex(vertex.get, globalId, this)) } else { None }
  }

  /** Takes a global vertex id - looks it up locally, and then retrieves and returns the associated External vertex
    *
    *
    * @param globalId the global Long id associated with the desired vertex
    * @return the vertex specified by Long id
    */
  protected def getExternalVertex(globalId: Identifier): Option[PartitionVertex] = {
    val localId = this.extVertexIdMap.get(globalId)
    val vertex = localId.flatMap { vId => Option(this.subGraph.getVertex(vId)) }
    if(vertex.isDefined) { Some(PartitionVertex(vertex.get, globalId, this)) } else { None }
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

  protected def removeExternalVertex(vertex: PartitionVertex): Unit = {
    if(this.extVertexIdMap.get(vertex.getId).isEmpty) { throw ExceptionFactory.vertexWithIdDoesNotExist(vertex.getId) }
    this.extVertexIdMap.remove(vertex.getId)
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
    if(vertexIdMap.get(vertex.getId).isDefined) { throw ExceptionFactory.vertexWithIdAlreadyExists(vertex.getId) }
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
  def getEdge(globalId: Identifier): Option[PartitionEdge] = {
    val localId = this.edgeIdMap.get(globalId)
    val edge = localId.map(this.subGraph.getEdge(_))
    if(edge.isDefined) { Some(PartitionEdge(edge.get, globalId, this)) } else { None }
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
    * TODO: work out if PartitionVertex unwrapping is needed here?
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
          extVertexIdMap(newVertex.getProperty[Any]("__globalId")) = newVertex.getId
          newVertex
        } else { out }
        val inVertex = if(v == in) {
          ElementHelper.copyProperties(in, newVertex)
          newVertex.setProperty("__external", i)
          extVertexIdMap(newVertex.getProperty[Any]("__globalId")) = newVertex.getId
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
