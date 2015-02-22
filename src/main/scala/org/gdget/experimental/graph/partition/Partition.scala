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

sealed trait Refinable {
  this: Partition =>

  private[partition] val visitorMatrix = MutableTrieMap[PartitionVertex, Float](Seq.empty[PartitionVertex] -> 1F)
  private val minToBeInThreshold = 0.001F
  private val maxIntroversionThreshold = 0.2F
  private lazy val patterns = parent.traversalSummary
  private val labelCounts = mutable.Map.empty[String, Int]

  //TODO: Replace the following with proper akka code
  //TODO: Check we're not returning "internal" vertices from external partitions?
  //  In most cases this can be easily achieved by making sure that we only get non-external neighbours from internal
  //  vertices. In this way external vertices will act as a "barrier" between partitions.
  //  A better long term solution would be to insure true vertex-cut partitioning. Its semantically clearer than what
  //  we have now.  I think we actually already have this and I am an idiot ... ='( long day
  @tailrec
  final def attemptSwap(potential: PartitionVertex, probability: Float, destinations: List[Partition]): Unit = destinations match {
    case head :: tail if head.shouldAccept(potential, probability-getLikelihoodOfPath(Seq(potential))) =>
      //Handle vertex having left
      //Check if any external vertices that potential was connected to now have no Internal (to this partition!) neighbours
      //If any exist, then they should be deleted.
      val externalNeighbourSet = potential.getPartitionVertices(Direction.BOTH).collect({ case e: External => e }).toSet
      val remove = for{
        removable <- externalNeighbourSet
        neighbours = removable.wrapped.getVertices(Direction.BOTH).asScala.toSet
        internalNeighbours = neighbours.filter( n => Option(n.getProperty[Int]("__external")).isEmpty )
        if internalNeighbours.size < 2
      } yield { removable }
      remove.foreach(this.removeExternalVertex)
      potential.setProperty("__external", head.id)
      vertexIdMap.remove(potential.getId)
      extVertexIdMap(potential.getId) = potential.wrapped.getId
    case head :: tail => attemptSwap(potential, probability, tail) //If partition doesn't want it then try next best bet
    case Nil => println("Swap failed completely")
  }

  def getPotentialDestPartitions(v: PartitionVertex): List[Partition] = {
    //Get vertex's neighbourhood of partitions
    val neighbourhood = v.getPartitionVertices(Direction.BOTH).collect({ case e: External => e })
      .groupBy( _.getProperty[Int]("__external") ).mapValues(_.size)
    val preference: List[(Int,Int)] = neighbourhood.toList.sortBy({ case (partitionId, incidence) => - incidence })
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
      neighboursByLabel = neighbours.groupBy[String](_.getLabel).mapValues(vItr => (vItr.collect({ case i: Internal => i }), vItr.size))
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
                                   (vNeighboursByLabel: Map[String, (Iterable[PartitionVertex], Int)],
                                    maxIntroversion: Float = maxIntroversionThreshold): (Float, Float) = {
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
          val neighbourLabels = mutual.last.getPartitionVertices(Direction.BOTH).groupBy( _.getLabel )
          val labelLikelihoods = patterns.getLikelihoodsFromPattern(mutual.toLabels)
          val nextLikelihood = {
            subTrie.value.getOrElse(0F) * (labelLikelihoods(head.getLabel) / neighbourLabels(head.getLabel).size)
          }
          subTrie += (Seq(head) -> nextLikelihood)
          if(nextLikelihood > minToBeIn) {
            traverseLikelihood(mutual:+head, subTrie.withPrefix(Seq(head)), tail)
          } else{ 0F }
        case head::tail =>
          val labelLikelihoods = patterns.getLikelihoodsFromPattern(mutual.toLabels)
          //special casing the first step. Starting to think I shouldn't.
          val nextLikelihood = {
            subTrie.value.getOrElse(0F) * (labelLikelihoods(head.getProperty[String]("__label")) /
              labelCounts.getOrElseUpdate(head.getLabel, getInternalVertices(head.getLabel).size))
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

  private def likelihoodThroughNeighbour(v: PartitionVertex)(n: PartitionVertex): Float = {
    val neighbours = v.getPartitionVertices(Direction.BOTH)
    if(neighbours.exists( _ == n ) && patterns.contains((n+:Seq(v)).toLabels)) {
      //get the probability of n->v
      val probability = getLikelihoodOfPath(n+:Seq(v))

      //if probability is greater than
    }
    //    if(neighbours.exists(_ == neighbour) && patterns.contains((path:+neighbour).toLabels)) {
    //      var likelihood = 0F
    //      likelihood += if(patterns.contains((path:+neighbour).map( _.getLabel ))) {
    //        //Get the likelihood associated with path
    //        val pathLikelihood = getLikelihoodOfPath(path)
    //        //Get the likelihoods associated with each possible subsequent label (internal and external) in the pathLabels sequence
    //        val labelLikelihoods = patterns.getLikelihoodsFromPattern(pathLabels)
    //        val absentLabels = labelLikelihoods.keySet &~ vNeighboursByLabel.keySet
    //        //Uniformly distribute the likelihood of each label across each of the neighbours of v (internal and external) with that label
    //        //Then assign their appropriate share to internal labels.
    //        val transitionsFromPath: Map[PartitionVertex, Float] = for {
    //          (label, labelledVertices) <- vNeighboursByLabel
    //          vIntNeighbour <- labelledVertices if Option(vIntNeighbour.getProperty[Int]("__external")).isEmpty
    //          likelihood = labelLikelihoods.getOrElse(label, 0F) / labelledVertices.size
    //          effectiveLikelihood = likelihood*pathLikelihood
    //        } yield { (vIntNeighbour, effectiveLikelihood) }
    //      }
    //    } else { 0F }
    ???
  }

  //TODO: Add checking of internal neighbours of v to see if any of them come with us, work out how that will work.
  // At the moment this is kind of a flood fill approach, but what is the threshold and why?
  // This is really tricky - I have spent some time trying to think of a solution, will do more soon.
  private def getFamily(v: PartitionVertex): Set[PartitionVertex] = {
    val internalNeighbours = v.getPartitionVertices(Direction.BOTH).collect({ case i: Internal => i })
      .groupBy(_.getLabel)
    val vLabel = v.getProperty[String]("__label")
    val validLabels = internalNeighbours.keySet.filter { p => patterns.contains(Seq(p,vLabel)) }
    // pass 0 up the line for each neighbour, up to path limit. As it accrues probability check if it passes threshold.
    // If it does then it is part of the family. The first generation family is the input to the second generation.
    ???
  }

  //TODO: Calculate gain *after* we have updated neighbours (somehow) otherwise its always the same value.
  def shouldAccept(potential: PartitionVertex, loss: Float): Boolean = getExternalVertex(potential.getId) match {
    case Some(external) =>
      val neighbours = external.getPartitionVertices(Direction.BOTH)
      val neighboursByLabel =
        neighbours.groupBy[String](_.getLabel)
      val internalNeighboursByLabel = neighboursByLabel.mapValues(vItr => (vItr.filter(_.getProperty[Int]("__external") == this.id), vItr.size))
      val introversion = calculateIntroversion(List(Seq(external)))(internalNeighboursByLabel, 0.5F)
      //We're comparing the introversion lost in the old partition, to the introversion gained in the new.
      val gain = introversion._1
      if(gain > loss) {
        val newV = receive(potential)
        println("Swap succeeded for vertex "+ potential+", from partition "+potential.partition.id+" to partition " +
          this.id+". Vertex is now "+newV)
        true
      } else { false }
    case _ => false
  }

  private def receive(vertex: PartitionVertex): Option[PartitionVertex] = this.getExternalVertex(vertex.getId) map { external =>
    //Remove the external property and re-wrap the vertex as Internal
    val offering = external.removeProperty[Int]("__external")
    val newVertex = PartitionVertex(PartitionVertex.unwrap(external), external.getId, this)
    extVertexIdMap.remove(external.getId)
    vertexIdMap(newVertex.getId) = newVertex.wrapped.getId
    //Get neighbours of potential and do Set disjoint with neighbours of it's external counterpart.
    //The resulting set are new "external" neighbours. Create them (with properties etc...)
    val newExternals: Set[(PartitionEdge, String)] = (vertex.getPartitionEdges(Direction.OUT).toSet &~
      newVertex.getPartitionEdges(Direction.OUT).toSet).map( (_, "Out") )  ++
      (vertex.getPartitionEdges(Direction.IN).toSet &~
        newVertex.getPartitionEdges(Direction.IN).toSet).map( (_, "In") )

    newExternals.foreach { case (edge, direction) =>
      //addEdge should handle vertex copying and externalisation for us.
      val newEdge = if (direction == "Out") {
        val (inV, ext)= getExternalVertex(edge.in.getId).map( (_, None) ).getOrElse( (edge.in, Some((edge.in, offering))) )
        val newEdge = addEdge(newVertex, inV, edge.getLabel, ext)
        extVertexIdMap(newEdge.in.getId) = newEdge.in.wrapped.getId
        newEdge
      } else {
        val (outV, ext) = getExternalVertex(edge.out.getId).map( (_,None) ).getOrElse( (edge.out, Some((edge.out, offering))) )
        val newEdge = addEdge(outV, newVertex, edge.getLabel, ext)
        extVertexIdMap(newEdge.out.getId) = newEdge.out.wrapped.getId
        newEdge
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
                         val id: Int) extends Refinable {

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
