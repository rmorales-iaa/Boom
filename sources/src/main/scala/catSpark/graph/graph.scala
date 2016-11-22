//=============================================================================
//File: graph.scala
//=============================================================================
/** It implements some graph algorithms in catSpark. See object declaration
 *  @author  Rafael Morales Muñoz
 *  @mail    rmorales.iaa.es
 *  @version 1.0
 *  @date    9 Nov 2016
 *  @history None
 */
//=============================================================================
//=============================================================================
// Package section
//=============================================================================
package catSpark.graph

//=============================================================================
// System import section
//=============================================================================
import java.awt.Color

import org.apache.spark.graphx._
import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

//=============================================================================
// User import section
//=============================================================================
import catSpark.logger.myLogger
import catSpark.dataFrame.myDataframe

//=============================================================================
// Class/Object implementation
//=============================================================================
  /** Implements useful graphs algorithms. The implementation of those algorithms 
   *  are based on the book: "Spark graphx in action" by Michael S. Malak and Robin East 
   */
object myGraph{

  //-------------------------------------------------------------------------
  //method list
  //-------------------------------------------------------------------------
	/** Factory for Graph instances.
	 *  Creates a graph using a name list of vertexes and a edge list
	 * @param sc Spark contex 
   * @param v List of names of vertexes 
   * @param e List of edges
   * @return A new Graph 
   */
  def build(sc: SparkContext
    ,v:Array[String]
    ,e:Array[(Int,Int,Double)]) : Graph[String,Double] = {

      val vertexList = sc.makeRDD(for((n, i) <- v.toArray.zipWithIndex) yield (i.longValue(),n))
      val edgeList = sc.makeRDD(e.map(t=>Edge(t._1.longValue,t._2.longValue,t._3)))

      Graph(vertexList, edgeList)
  }

  //-------------------------------------------------------------------------
  /** Implements the Dijkstra algorithm: shorter paths distance including weights from a vertex
   * including paths followed.
   * Based on book "Spark graphx in action", section 6.1
   * @param g Graph with a vertexes list with type 'VD' and a edges list with type 'Double' 
   * @param origin Origin vertex used in the algorithm
   */
  def dijkstra[VD](g:Graph[VD,Double]
    ,origin:VertexId) = {

    var g2 = g.mapVertices(
    (vid,vd) => (false, if (vid == origin) 0 else Double.MaxValue,
                 List[VertexId]()))

    for (i <- 1L to g.vertices.count-1) {
      val currentVertexId =
        g2.vertices.filter(!_._2._1)
          .fold((0L,(false,Double.MaxValue,List[VertexId]())))((a,b) =>
            if (a._2._2 < b._2._2) a else b)
          ._1

      val newDistances = g2.aggregateMessages[(Double,List[VertexId])](
        ctx => if (ctx.srcId == currentVertexId)
                 ctx.sendToDst((ctx.srcAttr._2 + ctx.attr,
                                ctx.srcAttr._3 :+ ctx.srcId)),
        (a,b) => if (a._1 < b._1) a else b)

      g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) => {
        val newSumVal =
        newSum.getOrElse((Double.MaxValue,List[VertexId]()))
        (vd._1 || vid == currentVertexId,
         math.min(vd._2, newSumVal._1),
        if (vd._2 < newSumVal._1) vd._3 else newSumVal._2)})
    }

    g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
      (vd, dist.getOrElse((false,Double.MaxValue,List[VertexId]()))
        .productIterator.toList.tail))
  }

  //-------------------------------------------------------------------------
  /** Implements the travelling salesman algorithm: greddy algorithm that calculates
   *  the shortest path through an undirectional graph that hits every vertex.
   * Based on book "Spark graphx in action", section 6.2
   * @param g Graph with a vertexes list with type 'VD' and a edges list with type 'Double' 
   * @param origin Origin vertex used in the algorithm
   */
  def greedy[VD](g:Graph[VD,Double]
   ,origin:VertexId) = {

    var g2 = g.mapVertices((vid,vd) => vid == origin)
              .mapTriplets(et => (et.attr,false))
    var nextVertexId = origin
    var edgesAreAvailable = true

    do {
      type tripletType = EdgeTriplet[Boolean,Tuple2[Double,Boolean]]

      val availableEdges =
        g2.triplets
          .filter(et => !et.attr._2
                        && (et.srcId == nextVertexId && !et.dstAttr
                            || et.dstId == nextVertexId && !et.srcAttr))

      edgesAreAvailable = availableEdges.count > 0

      if (edgesAreAvailable) {
        val smallestEdge = availableEdges
          .min()(new Ordering[tripletType]() {
             override def compare(a:tripletType, b:tripletType) = {
               Ordering[Double].compare(a.attr._1,b.attr._1)
            }
          })

        nextVertexId = Seq(smallestEdge.srcId, smallestEdge.dstId)
                       .filter(_ != nextVertexId)(0)

        g2 = g2.mapVertices((vid,vd) => vd || vid == nextVertexId)
               .mapTriplets(et => (et.attr._1,
                                   et.attr._2 ||
                                     (et.srcId == smallestEdge.srcId
                                      && et.dstId == smallestEdge.dstId)))
       }
    } while(edgesAreAvailable)

    g2  //return
  }

  //-------------------------------------------------------------------------
  /** Implements the minimum spanning tree: ensure all nodes are visited at minimum cost.
   * It is equivalent to "travelling salesman" algorithm but with backtrack.
   * Based on book "Spark graphx in action", section 6.3
   * @param g Graph with a vertexes list with type 'VD' and a edges list with type 'Double' 
   * @param origin Origin vertex used in the algorithm
   */
  def minSpanningTree[VD:scala.reflect.ClassTag](g:Graph[VD,Double]) = {
    var g2 = g.mapEdges(e => (e.attr,false))

    for (i <- 1L to g.vertices.count-1) {
      val unavailableEdges =
        g2.outerJoinVertices(g2.subgraph(_.attr._2)
                               .connectedComponents
                               .vertices)((vid,vd,cid) => (vd,cid))
          .subgraph(et => et.srcAttr._2.getOrElse(-1) ==
                            et.dstAttr._2.getOrElse(-2))
          .edges
          .map(e => ((e.srcId,e.dstId),e.attr))

      type edgeType = Tuple2[Tuple2[VertexId,VertexId],Double]

      val smallestEdge =
        g2.edges
          .map(e => ((e.srcId,e.dstId),e.attr))
          .leftOuterJoin(unavailableEdges)
          .filter(x => !x._2._1._2 && x._2._2.isEmpty)
          .map(x => (x._1, x._2._1._1))
          .min()(new Ordering[edgeType]() {
            override def compare(a:edgeType, b:edgeType) = {
              val r = Ordering[Double].compare(a._2,b._2)
               if (r == 0)
                 Ordering[Long].compare(a._1._1, b._1._1)
               else
                 r
            }
           })

      g2 = g2.mapTriplets(et =>
        (et.attr._1, et.attr._2 || (et.srcId == smallestEdge._1._1
                                    && et.dstId == smallestEdge._1._2)))
    }

    g2.subgraph(_.attr._2).mapEdges(_.attr._1)
  }
  
  //-------------------------------------------------------------------------
  /** Type of vertex in N-dimensions used in k-nearest neighbors algorithm.
   *  Based on book "Spark graphx in action", section 7.4.1
   * @constructor Created a new vertex with a label and a N-Dimension double value list
   * @param label Name of the vertex 
   * @param valueList List of N-dimensions values of the vertex
   */  
  case class vertex_ND(label:String
    ,valueList:Array[Double]) extends Serializable {

    def dist(that:vertex_ND) = math.sqrt(
      valueList.zip(that.valueList).map(x => (x._1-x._2)*(x._1-x._2)).reduce(_ + _))
  }

  //-------------------------------------------------------------------------
  /** Implements k-nearest neighbors algorithm with a complexity of: n2 log n
   * Based on book "Spark graphx in action", section 7.4.1
   * @param sc Spark context 
   * @param a List of vertexes
   * @param k Number of neighbors
   */
  def kNearestNeighbor(sc: SparkContext
    ,a:Seq[vertex_ND]
    ,k:Int) : Graph[vertex_ND,Double]= {
    val a2 = a.zipWithIndex.map(x => (x._2.toLong, x._1)).toArray
    val v = sc.makeRDD(a2)
    val e = v.map(v1 => (v1._1, a2.map(v2 => (v2._1, v1._2.dist(v2._2)))
                          .sortWith((e,f) => e._2 < f._2)
                          .slice(1,k+1)
                          .map(_._1)))
              .flatMap(x => x._2.map(vid2 =>
               Edge(x._1
                    , vid2
                    , 1 / (1+a2(vid2.toInt)._2.dist(a2(x._1.toInt)._2)))))
    Graph(v,e)
  }

  //-------------------------------------------------------------------------  
  /** Implements k-nearest neighbors parallel version (Wang algorithm)
   * Based on book "Spark graphx in action", section 7.4.1
   * @param sc Spark context 
   * @param a List of vertexes
   * @param k Number of neighbors
   */
  def kNearestNeighborApprox(sc: SparkContext
    ,a:Seq[vertex_ND]
    , k:Int) = {
    val a2 = a.zipWithIndex.map(x => (x._2.toLong, x._1)).toArray
    val v = sc.makeRDD(a2)
    val n = 3
    val minMax =
      v.map(x => (x._2.valueList(0), x._2.valueList(0), x._2.valueList(1), x._2.valueList(1)))
       .reduce((a,b) => (math.min(a._1,b._1), math.max(a._2,b._2),
                         math.min(a._3,b._3), math.max(a._4,b._4)))
    val xRange = minMax._2 - minMax._1
    val yRange = minMax._4 - minMax._3

   def calcEdges(offset: Double) =
     v.map(x => (math.floor((x._2.valueList(0) - minMax._1)
                             / xRange * (n-1) + offset) * n
                    + math.floor((x._2.valueList(1) - minMax._3)
                                 / yRange * (n-1) + offset),
                  x))
       .groupByKey(n*n)
       .mapPartitions(ap => {
         val af = ap.flatMap(_._2).toList
         af.map(v1 => (v1._1, af.map(v2 => (v2._1, v1._2.dist(v2._2)))
                        .toArray
                        .sortWith((e,f) => e._2 < f._2)
                        .slice(1,k+1)
                        .map(_._1)))
           .flatMap(x => x._2.map(vid2 => Edge(x._1, vid2,
             1 / (1+a2(vid2.toInt)._2.dist(a2(x._1.toInt)._2)))))
           .iterator
        })

    val e = calcEdges(0.0).union(calcEdges(0.5))
                        .distinct
                        .map(x => (x.srcId,x))
                        .groupByKey
                        .map(x => x._2.toArray
                                   .sortWith((e,f) => e.attr > f.attr)
                                   .take(k))
                        .flatMap(x => x)

    Graph(v,e)
  }
  
  //-------------------------------------------------------------------------
  /** Remove isolated vertexes without edges 
   * Based on book "Spark graphx in action", section  8.1.1
   * @param g Graph with a vertexes list with type 'VD' and a edges list with type 'ED'  
   */
  def removeSingletons[VD:ClassTag,ED:ClassTag](g:Graph[VD,ED]) =
    Graph(g.triplets.map(et => (et.srcId,et.srcAttr))
                    .union(g.triplets.map(et => (et.dstId,et.dstAttr)))
                    .distinct,
            g.edges)

  //-------------------------------------------------------------------------
  /** Merges the vertices of two graphs
   * Based on book "Spark graphx in action", section 8.1.2
   * @param g1 Graph vertexes list with type 'String' and a edges list with type 'String'
   * @param g2 Graph vertexes list with type 'String' and a edges list with type 'String' 
   */
  def mergeGraphs(g1:Graph[String,String], g2:Graph[String,String]) = {
    val v = g1.vertices.map(_._2).union(g2.vertices.map(_._2)).distinct
                       .zipWithIndex
    def edgesWithNewVertexIds(g:Graph[String,String]) =
      g.triplets
       .map(et => (et.srcAttr, (et.attr,et.dstAttr)))
       .join(v)
       .map(x => (x._2._1._2, (x._2._2,x._2._1._1)))
       .join(v)
       .map(x => new Edge(x._2._1._1,x._2._2,x._2._1._2))

    Graph(v.map(_.swap),
        edgesWithNewVertexIds(g1).union(edgesWithNewVertexIds(g2)))
  }

  //-------------------------------------------------------------------------
  /** Obtains a measure of the connectedness of the graph
   * Based on book "Spark graphx in action", section 8.4
   * @param g Graph with a vertexes list with type 'VD' and a edges list with type 'ED'
   */
  def clusteringCoefficient[VD:ClassTag,ED:ClassTag](g:Graph[VD,ED]) = {
    val numTriplets =
      g.aggregateMessages[Set[VertexId]](
        et => { et.sendToSrc(Set(et.dstId));
                et.sendToDst(Set(et.srcId)) },
        (a,b) => a ++ b)
       .map(x => {val s = (x._2 - x._1).size; s*(s-1) / 2})
       .reduce(_ + _)

    if (numTriplets == 0) 0.0 else
      g.triangleCount.vertices.map(_._2).reduce(_ + _) /
        numTriplets.toFloat
  }
  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'myGraph'
//=============================================================================
// End of 'graph.scala' file
//=============================================================================
