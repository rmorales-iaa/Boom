//=============================================================================
//File: gexf.scala
//=============================================================================
/** It manages the graph file format Gexf in catSpark. See class declaration
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
import org.apache.spark.graphx._

import java.io.PrintWriter

//=============================================================================
// User import section
//=============================================================================
import catSpark.util.myUtil

//=============================================================================
// Class/Object implementation
//=============================================================================

//-------------------------------------------------------------------------
  /** Implements useful methods to manage the graph file format gexf: [[https://gephi.org/gexf/format/]]
   * . Those method are based on the book: "Spark graphx in action" by Michael S. Malak and Robin East.
   * . Graphs files generated can be visualized with Gephi: [[https://gephi.org]]
   * @constructor Creates an new 'myGexf' with an author, description and type
   * @param author Person that creates the graph
   * @param description Description of the content of the graph
   * @param directed True for 'directed' graph or false for indirected
   */
case class myGexf(author: String
  ,description: String
  ,directed: Boolean){

  //-------------------------------------------------------------------------
  private val date = myUtil.getTimeStamp()
  private val typeOf = if (directed) "directed" else "undirected"

  //-------------------------------------------------------------------------
  private val gexfHeader = s"""<?xml version="1.0" encoding="UTF-8"?>
<gexf xmlns="http://www.gexf.net/1.2draft"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.gexf.net/1.2draft
                      http://www.gexf.net/1.2draft/gexf.xsd"
  version="1.3">
  <meta lastmodifieddate="$date">
    <creator>$author</creator>
    <description>$description</description>
  </meta>\n"""

  //-------------------------------------------------------------------------
  //see https://gephi.org/gexf/format/viz.html
  private val gexfHeaderVisualization = s"""<?xml version="1.0" encoding="UTF-8"?>
<gexf xmlns="http://www.gexf.net/1.2draft"
  xmlns:viz="http://www.gexf.net/1.1draft/viz"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.gexf.net/1.2draft
                      http://www.gexf.net/1.2draft/gexf.xsd"
  version="1.3">
  <meta lastmodifieddate="$date">
    <creator>$author</creator>
    <description>$description</description>
  </meta>\n"""

  //-------------------------------------------------------------------------
  private val gexfFooter = "</gexf>"
 //-------------------------------------------------------------------------

  private val graphNodeHeader = s"""  <graph mode="static" defaultedgetype="$typeOf">\n"""
  private val graphNodeFooter = "  </graph>\n"
  //-------------------------------------------------------------------------
  /** Saves a graph into a file using gexf format
   *  Based on book "Spark graphx in action", section 4.16
   * @param g Graph vertexes list with type VD and a edges list with type 'ED'
   * @param fileName Name of the file to be saved
   */
  def save[VD,ED](g:Graph[VD,ED]
    ,fileName: String) = {

    val finalFileName = myUtil.ensureFileExtension(fileName,
                                                   myUtil.GexfFileExtension)

    val writer = new PrintWriter(finalFileName)

    writer.write(gexfHeader)
    writer.write(graphNodeHeader)

    //vertex list
    writer.write("    <nodes>\n" +
      g.vertices.map(v =>
                       "      <node id=\""
                       + v._1 + "\" label=\""
                       + v._2 + "\" />\n").collect.mkString
      +"    </nodes>\n")

    //edge list
    writer.write("    <edges>\n" +
      g.edges.map(e =>
                    "      <edge source=\"" + e.srcId
                    + "\" target=\"" + e.dstId
                    + "\" label=\"" + e.attr
                    + "\" />\n").collect.mkString
      +"    </edges>\n")

    writer.write(graphNodeFooter)
    writer.write(gexfFooter)
    writer.close
  }

  //-------------------------------------------------------------------------
  /** Saves a graph into a file using gexf format
   * @param g Graph using [[myGraph.vertex_ND]] as the type of vertexes list 
   * and double as type of edges list
   * @param dimension Number of dimensions of the graph: 2 or 3
   * @param scale Scale of the graph
   * @param fileName Name of the file to be saved
   */                         
  def saveND_Position(g:Graph[myGraph.vertex_ND,Double]
    , dimension: Int
    , scale:Double
    , fileName: String) = {

    val finalFileName = myUtil.ensureFileExtension(fileName,
                                                   myUtil.GexfFileExtension)
    val writer = new PrintWriter(finalFileName)

    writer.write(gexfHeaderVisualization)
    writer.write(graphNodeHeader)

    //vertexes list
    writer.write("    <nodes>\n" +
      g.vertices.map(v =>{
                       var yzDimension = "\" y=\"" + v._2.valueList(1) * scale
                       if (dimension == 3) yzDimension += "\" z=\"" + v._2.valueList(2) * scale
                       yzDimension += "\" />\n"

                       "      <node id=\"" +
                       v._1 + "\" label=\"" + v._2.label + "\">\n" +
                       "        <viz:position x=\"" + v._2.valueList(0) * scale +
                       yzDimension +
                       "      </node>\n"}).collect.mkString
      + "    </nodes>\n")
    //edge list
    writer.write("    <edges>\n" +
      g.edges.map(e =>
                    "      <edge source=\"" + e.srcId
                    +"\" target=\"" + e.dstId
                    + "\" label=\"" + e.attr
                    +"\" />\n").collect.mkString
      +"    </edges>\n")

    writer.write(graphNodeFooter)
    writer.write(gexfFooter)
    writer.close
  }

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of class 'myGexf'
//=============================================================================
// End of 'gexf.scala' file
//=============================================================================
