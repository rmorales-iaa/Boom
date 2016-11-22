//=============================================================================
//File: plot.scala
//=============================================================================
/** Adds gnu-plot capabilities in catSpark. See object declaration
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
package catSpark.plot

//=============================================================================
// System import section
//=============================================================================
import org.apache.spark.sql.DataFrame

import java.io.PrintWriter

//=============================================================================
// User import section
//=============================================================================
import catSpark.logger.myLogger
import catSpark.util.myUtil
import catSpark.dataFrame.myDataframe

//=============================================================================
// Class/Object implementation
//=============================================================================
/**
 * Creates a X-Y plot the file using the format of gnu-plot: [[http://gnuplot.sourceforge.net/]]
 * Factory for [[catSpark.plot.myPlot]] instances
 */
object myPlot {
  
  //-------------------------------------------------------------------------
  private val common = s"""reset
    set datafile separator ","
    set autoscale xfix
    set grid
    set ytics nomirror
    set autoscale y
    set tics out"""

  private val commonY_2_axis = s"""    set y2tics
    set autoscale y2"""

  private val plot = "plot"
  private val skip = """    skip 1 """

  private val using =   "    using 1:"
  private val plotX_Y_1_axe = """ with lines axes x1y1 """
  private val plotX_Y_2_axe = """ with lines axes x1y2 """

  private val title = s"""    title """"

  private val commonEndPlot = """bind 'e' 'replot'
pause -1"""

  private val plot_dat  = "plot.dat"

  //-------------------------------------------------------------------------
  private var writer: PrintWriter = null
  private var df: DataFrame = null
  private var rootOutputPath = ""
  private var outputFileName = ""

  //-------------------------------------------------------------------------
  //method list
  //-------------------------------------------------------------------------  
  /** Creates a X-Y plot using a set of columns of a dataframe
   * @constructor Creates a X-Y new gnu-plot file using a set of columns of a dataframe.
   * It is possible to specify a column name list for each Y axis and use up to two
   * different Y axis
   * @param _df Data sorage
   * @param _rootOutputPath Root path where to store the gnu-plot file
   * @param _outputFileName Name of the gnu-plot file
   * @param xColName Name of the column used as X-axis
   * @param y_1_ColNameList Name list of the columns used as Y-axis
   * @param y_2_ColNameList Name list of the columns used as Y2-axis. Empty if it is not used 
 	 * @return true if gnu-plot has been properly created
             or false if gnu-plot has not been properly created
  */
  def create(_df: DataFrame
    , _rootOutputPath : String
    , _outputFileName : String
    , xColName: String
    , y_1_ColNameList: Seq[String]
    , y_2_ColNameList: Seq[String]): Boolean = {

    //copy input
    df = _df
    rootOutputPath = _rootOutputPath
    outputFileName= _outputFileName

    //create file
    writer = new PrintWriter(outputFileName)
    writer.write(common+myUtil.LineSeparator)

    if(!y_2_ColNameList.isEmpty)
      writer.write(commonY_2_axis++myUtil.LineSeparator)

    //create data
    createDataAndPlot(xColName
      , y_1_ColNameList
      , y_2_ColNameList)

    //end of plot
    writer.write(commonEndPlot++myUtil.LineSeparator)
    writer.close()

    true
  }

  //-------------------------------------------------------------------------
  /** Creates a data file with all columns used in the gnu-plot file and also the 
   *  gnu-plot file itself
   * @param xColName Name of the column used as X-axis
   * @param y_1_ColNameList Name list of the columns used as Y-axis
   * @param y_2_ColNameList Name list of the columns used as Y2-axis. Empty if it is not used 
  */
   def createDataAndPlot( xColName: String
    , y_1_ColNameList: Seq[String]
    , y_2_ColNameList: Seq[String]) = {

    val all = Seq(xColName) ++ y_1_ColNameList ++ y_2_ColNameList

    all.map { name =>
      def newDF = myDataframe.filterByColName(df, all)._2.orderBy(xColName)
      myDataframe.saveAsCSV(newDF
        ,rootOutputPath+plot_dat
        , true   //generateHeader
        , ','    //itemDivider
        , "#")   //commentHeaderPrefix
    }

    plot_y(y_1_ColNameList
      , y_2_ColNameList)
  }

  //-------------------------------------------------------------------------   
  /** Creates the Y-axis of the gnu-plot file based on a sequence of names of columns
   * @param list_y_1 Name list of the columns used as Y-axis
   * @param list_y_2 Name list of the columns used as Y2-axis. Empty if it is not used 
  */
  def plot_y(list_y_1: Seq[String]
  , list_y_2: Seq[String]) = {

    writer.write(plot+" \\"+myUtil.LineSeparator)

    plot_line(list_y_1
      , plotX_Y_1_axe
      , 0)

    if (!list_y_2.isEmpty){

      writer.write(", \\"+myUtil.LineSeparator)

      plot_line(list_y_2
      , plotX_Y_2_axe
      , list_y_1.length)
    }

    writer.write(myUtil.LineSeparator)
    writer.write(myUtil.LineSeparator)
  }
  
  //-------------------------------------------------------------------------
	/** Creates the plot line for an axis in the gnu-plot file 
   * @param nameList Name list of the columns of the plot
   * @param axe Name of the axis
   * @param columnPos Position of the column in the data file
  */
  def plot_line(nameList: Seq[String]
    , axe: String
    , columnPos: Int) = {

    for (index <- 0 until nameList.length){

      writer.write(s"""    "$plot_dat"""")
      writer.write(skip)
      writer.write(using+(columnPos+index+2)+axe)

      writer.write(title
        + nameList(index)+"\"")

      if(index != nameList.length -1)
        writer.write(", \\"+myUtil.LineSeparator)
    }
  }

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'myPlot'
//=============================================================================
// End of 'plot.scala' file
//=============================================================================
