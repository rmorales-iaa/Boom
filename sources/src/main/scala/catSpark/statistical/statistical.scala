//=============================================================================
//File: statistical.scala
//=============================================================================
/** It implements some basic statistics in catSpark. See object declaration
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
package catSpark.statistical

//=============================================================================
// System imports
//=============================================================================
import java.io.PrintWriter
import java.sql.{DriverManager, Connection}

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Matrix,Vector}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext,DataFrame}

//=============================================================================
// User imports
//=============================================================================
import catSpark.graph.{myGexf,myGraph}
import catSpark.logger.myLogger
import catSpark.dataFrame.myDataframe
import catSpark.util.myUtil

//=============================================================================
// Class/Object implementation
//=============================================================================
/** Utilities to calculate basic descriptive measures of a dataframe */
object myStat {

  //-------------------------------------------------------------------------
  //method list
  //-------------------------------------------------------------------------
  /** Calculates a set of  basic descriptive measures for each column of a dataframe:
   * count, mean, standard deviation, min, max and correlation (regarding to remain columns).
   * The result is shown in the console and in a output file.  
   * @param data Dataframe to process
   * @param fileName Name of the file to store the output
   * @param useCSV True is the output file has the CSV format, false for TSV 
   */
  def basicStat(data: DataFrame
    , fileName: String
    , useCSV: Boolean) = {

    //to console
    data.describe().show

    //to a file
    val conf = myUtil.getConfFileNameFormat(fileName)

    myDataframe.saveAsCSV(data.describe()
      ,conf._1
      ,true     //generateHeader: Boolean
      ,conf._2) //item divider
  }
  
  //-------------------------------------------------------------------------
  /** Calculates the statistic correlation between all columns of a row list
   * Based on [[https://github.com/sequenceiq/sequenceiq-samples/tree/master/spark-correlation]]
   * @param data List of data rows
   * @param typeOfCorr Type of the correlation to calculate: "pearson" or "spearman"
   * @param decimalCount Number of decimals of the correlation values 
   */
  def correlation(data: RDD[Vector]
    , typeOfCorr: String
    , decimalCount: Int = 3): Array[(Int, Int, Double)] = {

    var corr = Statistics.corr(data
      , typeOfCorr)

    //get the upper triangular matrix without diagonal
    val num = corr.numRows
    return for ((x, i) <- corr.toArray.zipWithIndex if (i / num) < i % num)
      yield ((i / num), (i % num), myUtil.formatDouble(x, decimalCount))
  }

  //-------------------------------------------------------------------------
  /** Calculates the statistic correlation between all columns of a row list,
   * showing the result in the console. It also generates a graph file in gexf format
   * [[catSpark.graph.myGexf]] where the vertexes are the name of the 
   * columns and the edges corresponding to correlation values between two columns
   * Based on [[https://github.com/sequenceiq/sequenceiq-samples/tree/master/spark-correlation]]
   * @param sc Spark context
   * @param data RDD List of data rows
   * @param typeOfCorr Type of the correlation to calculate: "pearson" or "spearman"
   * @param outputFileName Name of the output graph file
   * @param decimalCount Number of decimals of the correlation values
   */
  def correlationWithNameAndGraph(sc: SparkContext
    , data: RDD[Vector]
    , name: Array[String]
    , typeOfCorr: String
    , outputFileName: String
    , decimalCount: Int = 3) = {

    val corr = correlation(data
      , typeOfCorr
      , decimalCount)

    //to console
    corr.map { t => myLogger.info(typeOfCorr+" correlation: ("+name(t._1)+","+name(t._2)+","+t._3+")") }

    //to a graph file
    correlationAsGraph(sc
    , corr
    , name
    , typeOfCorr
    , outputFileName)
  }

  //-------------------------------------------------------------------------
  /** Generates a graph file in gexf format [[catSpark.graph.myGexf]] where 
   *  the vertexes are the name of the columns and the edges corresponding 
   *  to correlation values between two columns
   * @param sc Spark context
   * @param corr Edge list: source, destination and correlation value
   * @param vertexList Vertexed list: name list of the columns
   * @param typeOfCorr Type of the correlation to calculate: "pearson" or "spearman"
   * @param outputFileName Name of the output graph file
   */
  def correlationAsGraph(sc: SparkContext
    , corr: Array[(Int, Int, Double)]
    , vertexList: Array[String]
    , typeOfCorr: String
    , outputFileName: String) = {

    //configure the graph output
    val gexf = new myGexf("Generated automatically by Boom!" //author
    , s"$typeOfCorr correlation matrix between all columns of an user table" //description
    , false) //directed

    //create and save the graph
    val fileName = myUtil.ensureFileExtension(outputFileName
                     , myUtil.GexfFileExtension)
    gexf.save(myGraph.build(sc
                            , vertexList
                            , corr)
      ,fileName)
    myLogger.info(s"File generated: '$fileName'")
  }

  //-------------------------------------------------------------------------
  /** Applies the parallelized no supervised clustering algorithm K-means to input data 
   *  generating a graph file in gexf format [[catSpark.graph.myGexf]] with the result
   * @param sc Spark context
   * @param data RDD List of data rows
   * @param k Number of cluster to divide the data
   * @param iteration Number of iterations in the k-means algorithm
   * @param run Number of executions of the k-means algorithm
   * @param epsilon Margin in the k-means algorithm
   * @param fileName Name of the output graph file
   * @param header First line of the header 
   */
  def kMeans(sqlContext: SQLContext
    , data: RDD[Vector]
    , k: Int
    , iteration: Int
    , run: Int
    , epsilon: Double
    , fileName: String
    , header: Seq[String]) = {

    //assuming random seed for cluster initialization
    val kmeans = new KMeans()
      .setK(k)
      .setMaxIterations(iteration)
      .setRuns(run)
      .setEpsilon(epsilon)

    val seed = kmeans.getSeed
    myLogger.info(s"Using $seed as random seed for cluster initialization")

    //K-means is applied on each column
    //using random initialization of cluster centroid
    val model = kmeans.run(data)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    // cost is the sum of squared distances of points to their nearest center
    // Optimal number of clusters is indicated as an elbow in within set sum of squared errors (WSSSE)
    val WSSSE = model.computeCost(data)
    myLogger.info("Kmeans||. Set sum of squared errors(increasing cluster count reduce this error): " + WSSSE)

    //list clusters and its members
    val clusterList = model.clusterCenters

    myLogger.info("Kmeans||. Cluster centers: ")
    var clusterID = 0
    for (cluster <- clusterList) {
      println(s"$clusterID =>" + cluster)
      clusterID += 1
    }

    //list data and in which cluster was assigned (last value)
    var clusterIDMap = data.map { x => model.predict(x) }
    val clusterMap = data.zip(clusterIDMap)
    myLogger.info("Kmeans||. Cluster assigments by value: ")

    //to console
    clusterMap.foreach(println)

    //to file
    val conf = myUtil.getConfFileNameFormat(fileName)
    val df = sqlContext.createDataFrame(clusterMap.map(x=>(x._1(0),x._1(1),x._2)))
         
    myDataframe.saveAsCSV(df.toDF(header: _*)
      ,conf._1  //filename
      ,true    //generateHeader: Boolean
      ,conf._2) //item divider
  }

  //------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'myStat'
//=============================================================================
// End of 'statistical.scala' file
//=============================================================================
