//=============================================================================
//File: boom.scala
//=============================================================================
/** It implements Boom!: "Big data on our M starts!". See class declaration
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
package boom

//=============================================================================
// System import section
//=============================================================================
import java.nio.file.{ Paths, Files }

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.linalg.{ Vector }
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.{ SQLContext, DataFrame }

//=============================================================================
// User import section
//=============================================================================
import catSpark.logger.myLogger
import catSpark.statistical.myStat
import catSpark.sql.{ mySQL, myJDBC }
import catSpark.dataFrame.myDataframe
import catSpark.configuration.myConf
import catSpark.util.myUtil
import catSpark.graph.myGraph
import catSpark.graph.myGexf
import catSpark.plot.myPlot

//=============================================================================
// Class/Object implementation
//=============================================================================

//-------------------------------------------------------------------------
// Class declaration
//-------------------------------------------------------------------------

/** BOOM! (Big data On Our M starts) is a application to process data from M-starts using CatSpark tool,
 *   providing: basic statistical descriptive measures,
 *   data clustering, data spatial distribution and data plot visualization
 */
case class Boom() {

  //-------------------------------------------------------------------------
  // Variable declaration
  //-------------------------------------------------------------------------

  //contexts
  /** Spark configuration of the class */
  private var sparkConf: SparkConf = _
  
  /** Spark context of the class */
  private var sparkContext: SparkContext = _
  
  /** SQL context of the class */
  private var sqlContext: SQLContext = _
  
  /** True if initialization has finished */
  private var initializationDone = false

  //dataSets, RDD, column names and object names
  /** [[org.apache.spark.sql.DataFrame]] associated to user input data */
  private var data: DataFrame = _
  
  /** [[org.apache.spark.mllib.linalg.Vector]] associated to user input data */
  private var dataRDD: RDD[Vector] = _ //it is the data stored as a list of rows (RDD)

  /** List of names of objects of user input data */
  private var objectNameList: Array[String] = _
  
  /** List of names of columns of user input data */
  private var colNameList: Array[String] = _

  //output directory
  
  /** Root of the output directory */
  private var outputRootDirectory: String = _
  
  /** Current output directory */
  private var outputDirectory: String = _
  
  /** Root of the user's input Parquet directory */
  private var inputParquetDirectory: String = _

  //-------------------------------------------------------------------------
  // Method declaration
  //-------------------------------------------------------------------------

  /** Configures the computation cluster (local or remote) according with configuration file
    @param suffix Suffix added to the name of the task reported in the Spark report web page
    @return true if cluster has been properly configured
            or false if cluster has not been properly configured 
  */
  private def configureCluster(suffix: String): Boolean = {

    try { //configure the parameters according to the configuration file
      val runInRemote = myConf.getBoolean("Boom.computationCluster.runInRemote")
      if (runInRemote) {

        val remoteURL = myConf.getString("Boom.computationCluster.remote.url")
        val corePerExecutor = myConf.getString("Boom.computationCluster.remote.corePerExecutor")
        val memPerExecutor = myConf.getString("Boom.computationCluster.remote.memoryPerExecutorGB") + "g" //g is required in the format

        sparkConf = new SparkConf()
          .setMaster(remoteURL)
          .set("spark.executor.cores", corePerExecutor)
          .set("spark.executor.memory", memPerExecutor)

        myLogger.info(s"Running in REMOTE CLUSTER: $remoteURL corePerExecutor:$corePerExecutor memPerExecutor:$memPerExecutor")
      } else { //local

        val localURL = "local[" + myConf.getString("Boom.computationCluster.local.totalCore") + "]"
        val memPerExecutor = myConf.getString("Boom.computationCluster.local.totalMemoryGB") + "g" //g is required in the format

        sparkConf = new SparkConf()
          .setMaster(localURL)
          .set("spark.executor.memory", memPerExecutor)
          .set("spark.executor.memory", memPerExecutor)

        myLogger.info("Running in LOCAL CLUSTER: " + localURL)
      }

      //common configuration
      sparkConf.set("spark.app.name", "Boom!" + suffix)
      sparkConf.set("spark.submit.deployMode", "cluster") //run the driver in the cluster
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sparkConf.set("spark.kryoserializer.buffer", "1mb")

      val driverMemory = myConf.getString("Boom.computationCluster.driverMemoryGB") + "g" //g is required in the format
      sparkConf.set("spark.driver.memory", driverMemory) //only in cluster deploy mode

      //create the spark context,and also sql contexts
      sparkContext = new SparkContext(sparkConf)
      sqlContext = mySQL.createContext(sparkContext)

      //assign the sqlContext to the object that require it
      myJDBC.setSQL_Context(sqlContext)

      myLogger.info("Spak configuration, Spark context and SQL context created suceesfully")
    } catch {
      case e: Exception =>
        {
          myLogger.exception(e, "Error initializaiting Spark context")
          return myLogger.error("Error initializaiting Boom")
        }
    }
  }
  //-------------------------------------------------------------------------
  /** Configures the computation cluster (local or remote) according with configuration file
   *  and initialize the paths for input and output
    @param suffix Suffix added to the name of the task reported in the Spark report web page
    @return true if cluster has been properly configured
            or false if cluster has not been properly configured 
  */
  def initialize(suffix: String): Boolean = {

    //configure computation cluster
    if (!configureCluster(suffix))
      return false

    //set input of parquet directory
    inputParquetDirectory = myConf.getString("Boom.path.inputParquetDirectory")

    //create path to root output directory
    outputRootDirectory = myConf.getString("Boom.path.outputRootDirectory")
    if (!myUtil.ensureDirectoryExist(outputRootDirectory))
      return false

    //create path to final output directory
    outputDirectory = outputRootDirectory + myUtil.getTimeStamp(myUtil.TimeFormatLong) + myUtil.FileSeparator
    if (!myUtil.ensureDirectoryExist(outputDirectory))
      return false

    initializationDone = true
    myLogger.info(s"Using as input Parquet directory: '$inputParquetDirectory'")
    myLogger.info(s"Using as output directory: '$outputDirectory'")
    myLogger.info("Boom initializated sucessfully")
    myLogger.info("Spark contex created")
  }

  //-------------------------------------------------------------------------
  /** Close the Spark context */
  def close() {
    if (initializationDone) {
      sparkContext.stop()
      myLogger.info("Spark context stopped")
    }
  }

  //-------------------------------------------------------------------------
  /** Exports a list of tables of a JDBC database to Parquet format 
   *  and initialize the paths variables for input and output
    @param tableList List of tables to be exported. The scheme set in the 
    configuration file will be used
    @param parquetRootDirectory Path to store the output  
    @return true if export has success
            or false if export has failed 
  */
  private def saveJDBC_AsParquet(tableList: Seq[String], 
    parquetRootDirectory: String): Boolean = {

    //convert a scheme or a table list?
    val commonConf = "Boom.database.jdbc"
    if (tableList.size > 0)
      myJDBC.saveTableListAsParquet(myConf.getString(s"$commonConf.url")//url
      , myConf.getString(s"$commonConf.scheme")                         //scheme
      , tableList                                                       //tableList
      , myConf.getString(s"$commonConf.user")                           //user
      , myConf.getString(s"$commonConf.password")                       //pass
      , parquetRootDirectory)                                           //rootPath
    else
      myJDBC.saveSchemeAsParquet(myConf.getString(s"$commonConf.url") //url
      , myConf.getString(s"$commonConf.scheme")                       //scheme
      , myConf.getString(s"$commonConf.user")                         //user
      , myConf.getString(s"$commonConf.password")                     //pass
      , parquetRootDirectory)                                         //rootPath

    return true
  }

  //-------------------------------------------------------------------------
  /** Saves as Parquet format an input that could be:
   *  a) JDBC scheme of a database  
   *  b) Table list of a database
   *  c) Input file with plain data
   *  d) Input file with a scheme         
    @param outputDirectory Path to store the Parquet output    
    @param jdbcScheme Name of the JDBC scheme to be exported. Empty if used b),c) and d) 
    @param tableList JDBC table list to be exported. Empty if used a),c) and d)
    @param inputFile Input file to be exported. Empty if used a) and b)
    @param commaAsItemDivider Divider of the items in the input file. Not used in  a) and b) 
    @param applySchemaToInput false if no scheme is applied, true in other case. 
    @return true if export has success
            or false if export has failed  
  */
  def saveAsParquet(outputDirectory: String,     
    jdbcScheme: Boolean,
    tableList: Seq[String],
    inputFile: String,
    commaAsItemDivider: Boolean,    
    applySchemaToInput: Boolean = false): Boolean = {

    if (jdbcScheme) //data with scheme?
      return saveJDBC_AsParquet(tableList,
        outputDirectory)

    myLogger.info(s"Loading Parquet data stored in directory: '$inputFile'")
    myLogger.info(s"Saving Parquet data stored in directory: '$outputDirectory'")

    //check input file
    if (!myUtil.fileExist(inputFile))
      return myLogger.error(s"Input file does not exist:'$inputFile'")

    //create path to output directory
    if (!myUtil.ensureDirectoryExist(outputDirectory))
      return false;

    var itemDivider = myUtil.TabString
    if (commaAsItemDivider) itemDivider = myUtil.CommaString

    //save data in Parquet format
    if (applySchemaToInput)
      myDataframe.saveFileAsParquetWithScheme(sqlContext  
       , inputFile
       , outputDirectory
       , itemDivider
       , Some(star.schema)
       , Some(star.buildNestedRow))
    else
      myDataframe.saveFileAsParquet(sqlContext
       , inputFile
       , outputDirectory
       , itemDivider)

    myLogger.info(s"Saved Parquet data stored in directory: '$outputDirectory'")
  }
  //-------------------------------------------------------------------------
  /** Loads data in Parquet format into the main dataframe
    @param inputDirectory Path to the input data
    @return true if load has success
            or false if load has failed 
  */
  private def loadParquetData(inputDirectory: String): Boolean = {

    myLogger.info(s"Loading Parquet data stored in directory: '$inputDirectory'")

    if (!myUtil.directoryExist(inputDirectory))
      return myLogger.error(s"Can not load the Parquet directory: '$inputDirectory'")

    //filter the no numerical data and get the name list
    val newData = myDataframe.dropNoNumericColumnList(myDataframe.loadParquetFile(sqlContext, inputDirectory))

    data = newData._1.cache //store the numeric columns in cache for fast processing

    dataRDD = myDataframe.getAsRowList(data).cache() //store the numeric columns in cache for fast processing

    objectNameList = myDataframe.getColAsArray(newData._2).map { x => x.toString() }
    colNameList = myDataframe.getColNameList(data)

    myLogger.info(s"Loaded Parquet data stored in directory: '$inputDirectory'")
  }
  
  //-------------------------------------------------------------------------
  /** Calculates a basic descriptive statistics measures on main dataframe: 
   *  count, mean, standard deviation, min, max and correlation (regarding to remain columns)
   *  The output is displayed in the console. Also a graph file with the correlation values 
   *  is generated in gexf format ([[catSpark.graph.myGexf]]) 
    @param spearman true to use Spearman correlation , false use Pearson correlation
    @return true if statistics calculation has success
            or false if statistics calculation has failed 
  */
  def basicStat(spearman: Boolean): Boolean = {
    myLogger.info("Starting descriptive statistics on input data")

    if (!loadParquetData(inputParquetDirectory))
      return false

    //basic stat
    myStat.basicStat(data
      , outputDirectory + myConf.getString("Boom.outputConf.fileName.basicStatisticsTable")
      , myUtil.getConfUseCommaDivider)

    //correlation to console
    var typeOfCorr = "pearson"
    if (spearman) typeOfCorr = "spearman"

    val corr = myStat.correlationWithNameAndGraph(sparkContext 
      , dataRDD
      , colNameList
      , typeOfCorr
      , outputDirectory + typeOfCorr + "_" + myConf.getString("Boom.outputConf.fileName.correlationGraph"), myConf.getInt("Boom.outputConf.correlationDecimalCount"))

    myLogger.info("End of descriptive statistics on input data")
  }

  //-------------------------------------------------------------------------
  /** "Applies the parallelized no supervised clustering algorithm K-means to 
   *  main dataframe. The result is saved as a output file in CSV format
    @param clusterCount Number of cluster to divide the data
    @param iteration Number of iterations in the k-means algorithm
    @param runNumber of executions of the k-means algorithm
    @param epsilon Margin in the k-means algorithm
    @param colNameList Column name list to be used as input for k-means algorithm
    @return true if algorithm application has success
            or false if algorithm application has failed 
  */
  def kmeans(clusterCount: Int 
    , iteration: Int
    , run: Int
    , epsilon: Double
    , userColNameList: Seq[String]): Boolean = {

    myLogger.info("Start of aplying K-means|| algorithm on input data")

    if (!loadParquetData(inputParquetDirectory))
      return false

    //filter the data according to column list
    var filteredData = dataRDD
    if (userColNameList.size > 0) {
      val result = myDataframe.filterByColName(data,userColNameList)
      if (!result._1) return false
      result._2.show()       
      filteredData = myDataframe.getAsRowList(result._2).cache //store the numeric columns in cache for fast processing
    }
    
    myLogger.info("End of aplying K-means || algorithm on input data")

    val fileName = outputDirectory + myConf.getString("Boom.outputConf.fileName.kMeans")
    myStat.kMeans(sqlContext 
      , filteredData
      , clusterCount
      , iteration
      , run
      , epsilon
      , fileName
      , userColNameList :+ "cluserID") //header

    myLogger.info(s"File generated: '$fileName'")
    myLogger.info("End of aplying K-means|| algorithm on input data")
  }

  //-------------------------------------------------------------------------
  /** Applies the spatial distribution graph algorithm k-nearest neighbor up to
   *  three columns (minimum two) to the main dataframe.
   *  The result is saved as a graph in gexf format ([[catSpark.graph.myGexf]])
    @param neighborCount Number of neighbors of the algorithm  
    @param scale Scale of the output graph
    @param colNameA First column name used as input for algorithm
    @param colNameB Second column name used as input for algorithm
    @param colNameC Third column name used as input for algorithm. Empty if only two are used. 
    @return true if algorithm application has success
            or false if algorithm application has failed  
  */
  def kNearestNeighbor(neighborCount: Int
    , scale: Double
    , colNameA: String
    , colNameB: String
    , colNameC: String): Boolean = {

    myLogger.info("Start of aplying k nearest neighbor algorithm on input data")

    //load input data
    if (!loadParquetData(inputParquetDirectory))
      return false

    //filter the data according to column list
    var userColNameList = Array(colNameA, colNameB, colNameC)
    if (colNameC.isEmpty()) userColNameList = userColNameList.dropRight(1)
    var dimension = userColNameList.length

    val result = myDataframe.filterByColName(data,userColNameList)
    if (!result._1) return false
    result._2.show()
    val filteredData = myDataframe.getAsRowList(result._2).collect //store the numeric columns in cache for fast processing

    val vertexList = filteredData.zipWithIndex.map {
      case (v, i) => myGraph.vertex_ND(objectNameList(i.toInt), if (dimension == 2) Array(v(0), v(1))
      else Array(v(0)
        , v(1)
        , v(2)))
    }

    //create the graph
    val g = myGraph.kNearestNeighbor(sparkContext 
      , vertexList.toSeq
      , neighborCount)

    //configure graph
    val gexf = new myGexf("Generated automatically by Boom!" //author
    , s"K-nearest neighbor algorithm with k=$neighborCount" //description
    , true) //directed

    //save the graph
    val fileName = myUtil.ensureFileExtension(outputDirectory + myConf.getString("Boom.outputConf.fileName.kNearestNeighborGraph"), myUtil.GexfFileExtension)

    gexf.saveND_Position(g
                         , dimension
                         , scale
                         , fileName)

    //log
    myLogger.info(s"File generated: 'fileName'")
    myLogger.info("End of aplying k nearest neighbor algorithm on input data")
  }

  //-------------------------------------------------------------------------
  /** Generates a gnuplot file ([[http://gnuplot.sourceforge.net/]]) that 
   *  represents a plot with two axis (X and Y) using the main dataframe. It is possible 
   *  so select a list of columns for each Y-axis up to two different Y-axis.   
    @param xColName Column name used as X axis
    @param y_1_ColNameList Name list of the columns used as Y-axis
    @param y_2_ColNameList Name list of the columns used as Y2-axis. Empty if it is not used 
    @return true if plot has success
            or false if plot has failed 
  */
  def plot(xColName: String, 
    y_1_ColNameList: Seq[String], 
    y_2_ColNameList: Seq[String]): Boolean = {

    //log
    myLogger.info("Start of creating plot script for input data")
    myLogger.info("Using as X axis:     " + xColName)
    if (y_2_ColNameList.isEmpty)
      myLogger.info("Using as Y axis:   ")
    else {
      myLogger.info("Using as Y_1 axis: " + y_1_ColNameList.mkString(","))
      myLogger.info("Using as Y_2 axis: " + y_2_ColNameList.mkString(","))
    }

    //load input data
    if (!loadParquetData(inputParquetDirectory))
      return false

    //check input file
    if (!checkColumListExits(y_1_ColNameList)) return false
    if (!checkColumListExits(y_2_ColNameList)) return false

    //build output filename
    var fileName = outputDirectory + myConf.getString("Boom.outputConf.fileName.plot")
    fileName = myUtil.ensureFileExtension(fileName,
      myUtil.plotFileExtension)

    //create the script
    val g = myPlot.create(data, outputDirectory, fileName, xColName, y_1_ColNameList, y_2_ColNameList)

    //log
    myLogger.info(s"File generated: '$fileName'")
    myLogger.info("End of creating plot script on input data")
  }

  //-------------------------------------------------------------------------
  /** Process a user's script file and returns the list of a valid command
   *  and parameters (as defined in [[boom.myCommandLineParser]]) 
   *  ignoring comments lines.   
    @param fileName Name of the user's script to be processes
    @return null if script file does not exist
            . A list of valid command and parameters 
  */
  def processScript(fileName: String): Array[String] = {

    myLogger.info(s"Loading script file: '$fileName'")

    if (!myUtil.fileExist(fileName)) {
      myLogger.error(s"Script file: '$fileName' does not exist")
      return null
    }
    myLogger.info(s"Script file: '$fileName' loaded")

    myUtil.loadFileAndSkipComment(fileName)
  }
  
  //-------------------------------------------------------------------------
  /** Returns the column name list that are present in the main dataframe   
    @param list Column name list of the main dataframe
    @return true if all column list exist in the main dataframe
            or false if at least one column does not exist in the main dataframe 
  */
  def checkColumListExits(list: Seq[String]): Boolean = {

    list.map { col =>
      if (!colNameList.contains(col))
        myLogger.error(s"The column: '$col' does no exist")
    }

    true
  }
  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'Boom'

//=============================================================================
// End of 'boom.scala' file
//=============================================================================
