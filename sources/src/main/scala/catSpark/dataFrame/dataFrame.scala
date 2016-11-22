//=============================================================================
//File: dataFrame.scala
//=============================================================================
/** It implements some utilities methods related with DataFrame in catSpark. See object declaration
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
package catSpark.dataFrame

//=============================================================================
// System import section
//=============================================================================
import com.databricks.spark.csv._

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.sql.Date;

import org.apache.spark.{SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.apache.spark.rdd.{RDD}
import org.apache.spark.mllib.linalg.{Vector,Vectors}

import org.apache.commons.csv.{CSVFormat}

import scala.collection.mutable.ListBuffer

//=============================================================================
// User import section
//=============================================================================
import catSpark.logger.myLogger
import catSpark.util.myUtil

//=============================================================================
// Class/Object implementation
//=============================================================================
/**
 * Methods to manipulate columns of a dataframe, CSV files and Parquet files 
 */
object myDataframe {

  /** CSV format used to manage CSV files */
  private var CVS_SavingFormat = CSVFormat.DEFAULT
    .withRecordSeparator(myUtil.LineSeparator)
    .withDelimiter(',')
    .withQuote('"')
    .withEscape(null)
    .withQuoteMode(null)
    .withSkipHeaderRecord(false)
    .withNullString(null)

  //-------------------------------------------------------------------------
  //method list
  //-------------------------------------------------------------------------

  //-------------------------------------------------------------------------
  /** Loads a plain data file, creates a dataframe with it, applies the provided scheme and 
   *  saves as Parquet format
    @param sqlContext SQLContext used to save the dataframe
    @param inputFileName File to be loaded
    @param outputFileName Path where save the output
    @param itemDivider Divider of the items in the input file
    @param customSchema Schema to apply to input data
    @param row_builder Transformer from input data to output data with scheme
    @return true if save has success
            or false if save has failed 
  */  
  def saveFileAsParquetWithScheme(sqlContext: SQLContext
    , inputFileName: String
    , outputFileName: String
    , itemDivider: String
    , customSchema: Option[StructType]
    , row_builder: Option[(Row) => Row]): Boolean =
    {
      try {
        //create a dataframe with the specified schema
        //the library "com.databricks.spark.csv" does not support nested structures
        //so start with a plain data
        val dataFrame = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")         // use first line of all files as header
          .option("inferSchema", "true")    // automatically infer data types
          .option("delimiter", itemDivider) // CSV or TSV
          .option("comment", "#")           // comments start with #
          .load(inputFileName);

        var finalDataFrame = dataFrame

        //apply schema
        customSchema match {
          case Some(schema) => { //to create nested schema see http://stackoverflow.com/questions/31615657/how-to-add-a-new-struct-column-to-a-dataframe
            val rowList = dataFrame.map { row_builder.get(_) }
            finalDataFrame = sqlContext.createDataFrame(rowList
                                                        , customSchema.get)
          }
          case None =>
        }
        
        //save dataframe as Parquet format     
        saveDataframeAsParquet(sqlContext
          , finalDataFrame
          , outputFileName)

        myLogger.info(s"Saved to Parquet file: '$outputFileName'")
      }
      catch {
        case e: Exception => myLogger.exception(e, s"Error parsing file: ' $inputFileName'")
      }

    //all ok
    return true
  }

  //-------------------------------------------------------------------------
  /** Loads a file, creates a dataframe with and saves it as Parquet format
    @param sqlContext SQLContext used to save the dataframe
    @param inputFileName File to be loaded
    @param outputFileName Path where save the output
    @param itemDivider Divider of the items in the input file
    @return true if save has success
            false if save has failed 
  */  
  def saveFileAsParquet(sqlContext: SQLContext
    , inputFileName: String
    , outputFileName: String
    , itemDivider: String): Boolean =
    {
      saveFileAsParquetWithScheme(sqlContext
        , inputFileName
        , outputFileName
        , itemDivider
        , None,
        None)
    }

  //-------------------------------------------------------------------------  
  /** Saves a dataframe as Parquet format in the provided path
    @param sqlContext SQLContext used to save the dataframe
    @param dataFrame Dataframe to be exported
    @param path Path where save the output
    @return true if save has success
            or false if save has failed 
  */  
  def saveDataframeAsParquet(sqlContext: SQLContext
    , dataFrame: DataFrame
    , path: String): Boolean =
    {
      myLogger.info(s"Saving Parquet data at '$path'")

      try {
        //write Parquet file
        dataFrame.write
          .mode(SaveMode.Overwrite)
          .parquet(path)

        myLogger.info(s"Saved Parquet data in '$path'" )
      }
      catch {
        case e: Exception => myLogger.exception(e, "Error Saving in ParquetFile")
      }

    //all ok
    return true
  }
  //-------------------------------------------------------------------------
  /**
   * Loads a Parquet file and creates a dataframe with its content
   * @param sqlContext SQL context
   * @param inputFileName Name of the file to be loaded
   * @return new dataFrame with the content of Parquet file or null if it is not
   *         possible to load the file or create the dataframe
   */
  def loadParquetFile(sqlContext: SQLContext
    , inputFileName: String): DataFrame = {
      myLogger.info(s"Loading Parquet directory:     '$inputFileName' ")
      if (!myUtil.directoryExist(inputFileName)){
        myLogger.error(s"Parquet directory: ' $inputFileName' does not exist")
        return null
      }
      val df = sqlContext.read.parquet(inputFileName)
      myLogger.info(s"Loaded from Parquet directory: '$inputFileName'")

      return df
    }

  //-------------------------------------------------------------------------
  /** Drops all no numeric columns and return two dataframes, one with
   *  the numeric columns other with the name of no numeric columns
   * @param data Dataframe to process
   * @return Two dataframes. The first one with the numeric columns 
   * and the second one with the name of no numeric columns
   */
  def dropNoNumericColumnList(data: DataFrame): (DataFrame, DataFrame) = {
      var colName: DataFrame = null

      var dropColNameList = ListBuffer[String]()

      data.schema.fields.map {
        f =>
          f.dataType.typeName match {
            case "string" => {
              dropColNameList += f.name
              if (colName == null) colName = data.select(f.name)
            }
            case "timestamp" => dropColNameList += f.name

            case _ =>
          }
      }
      var filteredDF: DataFrame = data
      dropColNameList.map { col => filteredDF = filteredDF.drop(col) }

      return (filteredDF, colName)
    }

  //-------------------------------------------------------------------------
  /** Returns the n-column of a dataframe
   * @param data Dataframe to process
   * @param colPos Position of the column to process
   * @return N-column of a dataframe 
   */
  def getCol(data: DataFrame
    , colPos: Integer = 0): Column =
    data.col(data.columns(colPos))

  //-------------------------------------------------------------------------
  /** Returns an array of rows of a dataframe
   * @param data Dataframe to process
   * @param colPos Position of the column to process
   * @return N-column of a dataframe 
   */
  def getColAsArray(data: DataFrame
    , colPos: Integer = 0): Array[Any] =
    data.select(data.columns(colPos)).rdd.map(r => r(colPos)).collect()

  //-------------------------------------------------------------------------
  /** Returns the column names list of a dataframe
   * @param data Dataframe to process
   * @return List of the column names 
   */
  def getColNameList(data: DataFrame): Array[String] =
    data.schema.fields.map { f => f.name }

  //-------------------------------------------------------------------------
  /** Returns name on N-column of a dataframe
   * @param data Dataframe to process
   * @param colPos Position of the column to process
   * @return Name of the  N-column
   */
  def getColName(data: DataFrame
    , colPos: Int): String =
    data.schema.fieldNames(colPos)

  //-------------------------------------------------------------------------
  /** Returns N-column as a dataframe
   * @param data Dataframe to process
   * @param colPos Position of the column to process
   * @return New dataframe with n-column 
   */
  def getCol(df: DataFrame
    , colPos: Int): DataFrame =

    df.select(getColName(df,colPos))

  //-------------------------------------------------------------------------
  /** Returns an array with all rows of a dataframe
   * @param data Dataframe to process
   * @return All colums of dataframe as an RDD 
   */
  def getAsRowList(data: DataFrame): RDD[Vector] =
    data.rdd.map { row => Vectors.dense(row.toSeq.map({ case col: Double => col }).toArray) }

  //-------------------------------------------------------------------------
  /** Returns a dataframe where the columns names are present in a column name list
   * @param data Dataframe to process
   * @param colNameList Column name list to be present in the final dataframe
   * @param columnNameOrderBy Name of the column used to order the result. Empty if not order required
   * @return New dataframe with all columns present in the column name list  
   */  
  def filterByColName(data: DataFrame
    , colNameList: Seq[String]
    , columnNameOrderBy: String=""): (Boolean, DataFrame) = {
    //check that all col name are present in the dataset
    val intersc = getColNameList(data).toSet.intersect(colNameList.toSet)

    if (intersc.size != colNameList.size) {
      myLogger.error("Invalid columList. Not all column name are in the table. List of valid column name: " + intersc)
      return (false, null)
    }

    if (columnNameOrderBy.isEmpty())
      return (true
        , data.select(colNameList.map(col): _*))
    else
      return (true
        , data.select(colNameList.map(col): _*).orderBy(columnNameOrderBy))
  }

  //-------------------------------------------------------------------------
  /** Returns, for each column of the dataframe, if they are float or double
   * @param data Dataframe to process
   * @return New array where pos-n is true if type on the associated 
   * column in the dataframe is float or double   
   */ 
  def getType(data: DataFrame): Array[Boolean] = {

    val schema = data.schema
    schema.fieldNames.map { colName =>
      schema(colName).dataType match {
        case FloatType => true
        case DoubleType => true
        case _ => false
      }
    }
  }

  //-------------------------------------------------------------------------
  /** Creates a new iterator that applies the CSV format [[catSpark.dataFrame.CVS_SavingFormat]] to an input row
   * @param iterator Input row iterator
   * @param newColFormat Format to apply to the row
   * @return New iterator that applies a format to an input row
   */
  private def applyCSV_FormatToRow(iterator: Iterator[Row]
    , newColFormat: Array[Any => AnyRef]): Iterator[String] = {

    new Iterator[String] {
      override def hasNext: Boolean = iterator.hasNext
      override def next: String = {

        if (!iterator.isEmpty) {
          val valueList: Seq[AnyRef] = iterator.next().toSeq.zipWithIndex.map {
            case (value, i) => newColFormat(i)(value)
          }
          CVS_SavingFormat.getRecordSeparator + CVS_SavingFormat.format(valueList:_*)
        } //if
        else ""
      } //def
    } //iterator
  } //method

  //-------------------------------------------------------------------------
  /** Saves a dataframe to a CSV file. Modified from: [[https://github.com/databricks/spark-csv]]
   * @param data Dataframe to process
   * @param fileName Name of the CSV file to save to
   * @param generateHeader True if the name of the dataframe columns will be added 
   *        to the output file as header. False to do not add any header
   * @param itemDivider Divider to add between columns
   * @param commentHeaderPrefix string to be added before header  
   */
  //-------------------------------------------------------------------------
  //modified from: [[https://github.com/databricks/spark-csv]]
  //file: package.scala
  def saveAsCSV(data: DataFrame
    , fileName: String
    , generateHeader: Boolean = true
    , itemDivider: Char = myUtil.Comma
    , commentHeaderPrefix: String = "") {
    
    CVS_SavingFormat = CVS_SavingFormat.withDelimiter(itemDivider)

    //get the data types that will be used late to formatting (doubles)
    val schema = data.schema
    val formatByColPos = data.schema.fieldNames.map(colName => schema(colName).dataType match {
      case _ => (fieldValue: Any) => fieldValue.asInstanceOf[AnyRef]
    })

    val stringList = data.rdd.mapPartitions {
      case (iterator) => applyCSV_FormatToRow(iterator
                                              , formatByColPos)
    }.collect //execute lazy transformations

    val writer = new java.io.PrintWriter(fileName)
    if (generateHeader)
      writer.write(commentHeaderPrefix + CVS_SavingFormat.format(data.columns.map(_.asInstanceOf[AnyRef]): _*))
    stringList.map { row => writer.write(row) }
    writer.close
  }

  //-------------------------------------------------------------------------
  /** Returns a new dataframe that add an extra column with an increasing index starting from 0) 
   *  to all rows of an input dataframe
   *  Modified from: [[http://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex]]
   * @param data Dataframe to process
   * @param offset Starting row index
   * @param inFront True if add index as first column of the result dataframe.
   *        False to be the last one
   * @return new dataframe that add an extra column with an increasing index (starting from 0) 
   *  to all rows of an input dataframe       
   */
  //-------------------------------------------------------------------------
    def dataFrameZipWithIndex(data: DataFrame,
    offset: Int = 1,
    colName: String = "id",
    inFront: Boolean = true) : DataFrame = {

    data.sqlContext.createDataFrame(
      data.rdd.zipWithIndex.map{ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset) else Seq())
            ++ ln._1.toSeq ++
          (if (inFront) Seq() else Seq(ln._2 + offset)))},
      StructType(
        (if (inFront) Array(StructField(colName,LongType,false))
         else Array[StructField]())
            ++ data.schema.fields ++
        (if (inFront) Array[StructField]()
         else Array(StructField(colName,LongType,false)))
      )
    )
  }

  //-------------------------------------------------------------------------
  /** Adds a prefix to a name of a column of a dataframe
   * @param structField Data structure with the name of the column
   * @param prefix String to be added before the name of the column
   * @return New data structure with the prefix added to the name of the column
   */
  //-------------------------------------------------------------------------
  def renameColWithPrefix(structField: StructField
    , prefix: String) : StructField =
    structField.copy(prefix+structField.name,
           structField.dataType,
           structField.nullable,
           structField.metadata)

  //-------------------------------------------------------------------------
  /** Renames a column of a dataframe
   * @param structField Data structure with the name of the column to be renamed
   * @param newName New name to assign to column
   * @return New data structure with column renamed
   */
  def renameCol(structField: StructField
    , newName: String) : StructField =
    structField.copy(newName,
           structField.dataType,
           structField.nullable,
           structField.metadata)

  //-------------------------------------------------------------------------
  /** Updates the type of a column of a dataframe 
   *  Modified from: [[http://stackoverflow.com/questions/29383107/how-to-change-column-types-in-spark-sqls-dataframe]]
   * @param data Dataframe to process
   * @param colName Column name to update type to
   * @return New dataframe with the new column type
   */
  // see [[http://stackoverflow.com/questions/29383107/how-to-change-column-types-in-spark-sqls-dataframe]]
  def castColumnTo(data: DataFrame
    , colName: String
    , newType: DataType ) : DataFrame =
    data.withColumn(colName
      , data(colName).cast(newType))

  //-------------------------------------------------------------------------
  /** Drops the first N rows of a dataframe
   * @param data Dataframe to process
   * @param n Number of columns to drop
   * @return New dataframe without the first n columns
   */  def dropFirstN_Row(data: DataFrame,
      n: Int = 1) : DataFrame = {
    val rdd = data.rdd.mapPartitionsWithIndex{
        case (index, iterator) => if(index==0) iterator.drop(n) else iterator
    }
    data.sqlContext.createDataFrame(rdd, data.schema)
  }

  //-------------------------------------------------------------------------
  /** Moves last column of a dataframe to be the first one 
   * @param data Dataframe to process
    * @return New dataframe where the last column is now the first one
   */  def moveLastColumnToFirst(data: DataFrame) : DataFrame = {
     var colNameList = getColNameList(data)  //move last col to first position
     val lastColumn = colNameList.last
     val remainCol = colNameList.dropRight(1)
     data.select(lastColumn,remainCol:_*)
  }

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'myDataFrame'
//=============================================================================
// End of 'dataFrame.scala' file
//=============================================================================
