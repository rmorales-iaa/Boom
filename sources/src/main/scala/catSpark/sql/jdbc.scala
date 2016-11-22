//=============================================================================
//File: jdbc.scala
//=============================================================================
/** It implements jdbc import capabilities in catSpark. See object declaration
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
package catSpark.sql

//=============================================================================
// System import section
//=============================================================================
import org.apache.spark.sql.{SQLContext,DataFrame}

import java.sql.{Connection, Statement, ResultSet}
import java.util.TimeZone

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
 * Allows to access to JDBC database scheme and tables and implements methods 
 * to export schemes and tables to Parquet format
 */
object myJDBC {

  /** Current SQLContext */
  private var sqlContext: SQLContext = null;
  
  /** Stored id driver has been loaded */
  private var JDBC_driver_loaded = false;

  /** Name of the JDBC driver */
  private val DefaultJDBC_DriverName = "com.mysql.jdbc.Driver"
  
  /** Time zone used to access to JDBC database */
  private val TimeZoneName = java.util.TimeZone.getDefault.getID
  
  //-------------------------------------------------------------------------
  /** Sets the current SQLContext
   * @param _sqlContext Current SQLContext
   */  
  def setSQL_Context(_sqlContext: SQLContext) =
      sqlContext = _sqlContext

  //-------------------------------------------------------------------------
  /** Builds a JDBC url to access JDBC databaseto a database using a starting url, user and password.
   *  It fixes the server time zone to 'java.util.TimeZone.getDefault.getID' to avoid
   *  problems when accessing time values
   * @param basicURL Starting url
   * @param user Name of the JDBC database user
   * @param pass Password of the JDBC database user
   * @return A new JDBC url with user and password 
   */      
  private def buildURL(basicURL: String
    , user: String
    , pass: String): String = {

    s"$basicURL?user=$user&password=$pass&useLegacyDatetimeCode=false&serverTimezone=$TimeZoneName"
  }

  //-------------------------------------------------------------------------
  /** Loads a JDBC driver
   * @param driverName Name of the driver
   * @return true if has been properly loaded
              or false if cluster has not been properly loaded 
   */  
  def loadDriver(driverName: String) : Boolean = {

    myLogger.info(s"Loading JDBC driver '$driverName'")

    try{
      Class.forName(driverName).newInstance
      myLogger.info(s"JDBC driver '$driverName' loaded sucessfully")
      JDBC_driver_loaded = true;
      true
    }
    catch{
      case e: Exception  => {
        myLogger.exception(e,s"Error loading JDBC driver '$driverName'. ")
        JDBC_driver_loaded = false;
        false
      }
    }
  }
  
  //-------------------------------------------------------------------------
  /** Gets a table from a JDBC database and create a dataframe with it content
   * @param url JDBC url
   * @param tableName Name of the JDBC database table
   * @param user Name of the JDBC database user
   * @param pass Password of the JDBC database user
   * @param driverName Name of the JDBC driver
   * @return true if dataframe has been properly created
             or false if dataframe has not been properly created 
   */
  def loadTable(url: String
    , tableName: String
    , user: String
    , pass: String
    , driverName: String = DefaultJDBC_DriverName) : Option[DataFrame] = {

    myLogger.info(s"Loading table '$tableName' from '$url'")

    //check if driver is loaded
    if (!JDBC_driver_loaded){
      if (!loadDriver(driverName))
        return None
    }

    var df : DataFrame = null
    try {
      val prop = new java.util.Properties
      prop.setProperty("driver", driverName)

      df = sqlContext.read.jdbc(buildURL(url,user,pass)
        , tableName
        , prop) //connectionProperties

      Some(df)
    }
    catch {
        case e: Exception => myLogger.exception(e, s"Error loading table from ' $url'")
        None
      }
  }

  //-------------------------------------------------------------------------
  /** Gets the name list of the JDBC database tables stored in a JDBC scheme 
   * @param url JDBC url
   * @param tableName Name of the JDBC database table
   * @param user Name of the JDBC database user
   * @param pass Password of the JDBC database user
   * @param driverName Name of the JDBC driver
   * @return Name list of the tables of the scheme
             or None if case of error or empty scheme 
   */
  def getTableListFromScheme(url: String
    , schemeName: String
    , user: String
    , pass: String
    , driverName: String = DefaultJDBC_DriverName): Option[Array[String]] = {

    val dfTableList = loadTable(url
      , "information_schema.tables"
      , user
      , pass);

    if (!dfTableList.isDefined) return None

    val df = dfTableList.get
    val temporalTable = "tableNameList"
    val query = s"select table_name from $temporalTable where table_schema='$schemeName'"
    df.registerTempTable(temporalTable)         //associate a temporal table to the loaded table
    var dfSQL = df.sqlContext.sql(query)        //execute query on temporal table
    scala.util.Try(sqlContext.dropTempTable(temporalTable)) //delete temporal table
    Some(dfSQL.rdd.map(r => r(0).asInstanceOf[String]).collect)  //get the result
  }

  //-------------------------------------------------------------------------
  /** Saves a JDBC database table into Parquet format, deleting previous content in the output 
   * @param schemeName Name of the scheme of the table 
   * @param tableName Name of the table
   * @param df Data of the table
   * @param rootPath Path to store the formatted data
   */
  def saveFileAsParquet(schemeName: String
    , tableName: String
    , df: DataFrame
    , rootPath: String) {

      val path = myUtil.ensureEndWithFileSeparator(rootPath)+tableName
      myUtil.deleteDirectory(path)
      myUtil.ensureDirectoryExist(path)

      myDataframe.saveDataframeAsParquet(sqlContext
        , df
        , path)
  }

  //-------------------------------------------------------------------------
  /** Saves a complete JDBC database scheme into Parquet format
   * @param url JDBC url
   * @param schemeName Name of the scheme 
   * @param user Name of the JDBC database user
   * @param pass Password of the JDBC database user 
   * @param rootPath Path to store the formatted data
   * @param driverName Name of the JDBC driver
   */
  def saveSchemeAsParquet(url: String
    , schemeName: String
    , user: String
    , pass: String
    , rootPath: String
    , driverName: String = DefaultJDBC_DriverName) : Boolean = {

    myLogger.info(s"Converting into Parquet format scheme: '$schemeName' into root path: '$rootPath'")

    val tableList = getTableListFromScheme(url
      , schemeName
      , user
      , pass
      , driverName)

     if (!tableList.isDefined) return false

    saveTableListAsParquet(url
      , schemeName
      , tableList.get
      , user
      , pass
      , rootPath
      , driverName)

    myLogger.info(s"Converted into Parquet format scheme: '$schemeName' into root path: '$rootPath'")
  }
  
  //-------------------------------------------------------------------------
  /** Saves a JDBC database table name list into Parquet format
   * @param url JDBC url 
   * @param schemeName Name of the scheme 
   * @param tableList Table name list
   * @param user Name of the JDBC database user
   * @param pass Password of the JDBC database user 
   * @param rootPath Path to store the formatted data
   * @param driverName Name of the JDBC driver
   */
  def saveTableListAsParquet(url: String
    , schemeName: String
    , tableList: Seq[String]
    , user: String
    , pass: String
    , rootPath: String
    , driverName: String = DefaultJDBC_DriverName) : Boolean = {

     val tableListAsString = tableList.mkString(",")
     val count = tableList.size
     myLogger.info(s"Converting into Parquet a list of $count tables: $tableListAsString into root path: '$rootPath'")

     tableList.map { tableName =>

        val df = loadTable(url
          , schemeName+"."+tableName
          , user
          , pass
          , driverName)

        if (df.isDefined){
          saveFileAsParquet(schemeName
           , tableName
           , df.get
           , myUtil.ensureEndWithFileSeparator(rootPath)+schemeName)

           df.get.unpersist() //remove form cache
        }
      }
      myLogger.info(s"Converted into Parquet a list of $count tables: $tableListAsString into root path: '$rootPath'")
  }

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'myJDBC'
//=============================================================================
// End of 'jdbc.scala' file
//=============================================================================
