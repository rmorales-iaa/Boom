//=============================================================================
//File: sql.scala
//=============================================================================
/** It implements some SQL capabilities in catSpark. See object declaration
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
import java.text.SimpleDateFormat
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SQLContext}

//=============================================================================
// User import section
//=============================================================================
import catSpark.logger.myLogger

//=============================================================================
// Class/Object implementation
//=============================================================================
/** Utilities to manage SQL context and SQL queries */
object mySQL {

  //-------------------------------------------------------------------------
  //method list
  //-------------------------------------------------------------------------
  /** Creates a SQL context from spark context
    * @param sparkContext SparkContext where to create the sql context
    * @return new SQL context created
    */
  def createContext(sparkContext: SparkContext): SQLContext =
    {
      val sqlContext = new HiveContext(sparkContext)
      import sqlContext.implicits._ //just below creating SQLcontext
      return sqlContext
    }
    
  //-------------------------------------------------------------------------
  /** Creates a 'java.sql.Date' from a string and later applies a format
    * @param s String that stores the date 
    * @param format Format to apply to the date
    * @return A new java.sql.Date or None in case of error creating the date
    */
  def getSQL_Date(s: String,
    format: SimpleDateFormat) : Option[java.sql.Date]  = {

      try{
       Some(new java.sql.Date(format.parse(s).getTime))
    }
   catch{
      case e: Exception  => {
        myLogger.exception(e,s"Error parsing SQL data '$s' with format '$format' ")
        None
      }
    }
  }
  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'mySQL'
//=============================================================================
// End of 'sql.scala' file
//=============================================================================
