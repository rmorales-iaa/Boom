//=============================================================================
//File: hive.scala
//=============================================================================
/** It manages some of Hive capabilities in catSpark. See object declaration
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
package catSpark.hive

//=============================================================================
// System import section
//=============================================================================
import com.databricks.spark.csv._

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext}

//=============================================================================
// User import section
//=============================================================================
import catSpark.logger.myLogger

//=============================================================================
// Class/Object implementation
//=============================================================================
/** Useful HIVE (extension of Spark-SQL) methods */
object myHive {

  //-------------------------------------------------------------------------
  //method list
  //-------------------------------------------------------------------------
   /**
   * Creates a Hive context from spark context
   * @param sparkContext Spark context 
   * @return Hive context created
   */
   def createHiveContext (sparkContext: SparkContext): HiveContext =
   {
      val hiveContext = new HiveContext(sparkContext)
      return hiveContext
   }

  //-------------------------------------------------------------------------
  /**
   * Loads a Tab Separated Value (TSV) file
   * @param sparkContext Spark context
   * @param fileName Name of the file with TSV content
   * @return Dataframe with the content of the file
   */
  def loadTSV(hiveContext: HiveContext,
    fileName: String): DataFrame =
   {
     val dataFrame = hiveContext.read
       .format("com.databricks.spark.csv")
       .option("inferSchema", "true") // automatically infer data types
       .option("delimiter", "\t")     // it is a TSV file
       .option("comment", "#")        // comments start with #
       .load(fileName);

     return dataFrame
   }
  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'myHive'
//=============================================================================
// End of 'hive.scala' file
//=============================================================================
