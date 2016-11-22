//=============================================================================
//File: myUtil.scala
//=============================================================================
/** It implements a set of utility methods used in catSpark. See object declaration
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
package catSpark.util

//=============================================================================
// System import section
//=============================================================================
import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit
import java.sql.{Date, Timestamp}
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter

import scala.io.Source
import scala.collection.mutable.ArrayBuffer

//=============================================================================
// User import section
//=============================================================================
import catSpark.configuration.myConf
import catSpark.logger.myLogger
import catSpark.dataFrame.myDataframe

//=============================================================================
// Class/Object implementation
//=============================================================================
/** List of common utility methods for catSpark */
object myUtil {

  //-------------------------------------------------------------------------
  //time format  
  /** Long time format */
  val TimeFormatLong = new SimpleDateFormat("yyyy'y_'MM'm_'dd'd_'HH'h_'mm'm_'ss's_'SSS'ms'")
  
  /** Short time format */
  val TimeFormatShort = new SimpleDateFormat("yyyy'-'MM'-'dd")
  
  /** Common time format */
  val TimeFormatCommon = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //Constants regarding to path
  /** Operating system file separator */
  val FileSeparator = java.io.File.separator
  
  /** Operating system path separator */
  val PathSeparator = java.io.File.pathSeparator // ':' in gnu-linux
  
  /** Operating system line separator */
  val LineSeparator = System.getProperty("line.separator","\n")

  //relevant string or chars
  /** Tabulator as char*/
  val Tab = '\t'
  
  /** Tabulator as string*/
  val TabString = "\t"

  /** Comma as char*/
  val Comma = ','
  
  /** Comma as sting*/
  val CommaString = ","

  /** gexf file extension*/
  val GexfFileExtension  = ".gexf"
  
  /** TSV file extension*/
  val TabFileExtension   = ".tsv"
  
  /** CSV file extension*/
  val CommaFileExtension = ".csv"
  
  /** gnu-plot file extension*/
  val plotFileExtension = ".gnuplot"
  
  /** decimal formatter */  
  val decimalFormatter= new DecimalFormatSymbols();
  decimalFormatter.setDecimalSeparator('.');
  
  //-------------------------------------------------------------------------
  //method list
  //-------------------------------------------------------------------------
  /** Returns a string that translates the input milliseconds to days,hours,minutes and seconds
   * @param milliSeconds Time to convert
   * @return String with the input milliseconds converted to days,hours,minutes and seconds
  */
  def getString(milliSeconds:Long) : String ={

    var m = milliSeconds

    val days = TimeUnit.MILLISECONDS.toDays(m);
    m -= TimeUnit.DAYS.toMillis(days);

    val hours = TimeUnit.MILLISECONDS.toHours(m);
    m -= TimeUnit.HOURS.toMillis(hours);

    val minutes = TimeUnit.MILLISECONDS.toMinutes(m);
    m -= TimeUnit.MINUTES.toMillis(minutes);

    val seconds = TimeUnit.MILLISECONDS.toSeconds(m);

    return days+"D "+hours+"H "+minutes+"m "+seconds+"s "
  }
  
  //-------------------------------------------------------------------------
  /** Gets the current timestamp applying a format to the result
   * @param f Format to apply
   * @return Current timestamp after applying the input format
  */
  def getTimeStamp(f: SimpleDateFormat = TimeFormatShort) : String =
    f.format(Calendar.getInstance().getTime())

   //-------------------------------------------------------------------------
  /** Returns if the input path is a directory
   * @param s Path to check 
   * @return True if input is a directory, false in other case
  */
  def directoryExist(s:String) : Boolean =
  {
     val f = new File(s);
     f.exists() && f.isDirectory()
  }

   //-------------------------------------------------------------------------
  /** Returns if the input path is a file
   * @param s Path to check 
   * @return True if input is a file, false in other case
  */
  def fileExist(s:String) : Boolean =
  {
     val f = new File(s);
     f.exists() && !f.isDirectory()
  }

  //-------------------------------------------------------------------------
  /** Returns a new string that ends with [catSpark.util.FileSeparator]
   * @param s String to process
   * @return Returns a new string that ends with [catSpark.util.FileSeparator]
  */
  def ensureEndWithFileSeparator(s:String) : String =
    if (s.endsWith(FileSeparator)) s else s+FileSeparator

  //-------------------------------------------------------------------------
  /** Returns a new string that ends with the provided suffix
   * @param s String to process 
   * @param Suffix to add if it necessary
   * @return Returns a new string that ends with the provided suffix
  */  
  def ensureFileExtension(s:String
    ,suffix: String) : String =
    if (s.endsWith(suffix)) s else s+suffix

  //-------------------------------------------------------------------------
  /** Creates a list of hierarchical directories (if it is necessary) using the specified path
   * @param s Path to process
   * @return True if the list of hierarchical directories can be created, false in other case
  */
  def ensureDirectoryExist(s:String) : Boolean = {
    val d=new java.io.File(s)
    if (!d.exists()){
      if (!d.mkdirs())
        return myLogger.error(s"Error creating output directory:'$s'")
      else
        return myLogger.info(s"Created output directory:'$d'")
    }
    myLogger.info(s"Using previous output directory:'$d'")
  }

  //-------------------------------------------------------------------------
  /** Converts a double value to a string with the provided precision
   * @param d Double to process
   * @param decimalCount Precision of the output
   * @return Double value converted to a string with the provided precision
  */

  def formatDouble(d: Double
    , decimalCount: Int) : Double = d.isNaN match {
      case false => new java.text.DecimalFormat("#."+"#"*decimalCount,decimalFormatter).format(d).toDouble
      case _ => d
  }

  //-------------------------------------------------------------------------
  /** Converts a float value to a string with the provided precision
   * @param d Float to process
   * @param decimalCount Precision of the output
   * @return Float value converted to a string with the provided precision
  */
  def formatFloat(d: Float,decimalCount: Int) : Float = d.isNaN match {
      case false => new java.text.DecimalFormat("#."+"#"*decimalCount,decimalFormatter).format(d).toFloat
      case _ => d
  }

   //-------------------------------------------------------------------------
  /** Returns if the comma divider has been specified in the configuration file  */
  def getConfUseCommaDivider :Boolean= {

    if (myConf.getBoolean("Boom.outputConf.useCSVforOutputTable"))
       true
    else
       false
  }

  //-------------------------------------------------------------------------
  /** Returns the comma divider specified in the configuration file  */
  def getConfFileNameExtension :Char= {

    if (myConf.getBoolean("Boom.outputConf.useCSVforOutputTable"))
        myUtil.Comma
    else
        myUtil.Tab
  }

  //-------------------------------------------------------------------------
  /** Returns the file extension for output files specified in the configuration file  */
  def getConfFileNameFormat(fileName: String) : (String,Char)= {

    if (myConf.getBoolean("Boom.outputConf.useCSVforOutputTable"))
       (myUtil.ensureFileExtension(fileName,myUtil.CommaFileExtension)
          , getConfFileNameExtension)
    else
       (myUtil.ensureFileExtension(fileName,myUtil.TabFileExtension)
         , getConfFileNameExtension)
  }

  //-------------------------------------------------------------------------
  /** Converts the input to a timestamp, applies a format and returns the result as a string
   * @param s Input timestamp
   * @param format Format to apply to input
   * @return Returns the input after applying the format
  */
  def getTimeStamp(s: String,
    format: SimpleDateFormat) : Option[java.sql.Timestamp]  = {

      try{
       Some(new java.sql.Timestamp(format.parse(s).getTime))
    }
   catch{
      case e: Exception  => {
        myLogger.exception(e,s"Error parsing SQL data '$s' with format '$format' ")
        None
      }
    }
  }

  //-------------------------------------------------------------------------
  /** Calculates the linear interpolation (y value) of one point (x value) regarding
   *  other two points in a two dimension space
   * @param x0 x coordinate of the first point 
   * @param y0 y coordinate of the first point 
   * @param x1 x coordinate of the second point 
   * @param y1 y coordinate of the second point
   * @param x Point to be interpolated
   * @return Linear interpolation of the x value regarding to other two points
  */
  def linearInterpolation(x0:Double
    , y0: Double
    , x1: Double
    , y1: Double
    , x: Double) = y0 + ((y1-y0)*((x-x0)/(x1-x0)))

  //-------------------------------------------------------------------------
  /** Loads a file, filters the line comments and returns the result 
   * @param fileName Name of the file to process
   * @return List of lines of the input file without comment lines
  */
  def loadFileAndSkipComment(fileName : String): Array[String] = {

    val storage = ArrayBuffer[String]()
    val seq = for (line <- Source.fromFile(fileName).getLines()
       if (!line.trim.startsWith("#") &&
           !line.isEmpty))
         storage+=line.trim

    storage.toArray
  }

  //-------------------------------------------------------------------------
  /** Deletes the provided directory
   * @param path Directory to delete
  */
  def deleteDirectory(path: String) ={
    FileUtils.deleteDirectory(new File(path))
  }

  //-------------------------------------------------------------------------
  /** Returns the program current path  */
  def getCurrentPath() : String ={

     ensureEndWithFileSeparator(new File("").getAbsolutePath())
  }

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'myUtil'
//=============================================================================
// End of 'myUtil.scala' file
//=============================================================================
