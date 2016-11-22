//=============================================================================
//File: logger.scala
//=============================================================================
/** It implements logging capabilities in catSpark. See object declaration
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
package catSpark.logger

//=============================================================================
// System import section
//=============================================================================
import com.typesafe.scalalogging._

import org.slf4j.LoggerFactory

import java.util.Calendar
import java.text.SimpleDateFormat

//=============================================================================
// User import section
//=============================================================================
import catSpark.util.myUtil

//=============================================================================
// Class/Object implementation
//=============================================================================
/**
 * Logging based on library scala-logging: [[https://github.com/typesafehub/scala-logging]]
 * The configuration of the log can be found in source code file 'src/main/resources/logback.xml'
 * Factory for [[catSpark.logger.myLogger]] instances
 */
object myLogger extends StrictLogging {

  //-------------------------------------------------------------------------
  //initialize the logger
  private val myLogger = LoggerFactory.getLogger(this.getClass)

  //-------------------------------------------------------------------------
  //time format used to print in console
  private val timeFormat_2 = new SimpleDateFormat("yyyy'y_'MM'm_'dd'd:'HH'h_'mm'm_'ss's_'SSS'ms'")

  //-------------------------------------------------------------------------
   /**
   * Prints a message into console
   * @param s string to print
   */
  private def console(s: String) {
	  Console.print(timeFormat_2.format(Calendar.getInstance().getTime())+"::"+s+myUtil.LineSeparator);
  }

  //-------------------------------------------------------------------------
  //method list
  //-------------------------------------------------------------------------
  /**
  * Adds a debug string to logger
  * @param s string to add
  */  
  def debug(s: String) : Boolean = {
    myLogger.debug(s)
    return true;
  }

  //-------------------------------------------------------------------------
  /**
  * Adds a warning string to logger
  * @param s string to add
  */  
  def warning(s: String) : Boolean = {
    myLogger.warn(s)
    return true;
  }

  //-------------------------------------------------------------------------
  /**
  * Adds a error string to logger
  * @param s string to add
  */
  def error(s: String) : Boolean = {
    myLogger.error(s)
    return false;
  }

  //-------------------------------------------------------------------------
  /**
  * Adds a info string to logger
  * @param s string to add
  */
  def info(s: String) : Boolean = {
    myLogger.info(s)
    console(s)
    return true
  }

  //-------------------------------------------------------------------------
  /**
  * Adds a trace string to logger
  * @param s string to add
  */
  def trace(s: String) : Boolean = {
    myLogger.trace(s)
    return true
  }

  //-------------------------------------------------------------------------
  /**
  * Adds a exception info to logger
  * @param exception Exception to be added to log
  * @param message String to be added to log anfter the exception message
  */
  def exception(exception: Exception
    , message: String) : Boolean = {

    val s =  message +
      " Exception:'" + exception.toString() +
      "' . Stack trace: '" + exception.getStackTrace + "'"

    myLogger.error(s)
    console(s)
    return false
  }

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'myLogger'
//=============================================================================
// End of 'logger.scala' file
//=============================================================================
