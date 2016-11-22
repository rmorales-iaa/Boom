//=============================================================================
//File: configuration.scala
//=============================================================================
/** It manages the user configuration file in catSpark. See object declaration
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
package catSpark.configuration

//=============================================================================
// System import section
//=============================================================================
import com.typesafe.config.{Config,ConfigFactory}

import java.io.File

//=============================================================================
// User import section
//=============================================================================
import catSpark.logger.myLogger

//=============================================================================
// Class/Object implementation
//=============================================================================

/** Parser of a user's configuration file based on 'config' ([[https://github.com/typesafehub/config]])
 *  The configuration files are based on key-value pairs
 */
object myConf{

  //-------------------------------------------------------------------------
  //Class variables
  //-------------------------------------------------------------------------

   private var config : Config = null

  //-------------------------------------------------------------------------
  //method list
  //-------------------------------------------------------------------------

  /** Loads and checks a user's  configuration file
    @param fileName Filename of the user's file
    @return True if file has been properly parsed
            or false if file has not been properly parsed 
  */
  def loadAndCheck(fileName: String) : Boolean ={

    try{
     myLogger.info(s"Loading configuration file: '$fileName'")

     val f = new File(fileName);
     if (!f.exists() || f.isDirectory())
        return myLogger.error(s"Configuration file: '$fileName' does not exist")

     config = ConfigFactory.parseFile(new File(fileName))
     config.checkValid(ConfigFactory.defaultReference(),fileName)
     myLogger.info(s"Loaded configuration file: '$fileName'")
    }
    catch {
      case e: Exception => println (s"Error parsing configuration file: ' $fileName '. " + e.toString())
      return false
    }
  }

  //-------------------------------------------------------------------------
  /** Returns a boolean value associated to a key in the current configuration file 
    @param key Name of the key
    @return The boolean value associated with the key
  */
  def getBoolean(key: String) : Boolean = config.getBoolean(key)

  //-------------------------------------------------------------------------
  /** Returns a string value associated to a key in the current configuration file 
    @param key Name of the key
    @return The string value associated with the key
  */
  def getString(key: String) : String = config.getString(key)

  //-------------------------------------------------------------------------
  /** Returns a integer value associated to a key in the current configuration file 
    @param key Name of the key
    @return The integer value associated with the key
  */
  def getInt(key: String) : Int = config.getInt(key)
  
  //-------------------------------------------------------------------------
  /** Returns a double value associated to a key in the current configuration file 
    @param key Name of the key
    @return The Double value associated with the key
  */
  def getDouble(key: String) : Double = config.getDouble(key)

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //End of object 'myConf'
//=============================================================================
// End of 'configuration.scala' file
//=============================================================================
