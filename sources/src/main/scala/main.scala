//=============================================================================
//File: main.scala
//=============================================================================
/** It is the main entrance of Boom!. See object declaration
 *  @author  Rafael Morales Muñoz
 *  @mail    rmorales.iaa.es
 *  @version 1.0
 *  @date    9 Nov 2016
 *  @history None
 */
//=============================================================================
//=============================================================================
// System import section
//=============================================================================

//=============================================================================
// User import section
//=============================================================================
import boom.Boom
import boom.myCommandLineParser;

import catSpark.configuration.myConf
import catSpark.logger.myLogger
import catSpark.util.myUtil

//=============================================================================
// Class/Object implementation
//=============================================================================

//=============================================================================
/** Main object to process M stars information using 'CatSpark' tool that is based
 * in Spark: [[http://spark.apache.org]] version 1.6.2
 *  @author Rafael Morales(rmorales@iaa.es)
 *  @version 1.0
 *  history:  None
 */
object Main {

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
  /** Main object */
  private val boom = new Boom()

  /** Keeps time when program starts */
  private var startTime = System.currentTimeMillis

  /** True is user's script is active */
  private var scriptActive = false

  /** True if boom object is closed */
  private var closed = false

  //-------------------------------------------------------------------------
  /** Main function of Boom. It loads the user input and process it
    @param userArgumentList User parameter list see [[boom.myCommandLineParser]]
  */
  def main(userArgumentList: Array[String]) : Unit ={

    //start message
    myLogger.info("Boom starts!!!") //log
    parseAndRun(userArgumentList)
  }

  //-------------------------------------------------------------------------
  /** Parses and runs user argument list. Is some problem raises it will be reported in the
   *  log file and console
    @param  userArgumentList User parameter list see [[boom.myCommandLineParser]]
    @return True if users's input is parseable and no problems found when running
            or false if users's input is not parseable or problems found when running
  */
  def parseAndRun(userArgumentList: Array[String]) : Boolean = {

    //parse user's argument list
    myCommandLineParser.parse(userArgumentList) match {

      case Some(config) => {runCommand(boom
        , config)}
      case None => false //Error parsing userArgumentList. A message was been displayed
    }
  }

  //-------------------------------------------------------------------------
  /** Parses and run a user's script.
    @param  fileName User's script file
    @return True if users's script is parseable and no problems found when running
            or false if users's script is not parseable or problems found when running
  */

  def processScript(fileName: String): Boolean ={

    scriptActive = true
    val userArgumentList= boom.processScript(fileName)
    if (userArgumentList == null)
      false
    else
      parseAndRun(userArgumentList)
  }

  //-------------------------------------------------------------------------
  /** Initializes main object 'Boom' if it necessary and
   *  runs the user's command. See [[boom.myCommandLineParser]])
    @param boom Object to be initiliased
    @param c User's argument list configuration
    @return True if users's command has no problems when running
            or false if users's command has problems when running
  */
  def runCommand(boom: Boom,
    c: myCommandLineParser.ParserConf) : Boolean ={

    //create boom object and initialize it
    if (!c.command.equals(myCommandLineParser.scriptCommand.name) ||
        scriptActive){
      //create the configuration file
      if (!myConf.loadAndCheck(c.configurationFile))
        return close

      if(!boom.initialize(c.applicationSuffix))
        return close
    }

    //process command
    val result = c.command match{

      case myCommandLineParser.scriptCommand.name => {processScript(c.scriptFile)}

      case myCommandLineParser.parquetCommand.name => {boom.saveAsParquet(c.parquetOutputDirectory
                                                                            , c.parquetScheme
                                                                            , c.parquetTableList
                                                                            , c.parquetInputFile
                                                                            , c.parquetInputItemDividerComma)};
                                                                          
      case myCommandLineParser.statCommand.name => {boom.basicStat(c.spearman)};

      case myCommandLineParser.kMeansCommand.name => {boom.kmeans(c.kMeansK
                                                                    , c.kMeansIteration
                                                                    , c.kMeansRun
                                                                    , c.kMeansEpsilon
                                                                    , c.kMeansColName)};

      case myCommandLineParser.kNearestNeighborCommand.name => {boom.kNearestNeighbor(c.kNearestNeighborK
                                                                                        , c.kNearestNeighScale
                                                                                        , c.kNearestNeighborColA
                                                                                        , c.kNearestNeighborColB
                                                                                        , c.kNearestNeighborColC)};

      case myCommandLineParser.plotCommand.name => {boom.plot(c.plot_x_axis
                                                                , c.plot_y_1_axis_list
                                                                , c.plot_y_2_axis_list)};

      case _ => false
    }

    close
  }
  //-------------------------------------------------------------------------
  /** Closes main object 'Boom'
    @return true in any case
  */
  def close() : Boolean = {
    if (!closed ){
      boom.close()
      myLogger.info("Elapsed time:" + myUtil.getString(System.currentTimeMillis-startTime))
      myLogger.info("Boom ends!!!")
      closed = true
    }
    true
  }
  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'Main'

//=============================================================================
// End of 'main.scala' file
//=============================================================================
