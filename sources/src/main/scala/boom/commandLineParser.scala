//=============================================================================
//File: commandLine.scala
//=============================================================================
/** It parsers user's command line arguments for BOOM!. See object declaration
 *  @author  Rafael Morales Muñoz
 *  @mail    rmorales.iaa.es
 *  @version 1.0
 *  @date    9 Nov 2016
 *  history  None
 */
//=============================================================================
//=============================================================================
// Package section
//=============================================================================
package boom

//=============================================================================
// System import section
//=============================================================================


//=============================================================================
// User import section
//=============================================================================
import BuildInfo._
import scopt.OptionParser

//=============================================================================
// Class/Object implementation
//=============================================================================

//-------------------------------------------------------------------------
//-------------------------------------------------------------------------
/** Parser the user arguments of Boom! application based on scopt ([[https://github.com/scopt/scopt]])
 */
object myCommandLineParser
{
  //-------------------------------------------------------------------------
  //list of commands 
  sealed abstract class CommadLine(val name: String)
  
  case object scriptCommand extends CommadLine("script")
  case object parquetCommand extends CommadLine("parquet")
  case object statCommand extends CommadLine("stat")
  case object kMeansCommand extends CommadLine("kmeans")
  case object kNearestNeighborCommand extends CommadLine("kNearestNeighbor")
  case object plotCommand extends CommadLine("plot")

  //-------------------------------------------------------------------------
  // Parser configuration
   /** BOOM! parser configuration based on scopt. See [[https://github.com/scopt/scopt]] */
  case class ParserConf
  (
      //user's configuration file 
      configurationFile: String =                "input/configuration/Boom.conf"

     //suffix added to the name of the task reported in the Spark report web page 
     , applicationSuffix: String =               ""

     //current user's command 
     , command: String =                         ""

     // Command script file  
     , scriptFile: String =                      ""

     //arguments of command Parquet
     , parquetInputFile: String =                ""
     , parquetOutputDirectory: String =          ""
     , parquetInputItemDividerComma: Boolean =   false
     , parquetJDBC : Boolean =                   false
     , parquetScheme : Boolean =                 false
     , parquetTableList : Seq[String] =          Seq()

     //arguments of command stat
     , spearman: Boolean =                       false

     //arguments of command  kmeans
     , kMeansK: Int =                            2
     , kMeansIteration: Int =                    10
     , kMeansRun: Int =                          10
     , kMeansEpsilon: Double =                   1e-4
     , kMeansColName: Seq[String] =              Seq()

      //command kNearestNeighbor
     , kNearestNeighborK: Int =                  2
     , kNearestNeighScale: Int =                 10
     , kNearestNeighborColA: String =            ""
     , kNearestNeighborColB: String =            ""
     , kNearestNeighborColC: String =            ""

     //command plot
     , plot_x_axis: String =                     ""
     , plot_y_1_axis_list: Seq[String] =         Seq()
     , plot_y_2_axis_list: Seq[String] =         Seq()
  )

  //-------------------------------------------------------------------------
  //Info about BOOM! to be shown 
  private val info =
"Description:      "+BuildInfo.description+"\n"+
"Version:          v"+BuildInfo.version+"\n" +
"Build:            "+BuildInfo.buildInfoBuildNumber+"\n" +
"Build date (UTC): "+BuildInfo.builtAtString+"\n" +
"Author list:      "+BuildInfo.authorList+"\n" +
"Scala version:    "+BuildInfo.scalaVersion+"\n" +
"sbt version:      "+BuildInfo.sbtVersion+"\n" +
"git branch:       "+BuildInfo.myGitCurrentBranch+"\n" +
"git commit:       "+BuildInfo.myGitHeadCommit+"\n" +
"License:          "+BuildInfo.license

  //-------------------------------------------------------------------------
  //example of use of Boom
   private val usageExampleList : String  =
"""
BOOM! (Big data On Our M starts) is a application to process data from M-starts 
using CatSpark tool.
CatSpark (Common data analysis tool with Spark verion 1.6.2) is 
single tool developed with Scala that incorporates a set
of Spark libraries to perform a common basic analysis of data regardless its semantics:

  1) basic statistical descriptive measures 
  2) data clustering not supervised
  3) data spatial distribution
  4) data plot visualization

CatSpark can be used as template to adapt/extend projects with specific requirements.
Large data management require manual data partition that depends on semantics and it is not implemented.

General comments:
  1) The input and output must be accessible from computation cluster
  2) The input file table format is described below in the "Notes section"
  3) String columns will be ignored in the calculations.

CatSpark components:

    Spark components
     "sql",   //data querying
    ,"hive"   //sql extension
    ,"mllib"  //machine learning
    ,"graphx" //machine learning

    External libraries
    Logging. https://github.com/typesafehub/scala-logging
    "com.typesafe.scala-logging" and "ch.qos.logback"

    Manage configuration files. https://github.com/typesafehub/config
    "com.typesafe"

    Comma Separated Value (CSV) and Tab Separated Value (TSV) importer. https://github.com/databricks/spark-csv
    "com.databricks"

    Command line interface (CLI). https://github.com/scopt/scopt
    "com.github.scopt"

    Mysql connector. https://mvnrepository.com/artifact/mysql/mysql-connector-java
    "mysql"

BOOM! usage examples:

  spark-submit Boom-project-assembly-X.X.X-SNAPSHOT.jar script -sf input/script/test.txt
    "X.X.X" is the current program version starting from: v1.0.0.
    It process all lines of file 'input/script/test.txt'.
    ignoring comments lines that start with '#' and considering each line
    as a single BOOM! command line parameter plus arguments.

  spark-submit Boom-project-assembly-X.X.X-SNAPSHOT.jar parquet -i input/data/100.txt -o input/parquet/100
    "X.X.X" is the current program version starting from: v1.0.0.
    It converts a file, storing float numbers or strings, into Parquet format using plain data structure.
    The output will be stored at 'input/parquet/100'.
    It uses the following item of the default configuration file located at 'input/configuration/Boom.conf':
      Boom.path, Boom.computationCluster and Boom.outputConf.filename.
    The input and the output must be accessible from computation cluster and have plain structure.

  spark-submit Boom-project-assembly-X.X.X-SNAPSHOT.jar parquet -scheme -o input/parquet
    "X.X.X" is the current program version starting from: v1.0.0.
    It converts all tables, stored in the database scheme, using the JDBC data source set in the configuration file
    into Parquet format using plain data structure. Each table must store float numbers. See "Notes section".
    The output will be stored at 'input/parquet' as root path. It will be created an additional inner directory
    for each table in the scheme.
    It uses the following item of the default configuration file located at 'input/configuration/Boom.conf':
      Boom.path, Boom.computationCluster and Boom.outputConf.filename.
     The input and the output must be accessible from computation cluster and have plain structure.

  spark-submit Boom-project-assembly-X.X.X-SNAPSHOT.jar parquet -scheme -tableList "NIR_CR_Ht01","NIR_CR_Ht02" -o input/parquet
    "X.X.X" is the current program version starting from: v1.0.0.
    It converts the list of tables: "t1,t2", stored in the database scheme "test", using the JDBC data source set in
    the configuration file, into Parquet format using plain data structure.
    The output will be stored at 'input/parquet' as root path. It will be created an additional inner directory
    for the scheme and for each table.
    It uses the following item of the default configuration file located at 'input/configuration/Boom.conf':
      Boom.path, Boom.computationCluster, Boom.database.jdbc and Boom.outputConf.filename.
     The input and the output must be accessible from computation cluster and have plain structure.

  spark-submit Boom-project-assembly-X.X.X-SNAPSHOT.jar stat -suffix "_test"
    "X.X.X" is the current program version starting from: v1.0.0.
    It calculates a set of basic statistical descriptive measures on the input data in Parquet format.
    The string "_test" will be added to to application name shown I the Spark report web page.
    It uses the following item of the default configuration file located at 'input/configuration/Boom.conf':
      Boom.path, Boom.computationCluster and Boom.outputConf filename.
     The input and the output must be accessible from computation cluster and have plain structure.

  spark-submit Boom-project-assembly-X.X.X-SNAPSHOT.jar kmeans -k 2 -max 20 -run 5 -ep 1e-4 -colList "data_01","data_10"
    "X.X.X" is the current program version starting from: v1.0.0.
    It applies the paralleled no supervised clustering algorithm K-means to input data in parquet format.
    Used parameters: partitions=2, maxIterations= 20, runs=5 and epsilon=1e-4 and applying only to column names: "data_01" and "data_10".
    All columns must exist and previously stored in Parquet format.
    It uses the following item of the default configuration file located at 'input/configuration/Boom.conf':
      Boom.path, Boom.computationCluster and Boom.outputConf.filename.
     The input and the output must be accessible from computation cluster and have plain structure.

  spark-submit Boom-project-assembly-X.X.X-SNAPSHOT.jar kNearestNeighbor -k 5 -scale 10 -colA "data_01" -colB "data_02"
    "X.X.X" is the current program version starting from: v1.0.0.
    It applies the K-nearest neighbor graph algorithm to input data in parquet format.
    Used parameters: clusters (k)=5, graph scale=10  column names: "data_01" and "data_02".
    All columns must exist and previously stored in Parquet format.
    It uses the following item of the default configuration file located at 'input/configuration/Boom.conf':
      Boom.path, Boom.computationCluster and Boom.outputConf.filename.
     The input and the output must be accessible from computation cluster and have plain structure.

  spark-submit Boom-project-assembly-X.X.X-SNAPSHOT.jar plot -x "data_01" -y_1 "data_02","data_04" -y_2 "data_05"
    "X.X.X" is the current program version starting from: v1.0.0.
    It build a gnuplot file (http://gnuplot.sourceforge.net/) using as x-axis the column called "data_01".
    The columns "data_02" and "data_04" will be shown in the y_1 axis and in the y_2 axis the "data_05".
    All columns must exist and previously stored in Parquet format.
    It uses the following item of configuration file located at 'input/configuration/Boom.conf':
      Boom.path, Boom.computationCluster and Boom.outputConf.
     The input and the output must be accessible from computation cluster and have plain structure.
"""
  //-------------------------------------------------------------------------
  //note message
   private val noteMessage : String = """
Notes section:
Th input table (before applying parquet format) must have at least one column of type string indicating
the name of the object. This name must be unique. If more than one string columns are presented, only the first
one will be considered as object name. The remaining no string columns, must contain double values.
The item divider must be a tabulator or a comma.
The first line will be considered the header of the table: the column name list
Comments are allowed starting with character #

#An example with tabulator divider
Star_name  data_01  data_02
star_1     1.000  	5.451

#An example with comma divider
Star_name,data_01,data_02
star_1,1.000,5.451

Using default Spark configuration parameters except for
the ones set in the configuration file and followings assignments in the source code:

spark.submit.deployMode         set to cluster
spark.serializer                set to org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer     set to 1mb

For additional Spark parameter configuration see htt://spark.apache.org/docs/latest/configuration.html
"""

//-------------------------------------------------------------------------
//license message
   private val licenseMessage : String ="""
Copyright [IAA-CSIC.2016] [Rafael Morales(rmorales@iaa.es) and Matilde Fernández (matilde@iaa.es)]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""
  //-------------------------------------------------------------------------
  // parser definition
  //-------------------------------------------------------------------------
   /** Get the BOOM! parser configuration
    *  @constructor create a new parser based on the configuration parameters  
    */
   private val parserConfiguration = new OptionParser[ParserConf]("boom") {
    //-------------------------------------------------------------------------
    //head
    head("Boom","Big data on our M starts." +"\n" +
          info+"\n" +
          usageExampleList
    )

    //-------------------------------------------------------------------------
    //version
    version("version")
      .abbr("v")
      .text("shows program version")

    //-------------------------------------------------------------------------
    //help
    help("help")
      .abbr("h")
      .text("prints this usage text")

    //-------------------------------------------------------------------------
    //command script
    cmd(scriptCommand.name)
      .action { (_, c) => c.copy(command = scriptCommand.name) }
      .text("""It loads a file that includes a list of lines that can store a valid command a comment.
      Comment line starts with # and it will be ignored. Each line will be considered as a input parameter
      of the command line""")
      .children(
        opt[String]("scriptFile")
          .abbr("sf")
          .required()
          .action{(x,parserConf) => parserConf.copy(scriptFile = x) }
          .text("input script file to be processed")
      )

    //-------------------------------------------------------------------------
    //configuration file
    opt[String]("configuration")
       .abbr("conf")
       .optional()
       .action{(x,parserConf) => parserConf.copy(configurationFile = x) }
       .text("path of the configutation file. Default value: 'input/configuration/Boom.conf'")

    //-------------------------------------------------------------------------
    //application suffix
    opt[String]("applicationSuffix")
       .abbr("suffix")
       .optional()
       .action{(x,parserConf) => parserConf.copy(applicationSuffix = x) }
       .text("string to be added to the name of application. It will be shown in the Spark web pages\n")

    //-------------------------------------------------------------------------
    //command parquet
    cmd(parquetCommand.name)
      .action { (_, c) => c.copy(command = parquetCommand.name) }
      .text("""  It Converts a input file data into Parquet format.
  Option list:""")
      .children(

        opt[String]("parquetInputFile")
         .abbr("i")
         .optional()
         .action{(x,parserConf) => parserConf.copy(parquetInputFile = x) }
         .text("input file in TSV format (Tab Separated Value)")

        , opt[String]("parquetOutputDirectory")
          .abbr("o")
          .optional()
          .action{(x,parserConf) => parserConf.copy(parquetOutputDirectory = x)}
          .text("output file directory in Parquet format")

        , opt[Unit]("parquetInputItemDividerComma")
          .abbr("comma")
          .optional()
          .action{(x,parserConf) => parserConf.copy(parquetInputItemDividerComma = true) }
          .text("use as item divider of the input table a comma: ','. By default a tabulator: '/\t' \n")

        , opt[Unit]("parquetScheme")
          .abbr("scheme")
          .optional()
          .action{(x,parserConf) => parserConf.copy(parquetScheme = true)}
          .text("use the database scheme as input")

        , opt[Seq[String]]("parquetTableList")
          .abbr("tableList")
          .optional()
          .action{(x,parserConf) => parserConf.copy(parquetTableList = x)}
          .text("use the table list as input\n")
      )

    //-------------------------------------------------------------------------
    //command stat
    cmd(statCommand.name)
      .action { (_, c) => c.copy(command = statCommand.name) }
      .text("""It calulates a set of descriptive measures on input data in parquet format.
It is calculated for each numeric column: count, mean, standard deviation, min, max and correlation (regarding to remain columns)
A Graph with the correlation it is also generated in gexf format. It can be loaed with Gephi: https://gephi.org/
Option list:""")

      .children(
        opt[Unit]("spearman")
          .abbr("sp")
          .optional()
          .action{(x,parserConf) => parserConf.copy(spearman = true)}
          .text("use Spearman correlation. By default Pearson\n")
        )

    //-------------------------------------------------------------------------
    //command K-Means||
    cmd(kMeansCommand.name)
      .action { (_, c) => c.copy(command = kMeansCommand.name) }
      .text("""It applies the parallelized no supervised clustering algorithm K-means to input data in parquet format
Option list:""")

      .children(
        opt[Int]("clusterCount")
          .abbr("k")
          .required()
          .action{(x,parserConf) => parserConf.copy(kMeansK = x)}
          .text("numbers of clusters (partitions) that group the data")

        , opt[Int]("maxIterations")
          .abbr("max")
          .required()
          .action{(x,parserConf) => parserConf.copy(kMeansIteration = x)}
          .text("max iteration to run used in K-means||")

        , opt[Int]("runsCount")
          .abbr("run")
          .required()
          .action{(x,parserConf) => parserConf.copy(kMeansRun = x)}
          .text("number of times to run the algorithm K-means||")

        , opt[Double]("epsilon")
          .abbr("ep")
          .required()
          .action{(x,parserConf) => parserConf.copy(kMeansEpsilon = x)}
          .text("determines the distance threshold within which we consider K-means has converged")

        , opt[Seq[String]]("columnList")
          .abbr("colList")
          .optional()
          .action{(x,parserConf) => parserConf.copy(kMeansColName = x)}
          .text("name of the columns used in the algorithm. Be default all columns\n")
        )

    //-------------------------------------------------------------------------
    //command kNearestNeighbor
    cmd(kNearestNeighborCommand.name)
      .action { (_, c) => c.copy(command = kNearestNeighborCommand.name) }
      .text("""It applies the K-Nearest neighbors graph algorithm to input data in parquet format.
This algoritm has computation complexity of: of n*n, where n is the number of vertexes.
The user can specify name of the columns used as axis values in a 2 dimension (2 columns)
or 3 dimension (3 columns) to locate an object. The last 3 dimension (3rd column name) is optional.
The output is a graph that can be loaed with Gephi: https://gephi.org/
  Option list:""")
      .children(
        opt[Int]("clusterCount")
          .abbr("k")
          .required()
          .action{(x,parserConf) => parserConf.copy(kNearestNeighborK = x)}
          .text("numbers of clusters (partitions) that group the data")

        , opt[Int]("kNearestNeighScale")
          .abbr("scale")
          .required()
          .action{(x,parserConf) => parserConf.copy(kNearestNeighScale = x)}
          .text("scale of the values of the vertexes (i.e. 10)")

        , opt[String]("kNearestNeighborColA")
          .abbr("colA")
          .required()
          .action{(x,parserConf) => parserConf.copy(kNearestNeighborColA = x)}
          .text("name of the column A used as X axis")

        , opt[String]("kNearestNeighborColB")
          .abbr("colB")
          .required()
          .action{(x,parserConf) => parserConf.copy(kNearestNeighborColB = x)}
          .text("name of the column B used as Y axis")

        , opt[String]("kNearestNeighborColC")
          .abbr("colC")
          .optional()
          .action{(x,parserConf) => parserConf.copy(kNearestNeighborColC = x)}
          .text("name of the column B used as Z axis\n")
        )

      //-------------------------------------------------------------------------
    //command plot
    cmd(plotCommand.name)
      .action { (_, c) => c.copy(command = plotCommand.name) }
      .text("""It creates a gnuplot script file using a set of colums as 'x' and 'y' axis. It is possible to define
two groups of columns ('y_1' and 'y_2') with a similar scale. The scound group ('y_2') is optional""")

      .children(
        opt[String]("x-axis")
          .abbr("x")
          .required()
          .action{(x,parserConf) => parserConf.copy( plot_x_axis = x)}
          .text("Define the name of the column used as x-axis\n")
        )

      .children(
        opt[Seq[String]]("y_1_axis_list")
          .abbr("y_1")
          .required()
          .action{(x,parserConf) => parserConf.copy(plot_y_1_axis_list = x)}
          .text("Define the name of the columns to be drawn in the y_1 axis\n")
        )

      .children(
        opt[Seq[String]]("y_2_axis_list")
          .abbr("y_2")
          .optional()
          .action{(x,parserConf) => parserConf.copy(plot_y_2_axis_list = x)}
          .text("Define the name of the columns to be drawn in the y_2 axis\n")
        )

    //-------------------------------------------------------------------------
    //Note message
    note(noteMessage + licenseMessage)

    //-------------------------------------------------------------------------
    //modify parser behavior

    override def renderingMode = scopt.RenderingMode.OneColumn

    //-------------------------------------------------------------------------
  } //end of 'ParserConf' class

  //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
  /** Parse a user's argument list 
    @param User's argument list
    @return A BOOM! parser configuration object
            . None is some error were found creating the configuration object
  */
  
  def parse(userArgumentList: Array[String]) : Option[ParserConf] = {
    parserConfiguration.parse(userArgumentList
      , ParserConf())
  }

  //-------------------------------------------------------------------------
  //-------------------------------------------------------------------------
} //end of object 'myCommandLineParser'

//=============================================================================
// End of 'myCommandLineParser.scala' file
//=============================================================================
