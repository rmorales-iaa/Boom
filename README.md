# Boom
BOOM! (Big data On Our M starts) is a application to process data from M-starts using CatSpark tool (based on Spark 1.6.2).

#Requirements

  1) Using Boom! 
  
      a) Java 1.6 or newer: Oracle version (preferred): www.java.com/en/download/
                            Open source version (alternative): http://openjdk.java.net/install/ 
      b) Apache Spark 1.6.2 : http://spark.apache.org/downloads.html
      
  2) Compiling Boom!
  
      a) Java 1.6 or newer (Oracle version (preferred): www.java.com/en/download/
                            Open source version (alternative): http://openjdk.java.net/install/ 
      b) Apache Spark 1.6.2 : http://spark.apache.org/downloads.html      
      c) Scala 2.11.8 or newer: http://www.scala-lang.org/download/      
      d) sbt 0.13.12 or newer: http://www.scala-sbt.org/download.html

The working flow is simple:

   1) Set the cluster configuration in "input/configuration/Boom.conf". If no cluster avialable, then set to run in local execution: "computationCluster.runInRemote = true" and indicate the number of cores and ram to be used in "computationCluster.local"
   2) Ensure that the input data has a valid format. See "General Comments" just below
   3) Convert the data to Parquet format 
   4) Process the data 

The data need to be converted once. An large example of valida data can be found in "input/data/1000.txt"

A simple example of using Boom! for converting data and calcualate a basic statistics measures:

./run. parquet -i input/data/1000.txt -o input/parquet/1000
./run  stat

#CatSpatk
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

#BOOM! usage examples

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
     
     
#Notes section
Th input table (before applying parquet format) must have at least one column of type string indicating
the name of the object. This name must be unique. If more than one string columns are presented, only the first
one will be considered as object name. The remaining no string columns, must contain double values.
The item divider must be a tabulator or a comma.
The first line will be considered the header of the table: the column name list
Comments are allowed starting with character #

\#An example with tabulator divider

Star_name  data_01  data_02

star_1     1.000  	5.451

\#An example with comma divider

Star_name,data_01,data_02

star_1,1.000,5.451

Using default Spark configuration parameters except for
the ones set in the configuration file and followings assignments in the source code:

spark.submit.deployMode         set to cluster
spark.serializer                set to org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer     set to 1mb

For additional Spark parameter configuration see htt://spark.apache.org/docs/latest/configuration.html


