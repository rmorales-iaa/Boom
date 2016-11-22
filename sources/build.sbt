//=============================================================================
//General comment
//=============================================================================
//After editing this file run "sbt eclipse" where *.sbt file is stored
//to generate a project compatible with eclipse

//=============================================================================

//add extra settings to be added to BuildInfoKey (see below)
// to be available in scala source
lazy val authorList =         SettingKey[String]("authorList")
lazy val license =            SettingKey[String]("license")
lazy val myGitCurrentBranch = SettingKey[String]("myGitCurrentBranch")
lazy val myGitHeadCommit =    SettingKey[String]("myGitHeadCommit")

//common settings
//version will be calculated using git repository
lazy val commonSettings = Seq(
    name :=                 "Boom-project"
    , description :=        "Boom! Big data on our M starts"
    , organization :=       "IAA-CSIC"
    , authorList  :=        "Rafael Morales (rmorales@iaa.es) and Matilde Fernández (matilde@iaa.es)"
    , homepage :=           Some(url("http://www.iaa.csic.es"))
    , license :=            "This project is licensed under the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)"
    , myGitCurrentBranch := git.gitCurrentBranch.value
    , myGitHeadCommit :=    git.gitHeadCommit.value.get
)

//Main class
lazy val mainClassName = "Main"

//fix scalaVersion 
scalaVersion := "2.11.8"

//fix version in iv: agile dependency manager
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }


//=============================================================================
//library dependences

//dependences managed by sbt-spark-package plugin
sparkComponents ++= Seq(
    "sql"
    ,"hive"  //sql extension
    ,"mllib" //machine learning
    ,"graphx" //machine learning
)
lazy val dependenceList = Seq(

    //logging. https://github.com/typesafehub/scala-logging
    "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0"    

    //logging backend. Add the excludeAll clause to avoid transitivities in the dependence library tree. https://github.com/typesafehub/scala-logging
    ,"ch.qos.logback" % "logback-classic" % "1.1.7" excludeAll(ExclusionRule(organization = "org.slf4j"))
    
    //manage configuration files. https://github.com/typesafehub/config
    ,"com.typesafe" % "config" % "1.3.0"

    //comma Separated Value (CSV) and Tab Separated Value (TSV) importer. https://github.com/databricks/spark-csv
    ,"com.databricks" %% "spark-csv" % "1.4.0"

    //Command line interface (CLI). https://github.com/scopt/scopt
    ,"com.github.scopt" %% "scopt" % "3.5.0"
  
    //Mysql conenctor. https://mvnrepository.com/artifact/mysql/mysql-connector-java
    //Working with When is raised error:   "java.lang.OutOfMemoryError: GC overhead limit exceeded"
    // Add '--driver-memory Zg'  where Z is the new size in GB as 1st parameter of spark-submit
    // Partitions are not considered to manage generic tables
    //,"mysql" % "mysql-connector-java" % "6.0.3"
    ,"mysql" % "mysql-connector-java" % "5.1.39"
 )

//remove the library slf4j-log4j12-1.7.10.jar to avoid multiple SLF4J bindings
//the only SLF4J binding will be at logback-classic-1.1.7.jar [ch.qos.logback.classic.util.ContextSelectorStaticBinder]
libraryDependencies := libraryDependencies.value.map(_.exclude("org.slf4j", "slf4j-log4j12"))

//=============================================================================
//resolvers

resolvers ++= Seq(
    Resolver.sonatypeRepo("public")
)

//=============================================================================
//build info options. check the generated scala file: BuildInfo.scala
buildInfoOptions += BuildInfoOption.BuildTime
buildInfoKeys += buildInfoBuildNumber


//=============================================================================
//merge strategy
assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) {
  (old) => {
    case x if x.startsWith("META-INF") => MergeStrategy.discard
    case "about.html" => MergeStrategy.rename
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x => old(x)
  }
}


//=============================================================================
//root project
lazy val root = (project in file("."))
    .enablePlugins(BuildInfoPlugin,
                   GitVersioning)
    .settings(commonSettings: _*)
    .settings(libraryDependencies ++= dependenceList)
    .settings(mainClass in (assembly) := Some(mainClassName))    
    .settings(buildInfoKeys := Seq[BuildInfoKey](name
                                                , version
                                                , description
                                                , organization
                                                , authorList
                                                , license
                                                , scalaVersion
                                                , sbtVersion
                                                , buildInfoBuildNumber
                                                , myGitCurrentBranch
                                                , myGitHeadCommit)
              , buildInfoPackage := "BuildInfo")   //Name of the file and package. 
                                                   //File generated at 
                                                   //'target/scala-2.11/src_managed/main/sbt-buildinfo/BuildInfo.scala'

//=============================================================================
//=============================================================================
//End of file
//=============================================================================
