//Buld a fat jar with all dependences included. https://github.com/sbt/sbt-assembly
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")

//Creade a eclipse project. https://github.com/typesafehub/sbteclipse
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")

//Dependence graph tree. https://github.com/jrudolph/sbt-dependency-graph
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

//spark package plugin. https://github.com/databricks/sbt-spark-package
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.5")

//generates Scala source from your build definitions in sbt file.  https://github.com/sbt/sbt-buildinfo
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.6.1")

//manager of git versioning. https://github.com/sbt/sbt-git
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.8.5")
