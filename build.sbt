name := "spark-groupby-helper"
organization := "com.tech1900"
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.6"
val sparkVersion = "2.3.0"
// Databricks uses 2.10.5 (as of 02/12/2016)

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided" withSources() withJavadoc(),
  "org.apache.hadoop" %% "hadoop-client" % "2.7.3" % "provided" withSources() withJavadoc(),
  // testing
  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % Test withSources() withJavadoc()
)

// allows us to include spark packages
resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"

resolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"