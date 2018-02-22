name := "spark-udfs-vs-sql"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,

  "com.storm-enroute" %% "scalameter-core" % "0.8.2",
  "com.storm-enroute" %% "scalameter" % "0.8.2" % Test,

  "org.scalatest" %% "scalatest" % "2.2.6" % Test
)