val sparkVersion = "2.2.0"

val sharedSettings = Seq(
  organization := "example",
  scalaVersion := "2.11.11",
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-Xlint"),
  resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases"
  )
)

lazy val core = (project in file("core"))
  .settings(
    sharedSettings,
    name := "udfs-core",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,

      "ch.qos.logback" % "logback-classic" % "1.1.7",

      "org.scalatest" %% "scalatest" % "2.2.6" % Test
    )
  )

lazy val bench = (project in file("bench"))
  .enablePlugins(JmhPlugin)
  .configs(IntegrationTest)
  .settings(
    name := "udfs-benchmark",
    sharedSettings,
    Defaults.itSettings,
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % IntegrationTest,
    IntegrationTest / parallelExecution := false,
    logBuffered := false
  )
  .dependsOn(core)