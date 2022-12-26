lazy val root = (project in file("."))
  .settings(
    name := "node2vec-spark",
    version := "1.0",
    scalaVersion := "2.12.8",
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.0", //% "provided",
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "com.typesafe" % "config" % "1.3.1",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.holdenkarau" %% "spark-testing-base" % "3.0.1_1.0.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "com.esotericsoftware" % "kryo" % "4.0.2",
  "it.unimi.dsi" % "fastutil" % "8.5.8",
)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

