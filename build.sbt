name := "top-songs"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % "test"
libraryDependencies += "org.rogach" %% "scallop" % "5.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.16" % Test

scalacOptions ++= Seq("-release", "8")

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) =>
    MergeStrategy.filterDistinctLines
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

assembly / mainClass := Some("com.kfedorov.topsongs.cli.Main")
