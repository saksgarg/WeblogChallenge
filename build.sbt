name := "WebLogChallenge"

version := "0.1"

organization := "com.sgarg"

scalaVersion := "2.11.12"

logLevel := Level.Error

libraryDependencies ++= {
  val sparkVer = "2.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVer % "provided"
  )
}

assemblyMergeStrategy in assembly := {
  case x => MergeStrategy.first
}

logLevel in assembly := Level.Error

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := "WebLogChallenge.jar"
