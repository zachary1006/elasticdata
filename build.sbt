name := "elasticdata"

version := "0.1"

scalaVersion := "2.12.5"

val elastic4sVersion = "6.1.3"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % elastic4sVersion

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.5"

libraryDependencies ++= Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,

  // for the http client
  "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,

  // for the http client
  "com.sksamuel.elastic4s" %% "elastic4s-tcp" % elastic4sVersion,

  // a json library
  "com.sksamuel.elastic4s" %% "elastic4s-jackson" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-play-json" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-circe" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-json4s" % elastic4sVersion,

  // testing
  "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % "test",
  "com.sksamuel.elastic4s" %% "elastic4s-embedded" % elastic4sVersion % "test",

  // json4s support
  "org.json4s" %% "json4s-native" % "3.6.0-M2"
)