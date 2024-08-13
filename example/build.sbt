version       := "0.0.1"

scalaVersion  := "2.11.8"

scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.4.16"
  val akkaHttp = "10.0.2"
  Seq(
    "com.typesafe.akka" %% "akka-http" % akkaHttp,
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.github.swagger-akka-http" %% "swagger-akka-http" % "0.9.1",
    "io.getquill" %% "quill-async" % "0.9.0",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0",
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "junit" % "junit" % "4.11" % "test",
    "ch.qos.logback" % "logback-classic" % "1.1.3",
    "com.trueaccord.scalapb" %% "scalapb-runtime" % "0.5.42" % "protobuf"
  )
}

fork := true
fork in (Test, run) := false
