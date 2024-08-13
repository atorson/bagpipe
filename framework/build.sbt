version       := "0.0.1"

scalaVersion  := "2.11.8"

scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation", "-encoding", "utf8")


resolvers ++= Seq(
  "snapshots"           at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"            at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "OSS Sonatype" at "https://repo1.maven.org/maven2/",
  Resolver.bintrayRepo("hseeberger", "maven")
)

libraryDependencies ++= {
  val akkaV = "2.4.16"
  val akkaHttp = "10.0.2"
  Seq(
    "com.typesafe.akka"   %%  "akka-http"    % akkaHttp,
    "com.typesafe.akka"   %%  "akka-http-testkit" % akkaHttp,
    "de.heikoseeberger" %% "akka-sse" % "2.0.0",
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.akka"   %%  "akka-stream" % akkaV,
    "com.typesafe.akka"   %%  "akka-remote" % akkaV,
    "com.github.swagger-akka-http" %% "swagger-akka-http" % "0.9.1",
    "com.typesafe.akka"   %%  "akka-testkit"  % akkaV   % "test",
    "org.specs2"          %%  "specs2-core"   % "2.3.11" % "test",
    "org.specs2"          %%  "specs2-mock"   % "2.3.11" % "test" ,
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "junit" % "junit" % "4.11" % "test",
    "io.getquill" %% "quill-async" % "0.9.0",
    "io.scalac" %%  "reactive-rabbit" % "1.1.4",
    "com.typesafe" % "config" % "1.2.1",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0",
    "ch.qos.logback" % "logback-classic" % "1.1.3",
    "com.trueaccord.scalapb" %% "scalapb-runtime" % "0.5.42" % "protobuf",
    "com.trueaccord.scalapb" %% "scalapb-json4s" % "0.1.2"


  )
}

fork := true
fork in (Test, run) := false
