version       := "0.0.1"

scalaVersion  := "2.11.8"

scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation", "-encoding", "utf8")


resolvers ++= Seq(
  "snapshots"           at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"            at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "OSS Sonatype" at "https://repo1.maven.org/maven2/"
)

libraryDependencies ++= {
  val jacksonVer = "2.7.3"
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVer,
    "com.turn" % "shapeshifter" % "1.1.1"
  )
}

