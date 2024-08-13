addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.1.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-twirl" % "1.1.1")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.1")
libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.5.42"
logLevel := Level.Warn
