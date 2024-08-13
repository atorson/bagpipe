import java.io.{File, FileOutputStream}

import sbt._
import Keys._
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.docker.DockerPlugin
import play.twirl.sbt.SbtTwirl
import sbtassembly.AssemblyPlugin.autoImport._
import sbtprotoc.ProtocPlugin
import sbtprotoc.ProtocPlugin.autoImport.PB


object BagpipeBuild extends Build {

  lazy val root = project.in(file(".")) aggregate(gen, frm, ex)

  lazy val gen = project.in(file("generator")).enablePlugins(SbtTwirl)

  lazy val persistentEntityGenerator = TaskKey[Seq[File]]("twirl-entities", "Generates Persistent Entities via TWIRL")

  lazy val frm = project.in(file("framework"))
    .settings(Defaults.coreDefaultSettings
      ++ getSourceGeneratorSettings
      ++ getAssemblySettings)
    .dependsOn(gen).enablePlugins(ProtocPlugin,JavaAppPackaging,DockerPlugin)

	
  lazy val ex = project.in(file("example"))
    .settings(Defaults.coreDefaultSettings
      ++ getSourceGeneratorSettings
      ++ getAssemblySettings
      ++ Seq(
        PB.includePaths in Compile += file(s"${(resourceDirectory in Compile in frm).value.absolutePath}/proto")
      ))
    .dependsOn(gen).dependsOn(frm % "compile->compile;test->test").enablePlugins(ProtocPlugin,JavaAppPackaging,DockerPlugin)

  def getSourceGeneratorSettings() = {
    Seq(
      PB.protoSources in Compile := Seq[File](file(s"${(resourceDirectory in Compile).value.absolutePath}/proto")),
      PB.targets in Compile := Seq(
        //PB.gens.java -> file(s"${(sourceDirectory in compile in Compile).value.toString}/gen/java"),
        scalapb.gen(flatPackage = true,
          javaConversions = false,
          grpc = true,
          singleLineToString = true) ->  (sourceManaged in Compile).value),
      sourceGenerators in Compile <+= (persistentEntityGenerator in Compile),
      persistentEntityGenerator in Compile <<= (
        sourceManaged in Compile,
        resourceDirectory in Compile,
        dependencyClasspath in Compile in gen,
        dependencyClasspath in Compile,
        streams
        ) map {
        (src, res, gp, cp, st) => runMyCodeGenerator(
          file(s"${src.absolutePath}/scala"),
          file(s"${res.absolutePath}/schema"),
          gp.files ++ cp.files, st.log)
      }
    )
  }


  def runMyCodeGenerator(sourceDir: File, resourceDir: File, cp: Seq[File], log: sbt.Logger): Seq[File] = {
    val mainClass = "net.andrewtorson.bagpipe.gen.TwirlPersistentEntityGenerator"
    val tmp = File.createTempFile("sources", ".txt")
    val os = new FileOutputStream(tmp)
    log.info("Running TwirlPersistentEntityGenerator...")
    try {
      val fork =  new Fork.ForkScala(mainClass)
      val JvmOptions = Nil
      /*scala.Seq[scala.Predef.String](
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005")*/
        val i = fork.fork(None, JvmOptions, cp,
        Seq(s"${sourceDir.toString}", s"${resourceDir.toString}"),
        None,
        false,
        CustomOutput(os)).exitValue()

     if (i != 0) {
        sys.error("Trouble running PersistentEntityGenerator")
      }
    } finally {
      os.close()
    }
    //println(scala.io.Source.fromFile(tmp).getLines)
    val result = scala.io.Source.fromFile(tmp).getLines.map(f ⇒ file(f)).toList
    result
  }

  def getAssemblySettings = {
    val slash = System.getProperty("file.separator")
    Seq(
      test in assembly := {},
      assemblyMergeStrategy in assembly := {
        case x if x.contains(s"bagpipe.conf") => MergeStrategy.first
        case x if x.contains(s"logback.xml") => MergeStrategy.first
        case x if x.contains(s"postgresql-schema.sql") => MergeStrategy.first
        case x if x.contains(s"javax${slash}annotation") => MergeStrategy.first
        case x => {
          val oldStrategy = (assemblyMergeStrategy in assembly).value
          oldStrategy(x)
        }
    }
    )
  }

}