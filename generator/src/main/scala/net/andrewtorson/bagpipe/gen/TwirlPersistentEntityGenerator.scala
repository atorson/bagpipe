/*
 * Author: Andrew Torson
 * Date: Oct 25, 2016
 */

package net.andrewtorson.bagpipe.gen

import java.io.{File, PrintWriter}

import net.andrewtorson.bagpipe.gen.templates.txt.EntityTemplate


object TwirlPersistentEntityGenerator {

  def main(args: Array[String]): Unit ={
    if (args.size == 2) {
      //Thread.sleep(10000)
      val rootSource = s"${args(0)}"
      val schemaPath = args(1)
        try{
           val rootResource = new File(schemaPath)
           val rootSourceDir = new File(rootSource)
           if (rootResource.isDirectory && (rootSourceDir.exists() || rootSourceDir.mkdirs())) {
             for (file <- rootResource.listFiles()) {
              if (file.isFile) {
                val name = file.getName
                val shortName = name.take(name.lastIndexOf('.'))
                val sourceName = s"$rootSource/$shortName.scala"
                val writer = new PrintWriter(new File(sourceName))
                try {
                  val schema = net.andrewtorson.bagpipe.gen.TypeSchema.fromJson(file.getAbsolutePath)
                  val code = EntityTemplate(schema).toString()
                  writer.write(code)
                  println(sourceName)
                } catch {
                  case x: Throwable => {
                    sys.error(s"Trouble with persistent entity generator execution: $x")
                  }
                } finally {
                  writer.close()
                }
              }
             }
           } else {
             sys.error(s"Persistent entity schema and/or generated source folder is invalid")
           }
        } catch {
          case x: SecurityException => {
            sys.error(s"Trouble with persistent entity schema and/or generated source folders access permissions: $x")
          }
        }
    } else {
      sys.error(s"Trouble with persistent entity generator input: $args")
    }
  }
}
