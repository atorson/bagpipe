/*
 *
 * Author: Andrew Torson
 * Date: Aug 30, 2016
 */

package net.andrewtorson.bagpipe.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Created by Andrew Torson on 8/30/16.
 */
trait TestConfigurationModule extends ConfigurationModule{
  override val config: Config = {
    val configDefaults = ConfigFactory.load(this.getClass().getClassLoader(), "bagpipe-test.conf")

    scala.sys.props.get("application.config") match {
      case Some(filename) => ConfigFactory.parseFile(new File(filename)).withFallback(configDefaults)
      case None => configDefaults
    }
  }

}
