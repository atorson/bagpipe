/*
 *
 * Author: Andrew Torson
 * Date: Aug 8, 2016
 */

package net.andrewtorson.bagpipe.rest



import scala.reflect.ClassTag
import scala.util.Success

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.ScalatestRouteTest
import net.andrewtorson.bagpipe.persistence.CRUD
import net.andrewtorson.bagpipe.utils.{ID, _}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{Matchers, WordSpec}
import org.specs2.mock.Mockito

trait AbstractRestTest  extends WordSpec with Matchers with ScalatestRouteTest with Mockito{

  outer =>

  trait TestActorModuleImpl extends ActorModule {
    this: ConfigurationModule =>
    val system = outer.system
  }


  def getConfig: Config = ConfigFactory.empty();

  private val registry = new ConcurrentMapSimpleRegistry[CRUD[_]](Reg.cbBuilder[CRUD[_]].build())

  private def id[E<:BaseEntity[E]](implicit ct: ClassTag[E]): ID = {
    ID(ID(ID.UtilityCategory, "MOCK_CRUD"), ct.runtimeClass.toString)
  }

  def setMock[E<:BaseEntity[E]](implicit ct: ClassTag[E]): Unit = {
    registry ++ (Reg.AddIfAbsent[CRUD[_]], id(ct), mock[CRUD[E]])
  }

  def getMock[E<:BaseEntity[E]](implicit ct: ClassTag[E]): CRUD[E] = {
    registry ? (Reg.Get[CRUD[_]], id(ct)) match {
      case Success(Some(x: CRUD[_])) => {
        x.asInstanceOf[CRUD[E]]
      }
      case _ => throw new ClassNotFoundException(s"Failed to find mock CRUD for entity ${ct.runtimeClass}")
    }
  }

}
