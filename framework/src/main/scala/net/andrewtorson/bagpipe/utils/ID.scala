/*
 *
 * Author: Andrew Torson
 * Date: Oct 4, 2016
 */

package net.andrewtorson.bagpipe.utils


import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{Semaphore}

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.MINUTES

import akka.Done
import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging


/**
 * Created by Andrew Torson on 10/4/16.
 */

object ID {

  val randomPrefix = "Bagpipe-RND:"

  def apply(parentID: ID, hashedID: UUID) = new HashedID(parentID, hashedID)

  def apply(parentID: ID, nameKeyID: String): NameKeyID  = new NameKeyID(parentID, nameKeyID)

  def apply(parentID: ID, numKeyID: Long) = new NumKeyID(parentID, numKeyID)

  def apply(parentID: ID, parentCompanionID: ID): RelationID = new RelationID(parentID, parentCompanionID)

  def apply(categoryName: String) = new CategoryID(categoryName)

  def apply(parentID: ID): NameKeyID  = apply(parentID, s"$randomPrefix${UUID.randomUUID().toString}")

  val ActorCategory = new CategoryID("A")

  val EntityCategory = new CategoryID("E")

  val ConnectionCategory = new CategoryID("C")

  val FlowCategory = new CategoryID("F")

  val UtilityCategory = new CategoryID("U")

  val MessageCategory = new CategoryID("M")

}


trait ID extends Serializable with Equals {

  val gkey: UUID

  val parentID: ID

  val parentCompanionID: Option[ID]

  def toString(slashOrder: Boolean): String

  override def toString = toString(true)

  /**
   * Finds the ancestor that is derived from all filter IDs
   * @param filter IDs to match on
   * @param drillDown if false then the topmost qualified ancestor is returned
   * if true then after finding topmost ancestor, the drilldown in its direct successors branch happens and the last child is returned
   * @return None, if there is no match in filter
   */
  def findAncestor(filter: Seq[ID], drillDown: Boolean = false): Option[ID] = {
    val f = internalFindAncestor(filter, 0, drillDown)
    f match {
        case Left(x) => Some(x._1)
        case Right(_) => None
    }
  }

  def isRoot() = (this == parentID)

  private def internalFindAncestor(filter: Seq[ID], curDist: Int, drillDown: Boolean): Either[(ID, Int, Boolean), mutable.BitSet] = {
    import scala.collection.mutable.BitSet
    if (filter.size == 1 && filter.contains(this)) getLeft(this, curDist, shouldDrill(drillDown, curDist)) else {
      if (isRoot()) {
        val s = new BitSet(scala.collection.immutable.BitSet(Range(0, filter.size): _*).toBitMask)
        val i = filter.indexOf(this)
        if (i>=0) s -=i
        if (s.isEmpty) getLeft(this, curDist, shouldDrill(drillDown, curDist)) else getRight(s)
      } else {
        if (parentCompanionID.isDefined) {
          val pFind = parentID.internalFindAncestor(filter, curDist + 1,drillDown)
          val cFind = parentCompanionID.get.internalFindAncestor(filter, curDist + 1,drillDown)
          (pFind, cFind) match {
            case (Left(x), Left(y)) => if (x._2 >= y._2) (if (x._3) getLeft(this,x._2,shouldDrill(true, curDist)) else pFind) else
              (if (y._3)  getLeft(y._1, y._2, false) else cFind)
            case (Left((_,l,b)), Right(_)) => if (b) getLeft(this, l, shouldDrill(true, curDist)) else pFind
            case (Right(_), Left((id, l, b))) => if (b) getLeft(id, l, false) else cFind
            case (Right(sx), Right(sy)) => {
              val i = filter.indexOf(this)
              if (i>=0) sx -=i
              sx &= sy
              if (sx.isEmpty) getLeft(this, curDist, shouldDrill(drillDown, curDist)) else getRight(sx)
            }
          }
        } else {
          val pFind = parentID.internalFindAncestor(filter, curDist + 1, drillDown)
          pFind match {
            case Left((_,l,b)) => if (b) getLeft(this, l, shouldDrill(true, curDist)) else pFind
            case Right(s: BitSet) => {
              val i = filter.indexOf(this)
              if (i>=0) s -=i
              if (s.isEmpty) getLeft(this, curDist, shouldDrill(drillDown, curDist)) else getRight(s)
            }
          }
        }
      }
    }

  }

  private def shouldDrill(drillDown: Boolean, curDist: Int) = if (drillDown) (curDist > 0) else false

  private def getLeft(id: ID, dist: Int, drillDown: Boolean): Either[(ID, Int, Boolean), mutable.BitSet] = {
    Left[(ID, Int, Boolean), mutable.BitSet](id, dist, drillDown)
  }


  private def getRight(s: mutable.BitSet): Either[(ID, Int, Boolean), mutable.BitSet] = {
    Right[(ID, Int, Boolean), mutable.BitSet](s)
  }


  final protected def generateKey(namespace: UUID, childName: String): UUID = {
    val childBytes = childName.getBytes
    UUID.nameUUIDFromBytes(ByteBuffer.allocate(16 + childBytes.size).
      putLong(namespace.getMostSignificantBits).
      putLong(namespace.getLeastSignificantBits).
      put(childBytes).array)
  }
/*
  def getLineage(includeSelf: Boolean, orderFromRoot: Boolean = true): Set[Seq[ID]] = {
   if (parentID == this) {
     if (includeSelf) Set[Seq[ID]](Seq[ID](this)) else Set[Seq[ID]]()
   } else {
     var newForrest = Set[Seq[ID]]()
     if (includeSelf) {
       for (branch <- parentID.getLineage(true, orderFromRoot)) newForrest = newForrest + (if (orderFromRoot) branch :+ this else branch.+:(this))
       if (parentCompanionID.isDefined) for (branch <- parentCompanionID.get.getLineage(true, orderFromRoot)) newForrest = newForrest + (if (orderFromRoot) branch :+ this else branch.+:(this))
     } else {
       newForrest = parentID.getLineage(true, orderFromRoot)
       if (parentCompanionID.isDefined) newForrest = newForrest ++ parentCompanionID.get.getLineage(true, orderFromRoot)
     }
     newForrest
   }
  }*/
 

  override def canEqual(that: Any): Boolean = {
    that.isInstanceOf[ID]
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case id: ID => id.canEqual(this) && this.gkey.equals(id.gkey)
      case _ => false
    }
  }

  override def hashCode(): Int = gkey.hashCode()

}

object Root extends ID {

  override val parentCompanionID = None

  override val parentID = this

  override val gkey = UUID.fromString("d4d1c83d-d63a-46cf-8597-d6e8d2a0b0ba")

  override def toString(slashOrder: Boolean): String = ""
}


class NameKeyID(override val parentID: ID, val nameKey: String) extends ID {

  override val parentCompanionID = None

  override val gkey = generateKey(parentID.gkey, nameKey)

  override def toString(slashOrder: Boolean): String = if (slashOrder) (nameKey + "/" + parentID.toString(slashOrder)) else (parentID.toString(slashOrder) + "\\" + nameKey)
}

class NumKeyID(override val parentID: ID, val numKey: Long) extends ID {

  override val parentCompanionID = None

  override val gkey = generateKey(parentID.gkey, numKey.toString)

  override def toString(slashOrder: Boolean): String = if (slashOrder) (numKey + "/" + parentID.toString(slashOrder)) else (parentID.toString(slashOrder) + "\\" + numKey)
}

class HashedID(parent: ID, hashedId: UUID) extends ID {

  override val parentID = this

  override val parentCompanionID = None

  override val gkey = generateKey(parent.gkey, hashedId.toString)

  override def toString(slashOrder: Boolean):  String = gkey.toString
}


class RelationID(override val parentID: ID, val parentCompanion: ID) extends ID {

  require(parentID != parentCompanion, "Parent and its companion must be different")

  override val parentCompanionID = Some(parentCompanion)

  override val gkey = generateKey(parentID.gkey, parentCompanion.gkey.toString)

  override def toString(slashOrder: Boolean):  String = "[" + parentID.toString(slashOrder) + "~" + parentCompanion.toString(slashOrder) +"]"
}

class CategoryID(val categoryName: String) extends ID {

  override val parentCompanionID = None

  override val parentID = Root

  override val gkey = generateKey(Root.gkey, categoryName)

  override def toString(slashOrder: Boolean): String = categoryName

}

trait RegistryContext[T, S]{

    /**
   * Atomic mutation operation: registers the new value and returns the old one, if any
   * Note: no calbacks are fired if the new value is equal to the old one, if any
   * Note: remove callbacks are fired if there was an existing value
   * @param id
   * @param value
   * @return
   */
  def add(id: ID, value: T, callbackArg: S): Option[T]

  /**
   * Atomic mutation operation: always removes the registered value and returns it, if any
   * Note: no calbacks are fired if there was no existing value
   * @param id
   * @return
   */
  def remove(id: ID, callbackArg: S): Option[T]

  /**
   * Atomic read operation: just returns the old value, if any
   * @param id
   * @return
   */
  def get (id: ID): Option[T]

  /**
   * Dynamically-added single fire-and-forget handler that will be executed on next Add of the given ID
   * @param id
   * @param handler
   */
  def fireOnNextAdd(id: ID, handler: (T, S) => Unit, executionContext: Option[ExecutionContext] = None)

  /**
   * Dynamically-added single fire-and-forget handler that will be executed on next Add of the given ID
   * @param id
   * @param handler
   */
  def fireOnNextRemove(id: ID, handler: (T, S) => Unit, executionContext: Option[ExecutionContext] = None)

/*
  /**
   * Dynamically-added single fire-and-forget handler that will be executed on next Update of the given ID
   * @param id
   * @param handler
   */
  def fireOnNextUpdate(id: ID, handler: (T, T, S) => Unit, executionContext: Option[ExecutionContext] = None)

*/

}

sealed trait RegistryOperation[T,S]{

  val name: String

  def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]]


  def ? (next: RegistryOperation[T,S])(implicit t: ClassTag[T], s: ClassTag[S]): RegistryOperation[T,S] = {
     IfFirstThenSecond(this, next)
  }

  def - (next: RegistryOperation[T,S])(implicit t: ClassTag[T], s: ClassTag[S]): RegistryOperation[T,S] = {
    ZeroThenEitherFirstOrSecond(this, next, next)
  }


  def -<(left: RegistryOperation[T,S], right: RegistryOperation[T,S])(implicit t: ClassTag[T], s: ClassTag[S]): RegistryOperation[T,S] = {
    ZeroThenEitherFirstOrSecond(this, left, right)
  }

}

sealed trait SimpleRegistryOperation[T] {

  val outer: RegistryOperation[T,Unit]

  def apply(id: ID, state: Option[T] = None, value: Option[ () => Option[T]] = None)(implicit context: RegistryContext[T, Unit], t: ClassTag[T]): Try[Either[Option[T], Option[T]]] = {
    outer.apply(id, state, value.map[() =>(Option[T],Unit)]{f: (() => Option[T]) => {() => {
      f() match {
        case Some(v: T) => (Some(v),Unit)
        case _ => (None, Unit)
      }
    }}})(context)

  }
}


object Reg{

  def cbBuilder[T: ClassTag, S: ClassTag] = new RegistryCallbacksBuilder[T,S]()

  def cbBuilder[T: ClassTag] = new SimpleRegistryCallbacksBuilder[T]()

  class RegistryCallbacksBuilder[T: ClassTag, S: ClassTag](){

    private val set = mutable.Set[(Option[ExecutionContext], Option[(ID, T, S) => Unit], Option[(ID, T, S) => Unit])]()

    def WithAll(context: Option[ExecutionContext], add: Option[(ID, T, S) => Unit], remove: Option[(ID, T, S) => Unit]): RegistryCallbacksBuilder[T,S] = {
      set add (context, add, remove)
      this
    }

    def WithAdd(listener: (ID, T, S) => Unit, context: Option[ExecutionContext] = None): RegistryCallbacksBuilder[T,S] = WithAll(context, Some(listener),None)

    def WithRemove(listener: (ID, T, S) => Unit, context: Option[ExecutionContext] = None): RegistryCallbacksBuilder[T,S] = WithAll(context, None, Some(listener))

    def WithBoth(add: (ID, T, S) => Unit, remove: (ID, T, S) => Unit, context: Option[ExecutionContext] = None): RegistryCallbacksBuilder[T,S] =  WithAll(context, Some(add), Some(remove))

    def build(): Set[(Option[ExecutionContext], Option[(ID, T, S) => Unit], Option[(ID, T, S) => Unit])] = {
      Set[(Option[ExecutionContext], Option[(ID, T, S) => Unit], Option[(ID, T, S) => Unit])]() ++ set
    }
  }

  class SimpleRegistryCallbacksBuilder[T: ClassTag](){

    private val set = mutable.Set[(Option[ExecutionContext], Option[(ID, T) => Unit], Option[(ID, T) => Unit])]()

    def WithAll(context: Option[ExecutionContext], add: Option[(ID, T) => Unit], remove: Option[(ID, T) => Unit]): SimpleRegistryCallbacksBuilder[T] = {
      set add (context, add, remove)
      this
    }

    def WithAdd(listener: (ID, T) => Unit, context: Option[ExecutionContext] = None): SimpleRegistryCallbacksBuilder[T] = WithAll(context, Some(listener),None)

    def WithRemove(listener: (ID, T) => Unit, context: Option[ExecutionContext] = None): SimpleRegistryCallbacksBuilder[T] = WithAll(context, None, Some(listener))

    def WithBoth(add: (ID, T) => Unit, remove: (ID, T) => Unit, context: Option[ExecutionContext] = None): SimpleRegistryCallbacksBuilder[T] =  WithAll(context, Some(add), Some(remove))

    def build(): Set[(Option[ExecutionContext], Option[(ID, T) => Unit], Option[(ID, T) => Unit])] = {
      Set[(Option[ExecutionContext], Option[(ID, T) => Unit], Option[(ID, T) => Unit])]() ++ set
    }
  }

  def unwrap[T: ClassTag](operationResult: Try[Either[Option[T], Option[T]]]): Try[Option[T]] = {
      operationResult match {
        case Failure(exc) => Failure[Option[T]](exc)
        case Success(v: Left[Option[T], Option[T]]) => Success(v.a)
        case Success(v: Right[Option[T], Option[T]]) => Success(v.b)
      }
  }

   def Add[T: ClassTag, S: ClassTag]: RegistryOperation[T,S] = new RegistryOperation[T,S]  {
    override val name = "Add"
    override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
      try {
        value match {
          case Some(h) => {
            try {
              h() match {
                case (Some(v), s) => {
                  context.add(id, v, s) match {
                    case None => Success(Left[Option[T], Option[T]](Some(v)))
                    case Some(ov: T) => Success(Right[Option[T], Option[T]](Some(ov)))
                  }
                }
                case o => Failure[Either[Option[T], Option[T]]](new IllegalArgumentException(s"New value $o did not comply with the Add operation  contract"))
              }
            } catch {
              case exc: Throwable => Failure[Either[Option[T], Option[T]]](exc)
            }
          }
          case _ => Failure[Either[Option[T], Option[T]]](new IllegalArgumentException("New value is not provided to the Add operation input"))
        }
      } catch {
        case x: Throwable => Failure(x)
      }
    }
  }

  def Replace[T: ClassTag, S: ClassTag]: RegistryOperation[T,S] = new RegistryOperation[T,S]  {
    override val name = "Replace"
    override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
      try {
        value match {
          case Some(h) => {
            try {
              // may have side effects by calling h() again
              val s = h()._2
              state match {
                case Some(v: T) => Success(Left[Option[T], Option[T]](context.add(id, v, s)))
                case _ => Success(Left[Option[T], Option[T]](context.remove(id, s)))
              }
            } catch {
              case exc: Throwable => Failure[Either[Option[T], Option[T]]](exc)
            }
          }
          case _ => Failure[Either[Option[T], Option[T]]](new IllegalArgumentException("Callback arg is not provided to the Replace operation input"))
        }
      } catch {
        case x: Throwable => Failure(x)
      }
    }
  }

  def Get[T: ClassTag, S: ClassTag]: RegistryOperation[T,S] = new RegistryOperation[T,S] {
    override val name = "Get"
    override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
      try {
        Success(Left[Option[T], Option[T]](context.get(id)))
      } catch {
        case x: Throwable => Failure(x)
      }
    }
  }

  def Remove[T: ClassTag, S: ClassTag]: RegistryOperation[T,S] = new RegistryOperation[T,S] {
    override val name = "Remove"
    override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
      try {
        value match {
          case Some(h) => {
            try {
              h() match {
                case (None, s) => Success(Left[Option[T], Option[T]](context.remove(id, s)))
                case o => Failure[Either[Option[T], Option[T]]](new IllegalArgumentException(s"New value $o did not comply with the Remove operation contract"))
              }
            } catch {
              case exc: Throwable => Failure[Either[Option[T], Option[T]]](exc)
            }
          }
          case _ => Failure[Either[Option[T], Option[T]]](new IllegalArgumentException("Callback argument is not provided to the Remove operation input"))
        }
      } catch {
        case x: Throwable => Failure(x)
      }
    }
  }

  def IfAbsent[T: ClassTag, S: ClassTag]: RegistryOperation[T,S] = IfTrue[T,S](!_.isDefined)

  def Nil[T: ClassTag, S: ClassTag]: RegistryOperation[T,S] = new RegistryOperation[T,S] {
    override val name = "Nil"
    override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
      try {
        Success(Right[Option[T], Option[T]](None))
      } catch {
        case x: Throwable => Failure(x)
      }
    }
  }

  def Map[T: ClassTag, S: ClassTag](mapper: T => T): RegistryOperation[T,S] = new RegistryOperation[T,S] {
    override val name = "Map"
    override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
      try {
        state match {
          case Some(x: T) => Success(Left[Option[T], Option[T]](Some(mapper(x))))
          case None => Success(Right[Option[T], Option[T]](None))
        }
      } catch {
        case x: Throwable => Failure(x)
      }
    }
  }

  def GetOrAdd[T: ClassTag, S: ClassTag]: RegistryOperation[T,S] = Get[T,S] - IfAbsent[T,S] ? Add[T,S]

  def AddIfAbsent[T: ClassTag, S: ClassTag]: RegistryOperation[T,S] = Get[T,S] - IfAbsent[T,S].-<(Add[T,S], Nil[T,S])

  def removeIfMatches[T: ClassTag, S: ClassTag](value: Option[T]): RegistryOperation[T,S] = {
    value match {
      case None => Remove[T,S]
      case Some(v) =>  Get[T,S] - IfTrue[T,S](_ == value) ? Remove[T,S]
    }
  }

  def Callable[T: ClassTag, S: ClassTag](handler: T => Unit): RegistryOperation[T,S] = new RegistryOperation[T,S] {
    override val name = "Call"
    override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
      try {
        state match {
          case Some(v: T) => {
            handler(v)
            Success(Left[Option[T], Option[T]](state))
          }
          case _ => {
            context.fireOnNextAdd(id, (x: T, _: S) => handler(x))
            Success(Right[Option[T], Option[T]](state))
          }
        }
      } catch {
        case x: Throwable => Failure(x)
      }
    }
  }

  def Call[T: ClassTag, S: ClassTag](handler: T => Unit): RegistryOperation[T,S] = {
    Get[T,S] - Callable[T,S](handler)
  }

  def onAdd[T: ClassTag, S: ClassTag](handler: (T,S) => Unit): RegistryOperation[T,S] = Get[T,S] - (new RegistryOperation[T,S] {
    override val name = "OnAdd"
    override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
        try {
          context.fireOnNextAdd(id, handler)
          Success(Left[Option[T], Option[T]](state))
        } catch {
          case x: Throwable => Failure(x)
        }
    }
  })

  def onRemove[T: ClassTag, S: ClassTag](handler: (T,S) => Unit): RegistryOperation[T,S] = Get[T,S] - (new RegistryOperation[T,S] {
    override val name = "OnRemove"
    override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
      try {
        context.fireOnNextRemove(id, handler)
        Success(Left[Option[T], Option[T]](state))
      } catch {
        case x: Throwable => Failure(x)
      }
    }
  })

  def onUpdate[T: ClassTag, S: ClassTag](handler: (T,T,S) => Unit): RegistryOperation[T,S] = {
    val onUpdate = new RegistryOperation[T,S] {
      override val name = "onUpdate"
      override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
        try {
          state match {
            case Some(v: T) => {
              context.fireOnNextAdd(id, (x: T, s: S) => handler(v, x, s))
              Success(Left[Option[T], Option[T]](state))
            }
            case _ => Success(Right[Option[T], Option[T]](state))
          }
        } catch {
          case x: Throwable => Failure(x)
        }
      }
    }
    Get[T,S] - onUpdate
  }

  def Wrap[T: ClassTag](operation: RegistryOperation[T, Unit]) = new SimpleRegistryOperation[T] {override val outer = operation}

  def Add[T: ClassTag] = Wrap[T](Add[T,Unit])

  def Get[T: ClassTag] = Wrap[T](Get[T,Unit])

  def Remove[T: ClassTag] = Wrap[T](Remove[T,Unit])

  def Replace[T: ClassTag] = Wrap[T](Replace[T,Unit])

  def IfAbsent[T: ClassTag] = Wrap[T](IfAbsent[T,Unit])

  def Nil[T: ClassTag] = Wrap[T](Nil[T,Unit])

  def Map[T: ClassTag](mapper: T => T) = Wrap[T](Map[T,Unit](mapper))

  def GetOrAdd[T: ClassTag] = Wrap[T](GetOrAdd[T,Unit])

  def AddIfAbsent[T: ClassTag] = Wrap[T](AddIfAbsent[T,Unit])

  def removeIfMatches[T: ClassTag](value: Option[T]) = Wrap[T](removeIfMatches[T,Unit](value))

  def Callable[T: ClassTag](handler: T => Unit) = Wrap[T](Callable[T,Unit](handler))

  def Call[T: ClassTag](handler: T => Unit) = Wrap[T](Call[T,Unit](handler))

  def onAdd[T: ClassTag](handler: T => Unit) = Wrap[T](onAdd[T,Unit]((x,Unit) => handler(x)))

  def onRemove[T: ClassTag](handler: T => Unit) = Wrap[T](onRemove[T,Unit]((x,Unit) => handler(x)))

  def onUpdate[T: ClassTag](handler: (T,T) => Unit)= Wrap[T](onUpdate[T,Unit]((x,y,Unit) => handler(x,y)))

  val defaultEC: ExecutionContext = SameThreadExecutionContext

}


final case class IfFirstThenSecond[T: ClassTag,S:ClassTag](first: RegistryOperation[T,S], second: RegistryOperation[T,S]) extends RegistryOperation[T,S] {
  override val name = first.name + "?" + second.name
  override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
     try {
       first.apply(id, state, value) match {
         case Success(v: Left[Option[T], Option[T]]) => second.apply(id, v.a, value)
         case x => x
       }
     } catch {
       case x: Throwable => Failure(x)
     }
  }
}

final case class ZeroThenEitherFirstOrSecond[T: ClassTag,S:ClassTag] (zero: RegistryOperation[T,S], first: RegistryOperation[T,S], second: RegistryOperation[T,S]) extends RegistryOperation[T,S] {
  override val name = zero.name + (if (first.name == second.name) ("-" + first.name) else ("-<(" + first.name + " ," + second.name +")"))

  override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
    try {
      zero.apply(id, state, value) match {
        case Success(vl: Left[Option[T], Option[T]]) => first.apply(id, vl.a, value)
        case Success(vr: Right[Option[T], Option[T]]) => second.apply(id, vr.b, value)
        case x => x
      }
    } catch {
      case x: Throwable => Failure(x)
    }
  }
}



final case class IfTrue[T: ClassTag,S:ClassTag](predicate: Option[T] => Boolean) extends RegistryOperation[T,S] {
  override val name = if (predicate(None)) "IfAbsent" else "IfTrue"

  override def apply(id: ID, state: Option[T] = None, value: Option[ () =>(Option[T],S)] = None)(implicit context: RegistryContext[T,S]): Try[Either[Option[T], Option[T]]] = {
    try {
      if (predicate(state)) Success (Left[Option[T], Option[T]] (state)) else Success (Right[Option[T], Option[T]] (state))
    } catch {
      case x: Throwable => Failure(x)
    }
  }
}


trait RegistryFacade[T,S] {

  def >>(operation: RegistryOperation[T,S], id: ID, value:  Option[ () => (Option[T],S)] = None, state: Option[T] = None): Boolean

  def ++(operation: RegistryOperation[T,S], id: ID, value: T, callbackArg: S, state: Option[T] = None): Boolean = {
    >>(operation, id, Some({ () => (Some(value), callbackArg)}), state)
  }

  def --(operation: RegistryOperation[T,S], id: ID, callbackArg: S, state: Option[T] = None): Boolean = {
    >>(operation, id, Some({ () => (None, callbackArg)}), state)
  }

  def ?(operation: RegistryOperation[T,S], id: ID, value:  Option[ () => (Option[T],S)] = None, state: Option[T] = None): Try[Option[T]]

}

trait SimpleRegistryFacade[T] {

  def >>(operation: SimpleRegistryOperation[T], id: ID, value: Option[()=> Option[T]] = None, state: Option[T] = None): Boolean

  def ++(operation: SimpleRegistryOperation[T], id: ID, value: T, state: Option[T] = None): Boolean = {
    >>(operation, id, Some({ () => Some(value)}), state)
  }

  def --(operation: SimpleRegistryOperation[T],id: ID, state: Option[T] = None): Boolean = {
    >>(operation, id, Some({ () => None}), state)
  }

  def ?(operation: SimpleRegistryOperation[T], id: ID, value: Option[()=> Option[T]] = None, state: Option[T] = None): Try[Option[T]]

}

class ActorBasedRegistryFacade[T: ClassTag, S: ClassTag] (actorRef: ActorRef, checkAlive: Boolean = false) extends RegistryFacade[T, S] {

  implicit val defaultTimeout = Timeout(FiniteDuration(1, MINUTES))

  private var isActive = true

  import akka.pattern._

  if (checkAlive) {
    try {
      Await.ready(ask(actorRef, TerminateCallback(true, { () => {
        isActive = false
      }
      })), defaultTimeout.duration)
    } catch {
      case x: Throwable => isActive = false
    }
  } else {
    actorRef ! TerminateCallback(false, {() => {isActive = false}})
  }

  def isAlive(): Boolean = if (isActive) true else false

  override def >>(operation: RegistryOperation[T,S], id: ID, value:  Option[ () => (Option[T],S)] = None, state: Option[T] = None): Boolean =
    if (isActive) {
      actorRef ! ExecuteOperation[T, S](false, operation, id, state, value)
      true
    } else {false}

    override def ?(operation: RegistryOperation[T,S], id: ID, value:  Option[ () => (Option[T],S)] = None, state: Option[T] = None): Try[Option[T]] =
    if (isActive) {
      try {
        Await.result(ask(actorRef, ExecuteOperation[T, S](true, operation, id, state, value)), defaultTimeout.duration).asInstanceOf[Try[Option[T]]]
      } catch {
        case x: Throwable => Failure(x)
      }
    } else {Failure[Option[T]](new IllegalStateException("Target actor has been terminated"))}

}


class ActorBasedSimpleRegistryFacade[T: ClassTag] (actorRef: ActorRef, checkAlive: Boolean = false) extends SimpleRegistryFacade[T] {

  implicit val defaultTimeout = Timeout(FiniteDuration(1, MINUTES))

  private var isActive = true

  import akka.pattern._

  if (checkAlive) {
    try {
      Await.ready(ask(actorRef, TerminateCallback(true, { () => {
        isActive = false
      }
      })), defaultTimeout.duration)
    } catch {
      case x: Throwable => isActive = false
    }
  } else {
    actorRef ! TerminateCallback(false,{() => {isActive = false}})
  }

  def isAlive(): Boolean = if (isActive) true else false

  override def >>(operation: SimpleRegistryOperation[T], id: ID, value: Option[()=> Option[T]] = None, state: Option[T] = None): Boolean = {
    if (isActive) {
      actorRef ! ExecuteSimpleOperation[T](false, operation, id, state, value)
      true
    } else {false}
  }

  override def ?(operation: SimpleRegistryOperation[T], id: ID, value: Option[()=> Option[T]] = None, state: Option[T] = None): Try[Option[T]] =
    if (isActive) {Await.result(ask(actorRef, ExecuteSimpleOperation[T](true, operation, id, state, value)), defaultTimeout.duration).asInstanceOf[Try[Option[T]]]
    } else {Failure[Option[T]](new IllegalStateException("Target actor has been terminated"))}
}

class BasicRegistryContext[T: ClassTag, S: ClassTag](val registeredItems: mutable.Map[ID,T],
  activeAddPromises: mutable.Map[ID,Promise[(T,S)]],
  activeRemovePromises: mutable.Map[ID,Promise[(T,S)]],
  listeners: Set[(Option[ExecutionContext], Option[(ID, T, S) => Unit], Option[(ID, T, S) => Unit])] = Set[(Option[ExecutionContext], Option[(ID, T, S) => Unit], Option[(ID, T, S) => Unit])](),
  defaultEc: ExecutionContext = Reg.defaultEC) extends RegistryContext[T,S] {


    override def add(id: ID, value: T, callbackArg: S): Option[T] = {
    val ov = removeAndReturnOldValue(id, Some(value), callbackArg)
    if (ov != Some(value)) {
      registeredItems  += (id -> value)
      activeAddPromises get (id) match {
        case Some (p) => {
          p complete (Success((value,callbackArg)))
          activeAddPromises -= id
        }
        case _ => {}
      }
      fireListeners(true)(id, value, callbackArg)
    }
    ov
  }

  override def remove(id: ID, callbackArg: S): Option[T] = {
    val oh = removeAndReturnOldValue(id, None, callbackArg)
    /*    activePromises get (id) match {
          case Some (p) => {
            p complete (Failure(new IllegalStateException(s"Value was never assigned for id[$id]")))
            activePromises -= id
          }
          case _ => {}
        }*/
    oh
  }

  override def get(id: ID): Option[T] = {
    registeredItems get(id)
  }

  override def fireOnNextAdd(id: ID, handler: (T, S) => Unit, executionContext: Option[ExecutionContext]): Unit = {
    /*  registeredItems get (id) match {
        case None => {*/
    activeAddPromises get (id) match {
      case Some(p) => decorateFuture(p.future, handler, executionContext)
      case _ => {
        val p = Promise[(T,S)]
        activeAddPromises += (id -> p)
        decorateFuture(p.future, handler, executionContext)
      }
    }
    /* }
     case _ => {}
   }*/

  }

  override def fireOnNextRemove(id: ID, handler: (T, S) => Unit, executionContext: Option[ExecutionContext]): Unit = {

    /*  registeredItems get (id) match {
        case None => {*/
    activeRemovePromises get (id) match {
      case Some(p) => decorateFuture(p.future, handler, executionContext)
      case _ => {
        val p = Promise[(T,S)]
        activeRemovePromises += (id -> p)
        decorateFuture(p.future, handler, executionContext)
      }
    }
    /* }
     case _ => {}
   }*/
  }

  def decorateFuture(f: Future[(T,S)], handler: (T,S) => Unit, context: Option[ExecutionContext]): Unit ={
    f.onSuccess {
      case (v:T, s:S) => handler(v,s)
      case _ => {}
    }(if (context.isDefined) context.get else defaultEc)
  }

  private def removeAndReturnOldValue(id: ID, value: Option[T], arg: S): Option[T] = {
    val oh: Option[T] = registeredItems get(id)
    if (oh != value) {
      oh match {
        case Some(h) => {
          registeredItems -= id
          activeRemovePromises get (id) match {
            case Some (p) => {
              p complete (Success((h,arg)))
              activeRemovePromises -= id
            }
            case _ => {}
          }
          fireListeners(false)(id, h, arg)
        }
        case _  => {}
      }
    }
    oh
  }

  private def fireListeners(isAdd: Boolean)(id: ID, value: T, arg: S) = {
    val f = Promise.successful(Done)
    for (listener <- listeners) {
      f.future.onSuccess[Unit]{case Done =>  (if (isAdd) (if (listener._2.isDefined) listener._2.get(id, value, arg)) else
        (if (listener._3.isDefined) listener._3.get(id, value, arg)))}(if (listener._1.isDefined) listener._1.get else defaultEc)
    }
  }

}

object SimpleReg {

  def expandListeners[T](listeners: Set[(Option[ExecutionContext], Option[(ID, T) => Unit], Option[(ID, T) => Unit])]):
  Set[(Option[ExecutionContext], Option[(ID, T,Unit) => Unit], Option[(ID, T,Unit) => Unit])] = {
    val result = mutable.Set[(Option[ExecutionContext], Option[(ID, T,Unit) => Unit], Option[(ID, T,Unit) => Unit])]()
    for (listener: (Option[ExecutionContext], Option[(ID, T) => Unit], Option[(ID, T) => Unit]) <-listeners){
      result.add((listener._1,
        listener._2.map[(ID, T,Unit) => Unit]{f: ((ID, T) => Unit) => {(x:ID, y: T, z: Unit) => f(x,y)}},
        listener._3.map[(ID, T,Unit) => Unit]{f: ((ID, T) => Unit) => {(x:ID, y: T, z: Unit) => f(x,y)}}))
    }
    Set[(Option[ExecutionContext], Option[(ID, T,Unit) => Unit], Option[(ID, T,Unit) => Unit])]() ++ result
  }
}

class ConcurrentMapRegistry[T: ClassTag, S: ClassTag] (listeners: Set[(Option[ExecutionContext], Option[(ID, T, S) => Unit], Option[(ID, T, S) => Unit])], defaultEc: Option[ExecutionContext] = None)
  extends RegistryFacade[T,S]{


  import scala.collection.convert.decorateAsScala._
  import java.util.concurrent.ConcurrentHashMap

  val regContext = new BasicRegistryContext[T,S](mapAsScalaConcurrentMapConverter[ID,T](new ConcurrentHashMap()).asScala,
    mapAsScalaConcurrentMapConverter[ID, Promise[(T,S)]](new ConcurrentHashMap()).asScala,
    mapAsScalaConcurrentMapConverter[ID, Promise[(T,S)]](new ConcurrentHashMap()).asScala, listeners, defaultEc.getOrElse(Reg.defaultEC))

  val monitors = mapAsScalaConcurrentMapConverter[ID, Semaphore](new ConcurrentHashMap()).asScala

  override def >>(operation: RegistryOperation[T, S], id: ID, value: Option[() => (Option[T], S)], state: Option[T]): Boolean = {
     internalExecute(operation, id, value, state)
     true
  }

  def internalExecute(operation: RegistryOperation[T, S], id: ID, value: Option[() => (Option[T], S)], state: Option[T]): Try[Either[Option[T], Option[T]]] = {

    val s = monitors.getOrElseUpdate(id, new Semaphore(1))
    s.acquire()
    val result: Try[Either[Option[T], Option[T]]] = operation.apply(id, state, value)(regContext)
    if (!s.hasQueuedThreads) monitors.remove(id)
    s.release()
    result
  }

  override def ?(operation: RegistryOperation[T, S], id: ID, value: Option[() => (Option[T], S)], state: Option[T]): Try[Option[T]] = {
    Reg.unwrap[T](internalExecute(operation, id, value, state))
  }
}

class ConcurrentMapSimpleRegistry[T] (simpleListeners: Set[(Option[ExecutionContext], Option[(ID, T) => Unit], Option[(ID, T) => Unit])], defaultEc: Option[ExecutionContext] = None) (implicit tag: ClassTag[T])
  extends  SimpleRegistryFacade[T] {

  val outer = new ConcurrentMapRegistry[T,Unit](SimpleReg.expandListeners[T](simpleListeners), defaultEc)

  override def >>(operation: SimpleRegistryOperation[T], id: ID, value: Option[() => Option[T]], state: Option[T]): Boolean = {
    internalExecute(operation, id, value, state) match {
      case Success(_) => true
      case _ => false
    }
  }

  def internalExecute(operation: SimpleRegistryOperation[T], id: ID, value: Option[() => Option[T]], state: Option[T]): Try[Either[Option[T], Option[T]]] = {

    val s = outer.monitors.getOrElseUpdate(id, new Semaphore(1))
    s.acquire()
    val result: Try[Either[Option[T], Option[T]]] = operation.apply(id, state, value)(outer.regContext, tag)
    if (!s.hasQueuedThreads) outer.monitors.remove(id)
    s.release()
    result
  }

  override def ?(operation: SimpleRegistryOperation[T], id: ID, value: Option[() => Option[T]], state: Option[T]): Try[Option[T]] = {
    Reg.unwrap[T](internalExecute(operation, id, value, state))
  }
}



case class ExecuteOperation[T: ClassTag,S: ClassTag](isSync: Boolean,
                                 operation: RegistryOperation[T,S],
                                 id: ID, state: Option[T] = None, value: Option[() => (Option[T],S)] = None)

case class ExecuteSimpleOperation[T: ClassTag](isSync: Boolean,
                               operation: SimpleRegistryOperation[T],
                               id: ID, state: Option[T] = None, value: Option[() => Option[T]] = None)

case class TerminateCallback(isSync: Boolean, handler: ()=>Unit)

class RegistryActor[T: ClassTag, S: ClassTag] (listeners: Set[(Option[ExecutionContext], Option[(ID, T, S) => Unit], Option[(ID, T, S) => Unit])])

  extends Actor {

  protected val regContext: RegistryContext[T,S] = new BasicRegistryContext[T,S](mutable.Map[ID, T](), mutable.Map[ID, Promise[(T,S)]](),
    mutable.Map[ID, Promise[(T,S)]](), listeners, context.dispatcher)

  protected val registryTerminationHandlers = mutable.Buffer[()=>Unit]()

  override def postStop() = {
    for(handler<-registryTerminationHandlers){
      handler()
    }
  }

  def receive = {

    case exec: ExecuteOperation[T,S]=> {
      val result: Try[Either[Option[T], Option[T]]] = exec.operation.apply(exec.id, exec.state, exec.value)(regContext)
      if (exec.isSync) sender ! Reg.unwrap[T](result)
    }

    case TerminateCallback(isSync, handler) => {
      registryTerminationHandlers += handler
      if (isSync) sender ! Done
    }

    case _ => {}

  }

}



class SimpleRegistryActor[T] (simpleListeners: Set[(Option[ExecutionContext], Option[(ID, T) => Unit], Option[(ID, T) => Unit])])(implicit tag: ClassTag[T])

   extends RegistryActor[T,Unit](SimpleReg.expandListeners[T](simpleListeners)) {

  override def receive = {

    case exec: ExecuteSimpleOperation[T]=> {
      val result: Try[Either[Option[T], Option[T]]] = exec.operation.apply(exec.id, exec.state, exec.value) (regContext, tag)
      if (exec.isSync) sender ! Reg.unwrap[T](result)
    }

    case x: TerminateCallback => super.receive(x)

    case _ => {}

  }
}

object SameThreadExecutionContext extends ExecutionContext with LazyLogging{

  override def execute(runnable: Runnable): Unit = runnable.run

  override def reportFailure(t: Throwable): Unit =
    logger.warn("Computation failed in same thread execution context",t)
}