package eu.swdev.caliban

import caliban.Value.StringValue
import caliban.introspection.adt.{__EnumValue, __Type}
import caliban.schema.{PureStep, Schema, Step, Types}
import org.jooq.{Condition, DSLContext, Field}
import zio.{Has, ZIO}

import scala.reflect.ClassTag

package object jooq {

  type DslEnv        = Has[DSLContext]

  /** Accesss a DSLContext from the environment. */
  val dslCtx = ZIO.access[DslEnv](_.get)

  trait PaginatedReq {
    def orderByColumn: Option[String]
    def limit: Option[Int]
    def offset: Option[Int]
    def sortOrder: Option[String]
  }

  /** Combines a sequence of conditions into a single condition. */
  def and(seq: Option[Condition]*): Option[Condition] = seq.fold(None) {
    case (Some(c1), Some(c2)) => Some(c1.and(c2))
    case (s @ Some(_), _)     => s
    case (_, s @ Some(_))     => s
    case _                    => None
  }

  /** Extension methods that support the construction of jooq conditions. */
  implicit class OptionOps[O](val opt: Option[O]) extends AnyVal {

    /** `isLike` can only be called if the type of the captured option is `String` */
    def isLike(f: Field[String])(implicit ev: O =:= String): Option[Condition] =
      opt.map(v => f.like(s"%$v%"))

    /** `isEq` can only be used of the type of the captured option can be converted into the type of the field. */
    def isEq[F](f: Field[F])(implicit ev: Conv[O, F]): Option[Condition] =
      opt.map(v => f.eq(ev.convert(v)))
  }

  trait Conv[-X, +Y] {
    def convert(x: X): Y
  }

  object Conv {

    /** The identity converter. */
    implicit def identityConv[X]: Conv[X, X] = (x: X) => x

    /** A converter for all kinds of enums. */
    implicit val enumConf = new Conv[Enum[_], String] {
      override def convert(x: Enum[_]): String = x.name()
    }
  }

  /** Some Kotlin inspired extension methods. */
  implicit class Extension[+A](val a: A) extends AnyVal {

    /** Kotlin like let method. */
    def let[B >: A](f: A => B): B = f(a)

    /** A let method that takes an optional argument. In case the argument is `None` the current value is kept. */
    def letOpt[O](o: Option[O]) = LetOpt(a, o)

  }

  // captures the original value and the option value
  // -> this class is only needed to guide Scala's type inference.
  case class LetOpt[+A, O](a: A, o: Option[O]) {
    def apply[B >: A](f: (A, O) => B): B = o match {
      case Some(o) => f(a, o)
      case None    => a
    }
  }

  def defineEnumSchema[T <: Enum[_]: ClassTag](values: Array[T]): Schema[Any, T] = new Schema[Any, T] {
    override def toType(isInput: Boolean = false, isSubscription: Boolean = false): __Type =
      Types.makeEnum(
        Some(implicitly[ClassTag[T]].runtimeClass.getSimpleName),
        None,
        values.toList.map(v => __EnumValue(v.name, None, false, None)),
        None
      )
    override def resolve(value: T): Step[Any] = PureStep(StringValue(value.name))
  }

  case class PagedResult[V](
      totalRecords: Int,
      offset: Int,
      pagedRecords: List[V]
  )

}

