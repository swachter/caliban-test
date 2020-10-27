import java.util.{NoSuchElementException, UUID}

import ct.sql.Tables.{ACTOR, DIRECTOR, GENRE, MOVIE}
import org.jooq.{Condition, Field, Table, DSLContext => JDSLContext}
import zio.blocking.{Blocking, effectBlocking}
import zio.{Chunk, Has, ZIO, ZManaged}
import javax.sql.{DataSource => JDataSource}
import org.jooq.impl.DSL
import zio.query.{Request, ZQuery, DataSource => ZDataSource}

import scala.jdk.CollectionConverters._

package object ct {

  case class Movie(id: UUID, name: String, directorId: UUID, year: Int, genreId: UUID)
  case class Director(id: UUID, name: String)
  case class Genre(id: UUID, name: String)
  case class Actor(id: UUID, name: String)

  //
  //
  //

  def and(seq: Option[Condition]*): Option[Condition] = seq.fold(None) {
    case (Some(c1), Some(c2)) => Some(c1.and(c2))
    case (s @ Some(_), _)     => s
    case (_, s @ Some(_))     => s
    case _                    => None
  }

  sealed trait Args {
    def condition: Option[Condition]
  }

  case class MovieArgs(
                        name: Option[String] = None,
                        director: Option[UUID] = None,
                        year: Option[Int] = None,
                        fromYear: Option[Int] = None,
                        toYear: Option[Int] = None,
                        genre: Option[UUID] = None
                      ) extends Args {

    override val condition = and(
      name.map(x => MOVIE.NAME.like(s"%$x%")),
      director.map(x => MOVIE.DIRECTOR_ID.eq(x)),
      year.map(x => MOVIE.YEAR.eq(x)),
      fromYear.map(x => MOVIE.YEAR.ge(x)),
      toYear.map(x => MOVIE.YEAR.le(x)),
      genre.map(x => MOVIE.GENRE_ID.eq(x))
    )

  }

  case class DirectorArgs(name: Option[String]) extends Args {
    override val condition: Option[Condition] = name.map(x => DIRECTOR.NAME.like(s"%$x%"))
  }

  case class GenreArgs(name: Option[String]) extends Args {
    override val condition: Option[Condition] = name.map(x => GENRE.NAME.like(s"%$x%"))
  }

  case class ActorArgs(name: Option[String]) extends Args {
    override val condition: Option[Condition] = name.map(x => ACTOR.NAME.like(s"%$x%"))
  }

  type DslCtx  = Has[JDSLContext]
  type LoadEnv = Blocking with DslCtx
  type Load[X] = ZIO[LoadEnv, Throwable, X]
  type DataSource = Has[JDataSource]

  type Query[X] = ZQuery[LoadEnv, Throwable, X]

  val acquireDslContext: ZIO[Blocking with DataSource, Throwable, JDSLContext] = for {
    dataSource <- ZIO.access[DataSource](_.get)
    connection <- effectBlocking(dataSource.getConnection())
  } yield {
    DSL.using(connection)
  }

  def releaseDslContext(dslCtx: JDSLContext): ZIO[Blocking, Nothing, Unit] = effectBlocking(dslCtx.close()).orDie

  val managedDslContext: ZManaged[Blocking with DataSource, Throwable, JDSLContext] = ZManaged.make(acquireDslContext)(releaseDslContext)

  val dslCtx = ZIO.access[DslCtx](_.get)

  trait GetById[V] extends Request[Throwable, V] {
    def id: UUID
  }

  trait View {
    def id: UUID
  }

  implicit class Fetcher[X](val clazz: Class[X]) extends AnyVal {

    private def select(ctx: JDSLContext, t: Table[_]) = ctx.select(t.asterisk()).from(t)

    def fetchOne[V](ctx: JDSLContext, t: Table[_], c: Condition)(db2view: X => V) =
      db2view(select(ctx, t).where(c).fetchOneInto(clazz))

    def fetch[V](ctx: JDSLContext, t: Table[_], c: Option[Condition])(db2view: X => V) = {
      val sel = c match {
        case Some(c) => select(ctx, t).where(c)
        case None    => select(ctx, t)
      }
      sel
        .fetchInto(clazz)
        .asScala
        .map(db2view)
        .toList
    }

    def byIdLoader[T <: Table[_], V](t: T, id: UUID)(idf: T => Field[UUID])(db2view: X => V): Load[V] = {
      val cond = idf(t).eq(id)
      for {
        ctx <- dslCtx
        res <- effectBlocking {
          fetchOne(ctx, t, cond)(db2view)
        }
      } yield {
        res
      }
    }

    def byIdsLoader[T <: Table[_], V](t: T, ids: Chunk[UUID])(idf: T => Field[UUID])(db2view: X => V): Load[List[V]] = {
      val cond = idf(t).in(ids: _*)
      for {
        ctx <- dslCtx
        res <- effectBlocking {
          fetch(ctx, t, Some(cond))(db2view)
        }
      } yield {
        res
      }
    }

    def byArgsLoader[V](t: Table[_], args: Args)(db2view: X => V): Load[List[V]] = {
      for {
        ctx <- dslCtx
        list <- effectBlocking {
          fetch(ctx, t, args.condition)(db2view)
        }
      } yield {
        list
      }
    }

    def dataSource[T <: Table[_], V <: View, R <: GetById[V]](t: T)(idf: T => Field[UUID])(db2view: X => V): ZDataSource[LoadEnv, R] =
      ZDataSource.fromFunctionBatchedM[LoadEnv, Throwable, R, V](clazz.getSimpleName) {
        case requests if requests.isEmpty   => ZIO.succeed(Chunk.empty)
        case requests if requests.size == 1 => byIdLoader(t, requests(0).id)(idf)(db2view).map(Chunk(_))
        case requests =>
          byIdsLoader(t, requests.map(_.id))(idf)(db2view).flatMap { entities =>
            val entitiesById = entities.map(m => m.id -> m).toMap
            def go(rs: Chunk[R], accu: List[V]): ZIO[Any, Throwable, Chunk[V]] = {
              if (rs.isEmpty) {
                ZIO.succeed(Chunk.fromIterable(accu.reverse))
              } else {
                entitiesById.get(rs.head.id) match {
                  case Some(s) => go(rs.tail, s :: accu)
                  case None    => ZIO.fail(new NoSuchElementException(s"unknown id: ${rs.head.id}"))
                }
              }
            }
            go(requests, Nil)
          }
      }

  }


}
