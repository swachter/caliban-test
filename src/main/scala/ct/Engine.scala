package ct

import java.util.UUID

import caliban.CalibanError.ExecutionError
import caliban.GraphQL.graphQL
import caliban.Value.StringValue
import caliban.schema.{ArgBuilder, Schema, _}
import caliban.{ResponseValue, RootResolver}
import ct.sql.Tables._
import javax.sql.{DataSource => JDataSource}
import org.jooq.impl.DSL
import org.jooq.{Condition, DSLContext, Field, Table}
import zio.blocking._
import zio._

import scala.jdk.CollectionConverters._
import scala.util.Try

object Engine {

  case class Movie(id: UUID, name: String, directorId: UUID, year: Int, genreId: UUID)
  case class Director(id: UUID, name: String)
  case class Genre(id: UUID, name: String)
  case class Actor(id: UUID, name: String)

  type DslCtx  = Has[DSLContext]
  type LoadEnv = Blocking with DslCtx
  type Load[X] = ZIO[LoadEnv, Throwable, X]

  case class MovieView(id: UUID,
                       name: String,
                       director: Load[DirectorView],
                       year: Int,
                       genre: Load[GenreView],
                       actors: Load[List[ActorView]]) {
    def this(d: Movie) = this(d.id, d.name, directorLoader(d.directorId), d.year, genreLoader(d.genreId), actorsByMovieLoader(d.id))
  }

  case class DirectorView(id: UUID, name: String, movies: Load[List[MovieView]]) {
    def this(d: Director) = this(d.id, d.name, moviesLoader(MovieArgs(director = Some(d.id))))
  }

  case class GenreView(id: UUID, name: String, movies: Load[List[MovieView]]) {
    def this(d: Genre) = this(d.id, d.name, moviesLoader(MovieArgs(genre = Some(d.id))))
  }

  case class ActorView(id: UUID, name: String, movies: Load[List[MovieView]]) {
    def this(d: Actor) = this(d.id, d.name, moviesByActorLoader(d.id))
  }

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

  case class Queries(
      movies: MovieArgs => Load[List[MovieView]],
      movie: UUID => Load[MovieView],
      directors: DirectorArgs => Load[List[DirectorView]],
      director: UUID => Load[DirectorView],
      genres: GenreArgs => Load[List[GenreView]],
      genre: UUID => Load[GenreView],
      actors: ActorArgs => Load[List[ActorView]],
      actor: UUID => Load[ActorView],
  )

  //
  //
  //

  val dslCtx = ZIO.access[DslCtx](_.get)

  implicit class Fetcher[X](val clazz: Class[X]) extends AnyVal {

    private def select(ctx: DSLContext, t: Table[_]) = ctx.select(t.asterisk()).from(t)

    def fetchOne[V](ctx: DSLContext, t: Table[_], c: Condition)(db2view: X => V) =
      db2view(select(ctx, t).where(c).fetchOneInto(clazz))

    def fetch[V](ctx: DSLContext, t: Table[_], c: Option[Condition])(db2view: X => V) = {
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

  }

  //
  //
  //

  def moviesLoader(args: MovieArgs): Load[List[MovieView]] = classOf[Movie].byArgsLoader(MOVIE, args)(new MovieView(_))
  def movieLoader(id: UUID): Load[MovieView]               = classOf[Movie].byIdLoader(MOVIE, id)(_.ID)(new MovieView(_))

  def directorsLoader(args: DirectorArgs): Load[List[DirectorView]] = classOf[Director].byArgsLoader(DIRECTOR, args)(new DirectorView(_))
  def directorLoader(id: UUID): Load[DirectorView]                  = classOf[Director].byIdLoader(DIRECTOR, id)(_.ID)(new DirectorView(_))

  def genresLoader(args: GenreArgs): Load[List[GenreView]] = classOf[Genre].byArgsLoader(GENRE, args)(new GenreView(_))
  def genreLoader(id: UUID): Load[GenreView]               = classOf[Genre].byIdLoader(GENRE, id)(_.ID)(new GenreView(_))

  def actorsLoader(args: ActorArgs): Load[List[ActorView]] = classOf[Actor].byArgsLoader(ACTOR, args)(new ActorView(_))
  def actorLoader(id: UUID): Load[ActorView]               = classOf[Actor].byIdLoader(ACTOR, id)(_.ID)(new ActorView(_))

  def moviesByActorLoader(actorIdd: UUID): Load[List[MovieView]] =
    for {
      ctx <- dslCtx
      res <- effectBlocking {
              ctx
                .select(MOVIE.asterisk())
                .from(MOVIE)
                .innerJoin(MOVIE_ACTOR)
                .on(MOVIE_ACTOR.ACTOR_ID.eq(actorIdd))
                .fetchInto(classOf[Movie])
                .asScala
                .map(new MovieView(_))
                .toList
            }
    } yield res

  def actorsByMovieLoader(movieId: UUID): Load[List[ActorView]] =
    for {
      ctx <- dslCtx
      res <- effectBlocking {
              ctx
                .select(ACTOR.asterisk())
                .from(ACTOR)
                .innerJoin(MOVIE_ACTOR)
                .on(MOVIE_ACTOR.MOVIE_ID.eq(movieId))
                .fetchInto(classOf[Actor])
                .asScala
                .map(new ActorView(_))
                .toList
            }
    } yield res

  val queries = Queries(
    moviesLoader,
    movieLoader,
    directorsLoader,
    directorLoader,
    genresLoader,
    genreLoader,
    actorsLoader,
    actorLoader,
  )

  //
  //
  //

  implicit val uuidSchema: Schema[Any, UUID] = Schema.stringSchema.contramap(_.toString)

  implicit val uuidArgBuilder: ArgBuilder[UUID] = {
    case StringValue(value) =>
      Try(UUID.fromString(value))
        .fold(ex => Left(ExecutionError(s"Can't parse $value into a UUID", innerThrowable = Some(ex))), Right(_))
    case other => Left(ExecutionError(s"Can't build a UUID from input $other"))
  }

  object schema extends GenericSchema[LoadEnv]
  import schema._

  // the query
  //
  // query {
  //   movies {
  //     name
  //   }
  // }
  //
  // fails with "ValidationError Error: Field 'name' does not exist on type 'ListMovieView'."
  //
  // Adding the following line solved the issue
  // (cf. https://discord.com/channels/629491597070827530/633200096393166868/767454716170076171)
  implicit val movieSchema = gen[MovieView]
  implicit val actorSchema = gen[ActorView]

  //
  //
  //

  val api         = graphQL(RootResolver(queries))
  val interpreter = api.interpreter

  def query(qry: String): ZIO[LoadEnv, Throwable, ResponseValue] = {
    for {
      intp <- interpreter
      resp <- intp.execute(qry)
      r <- if (resp.errors.isEmpty) {
            ZIO.succeed(resp.data)
          } else {
            ZIO.fail {
              val t = new Throwable()
              resp.errors.foreach(t.addSuppressed(_))
              t
            }
          }
    } yield {
      r
    }
  }

  type DataSource = Has[JDataSource]

  val acquireDslContext: ZIO[Blocking with DataSource, Throwable, DSLContext] = for {
    dataSource <- ZIO.access[DataSource](_.get)
    connection <- effectBlocking(dataSource.getConnection())
  } yield {
    DSL.using(connection)
  }

  def releaseDslContext(dslCtx: DSLContext): ZIO[Blocking, Nothing, Unit] = effectBlocking(dslCtx.close()).orDie

  val managed: ZManaged[Blocking with DataSource, Throwable, DSLContext] = ZManaged.make(acquireDslContext)(releaseDslContext)

  def runQuery(dataSource: JDataSource, qry: String) = {
    val prg = managed
      .use { dslCtx =>
        query(qry).provideSome[Blocking](_.add(dslCtx))
      }
      .provideSome[Blocking](_.add(dataSource))
      .provideLayer(Blocking.live)

    zio.Runtime.default.unsafeRun(prg)
  }

}
