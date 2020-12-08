package eu.swdev.caliban.jooq

import java.util.UUID

import caliban.GraphQL.graphQL
import caliban.RootResolver
import caliban.schema.GenericSchema
import ct.sql.tables._
import ct.sql.Tables._
import eu.swdev.caliban.GraphQlEngine
import org.jooq.Table
import javax.sql.{DataSource => JDataSource}
import zio.ZIO
import zio.blocking.{Blocking, effectBlocking}

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * Setup of a GraphQL engine.
 *
 * The setup contains the following pieces
 *
 *   1. view classes that give access to corresponding model classes
 *   1. request classes that define requests for these views
 *   1. the top-level query entrypoint
 *   1. the [[GraphQlSetup.createEngine]] method
 */
object GraphQlSetup {

  // use the JooqGraphQlSetup class to build views of the data model and a GraphQL engine
  val jooqSetup = new JooqGraphQlSetup[RequestContext]

  type Query[X]                  = jooqSetup.GQuery[X]
  type Load[X]                   = jooqSetup.GOp[X]
  type ListReq[T <: Table[_], V] = jooqSetup.ListReq[T, V]

  object loader {

    // define some loader objects that provide queries for entities by their id or by conditions

    val movie    = jooqSetup.Loader(classOf[MovieEntity], MOVIE)(_.ID)(new MovieView(_))
    val director = jooqSetup.Loader(classOf[DirectorEntity], DIRECTOR)(_.ID)(new DirectorView(_))
    val genre    = jooqSetup.Loader(classOf[GenreEntity], GENRE)(_.ID)(new GenreView(_))
    val actor    = jooqSetup.Loader(classOf[ActorEntity], ACTOR)(_.ID)(new ActorView(_))

    // define load operations for cases that are not covered by the loaders defined above
    // -> note that load operations are not cached
    // -> a [[zio.query.Datasource]] could be defined that supports querying over join tables

    def moviesByActorLoader(actorId: UUID): Load[List[MovieView]] =
      for {
        ctx <- dslCtx
        res <- effectBlocking {
                ctx
                  .select(MOVIE.asterisk())
                  .from(MOVIE)
                  .innerJoin(MOVIE_ACTOR)
                  .on(MOVIE_ACTOR.ACTOR_ID.eq(actorId))
                  .fetchInto(classOf[MovieEntity])
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
                  .fetchInto(classOf[ActorEntity])
                  .asScala
                  .map(new ActorView(_))
                  .toList
              }
      } yield res

  }

  case class RequestContext(user: String)

  trait NoPagination extends PaginatedReq {
    override def orderByColumn: Option[String] = None
    override def limit: Option[Int]            = None
    override def offset: Option[Int]           = None
    override def sortOrder: Option[String]     = None
  }

  case class MovieView(id: UUID,
                       name: String,
                       director: Query[DirectorView],
                       year: Int,
                       genre: Query[GenreView],
                       actors: Load[List[ActorView]]) {
    def this(d: MovieEntity) =
      this(d.id, d.name, loader.director.byId(d.directorId), d.year, loader.genre.byId(d.genreId), loader.actorsByMovieLoader(d.id))
  }

  case class DirectorView(id: UUID, name: String, movies: Query[List[MovieView]]) {
    def this(d: DirectorEntity) = this(d.id, d.name, loader.movie.list(MovieReq(director = Some(d.id))))
  }

  case class GenreView(id: UUID, name: String, movies: Query[List[MovieView]]) {
    def this(d: GenreEntity) = this(d.id, d.name, loader.movie.list(MovieReq(genre = Some(d.id))))
  }

  case class ActorView(id: UUID, name: String, movies: Load[List[MovieView]]) {
    def this(d: ActorEntity) = this(d.id, d.name, loader.moviesByActorLoader(d.id))
  }

  case class MovieReq(
      name: Option[String] = None,
      director: Option[UUID] = None,
      year: Option[Int] = None,
      fromYear: Option[Int] = None,
      toYear: Option[Int] = None,
      genre: Option[UUID] = None
  ) extends ListReq[Movie, MovieView]
      with NoPagination {

    override def condition(t: Movie) =
      ZIO.succeed(
        and(
          name.map(x => t.NAME.like(s"%$x%")),
          director.map(x => t.DIRECTOR_ID.eq(x)),
          year.map(x => t.YEAR.eq(x)),
          fromYear.map(x => t.YEAR.ge(x)),
          toYear.map(x => t.YEAR.le(x)),
          genre.map(x => t.GENRE_ID.eq(x))
        ))

  }

  case class DirectorReq(name: Option[String]) extends ListReq[Director, DirectorView] with NoPagination {
    override def condition(t: Director) = ZIO.succeed(name.map(x => DIRECTOR.NAME.like(s"%$x%")))
  }

  case class GenreReq(name: Option[String]) extends ListReq[Genre, GenreView] with NoPagination {
    override def condition(t: Genre) = ZIO.succeed(name.map(x => t.NAME.like(s"%$x%")))
  }

  case class ActorReq(name: Option[String]) extends ListReq[Actor, ActorView] with NoPagination {
    override def condition(t: Actor) = ZIO.succeed(name.map(x => t.NAME.like(s"%$x%")))
  }

  case class Queries(
      movies: MovieReq => Query[List[MovieView]],
      movie: UUID => Query[MovieView],
      directors: DirectorReq => Query[List[DirectorView]],
      director: UUID => Query[DirectorView],
      genres: GenreReq => Query[List[GenreView]],
      genre: UUID => Query[GenreView],
      actors: ActorReq => Query[List[ActorView]],
      actor: UUID => Query[ActorView],
  )

  val queries = Queries(
    loader.movie.list,
    loader.movie.byId,
    loader.director.list,
    loader.director.byId,
    loader.genre.list,
    loader.genre.byId,
    loader.actor.list,
    loader.actor.byId,
  )

  object schema extends GenericSchema[jooqSetup.GEnv]
  import schema._

  // the movie schema must be generated explicitly; cf.:
  // https://discord.com/channels/629491597070827530/633200096393166868/767454716170076171
  // https://github.com/ghostdogpr/caliban/releases/tag/v0.9.4
  implicit val movieSchema = gen[MovieView]

  // create the GraphQL API for the specified queries
  val graphQl = graphQL(RootResolver(queries))

  def createEngine(dataSource: JDataSource): GraphQlEngine[RequestContext] = jooqSetup.createGraphQlEngine(graphQl, dataSource)
}
