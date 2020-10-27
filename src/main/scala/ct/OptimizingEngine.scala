package ct

import java.util.UUID

import caliban.CalibanError.ExecutionError
import caliban.GraphQL.graphQL
import caliban.RootResolver
import caliban.Value.StringValue
import caliban.schema.{ArgBuilder, Schema, _}
import ct.sql.Tables._
import zio.blocking._
import zio.query.ZQuery

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Optimizing GraphQL engine that uses ZQuery to batch/cache requests for entities by their id
  */
object OptimizingEngine {

  //
  // define views of the database entities; relationships are represented as Load or Query effects
  //

  case class MovieView(id: UUID,
                       name: String,
                       director: Query[DirectorView],
                       year: Int,
                       genre: Query[GenreView],
                       actors: Load[List[ActorView]])
      extends View {
    def this(d: Movie) = this(d.id, d.name, directorLoader(d.directorId), d.year, genreLoader(d.genreId), actorsByMovieLoader(d.id))
  }

  case class DirectorView(id: UUID, name: String, movies: Load[List[MovieView]]) extends View {
    def this(d: Director) = this(d.id, d.name, moviesLoader(MovieArgs(director = Some(d.id))))
  }

  case class GenreView(id: UUID, name: String, movies: Load[List[MovieView]]) extends View {
    def this(d: Genre) = this(d.id, d.name, moviesLoader(MovieArgs(genre = Some(d.id))))
  }

  case class ActorView(id: UUID, name: String, movies: Load[List[MovieView]]) extends View {
    def this(d: Actor) = this(d.id, d.name, moviesByActorLoader(d.id))
  }

  //
  //
  //

  case class GetMovieById(id: UUID)    extends GetById[MovieView]
  case class GetDirectorById(id: UUID) extends GetById[DirectorView]
  case class GetGenreById(id: UUID)    extends GetById[GenreView]
  case class GetActorById(id: UUID)    extends GetById[ActorView]

  //
  //
  //

  val movieDataSource    = classOf[Movie].dataSource(MOVIE)(_.ID)(new MovieView(_))
  val directorDataSource = classOf[Director].dataSource(DIRECTOR)(_.ID)(new DirectorView(_))
  val genreDataSource    = classOf[Genre].dataSource(GENRE)(_.ID)(new GenreView(_))
  val actorDataSource    = classOf[Actor].dataSource(ACTOR)(_.ID)(new ActorView(_))

  def moviesLoader(args: MovieArgs): Load[List[MovieView]] = classOf[Movie].byArgsLoader(MOVIE, args)(new MovieView(_))
  def movieLoader(id: UUID): Query[MovieView]              = ZQuery.fromRequest(GetMovieById(id))(movieDataSource)

  def directorsLoader(args: DirectorArgs): Load[List[DirectorView]] = classOf[Director].byArgsLoader(DIRECTOR, args)(new DirectorView(_))
  def directorLoader(id: UUID): Query[DirectorView]                 = ZQuery.fromRequest(GetDirectorById(id))(directorDataSource)

  def genresLoader(args: GenreArgs): Load[List[GenreView]] = classOf[Genre].byArgsLoader(GENRE, args)(new GenreView(_))
  def genreLoader(id: UUID): Query[GenreView]              = ZQuery.fromRequest(GetGenreById(id))(genreDataSource)

  def actorsLoader(args: ActorArgs): Load[List[ActorView]] = classOf[Actor].byArgsLoader(ACTOR, args)(new ActorView(_))
  def actorLoader(id: UUID): Query[ActorView]              = ZQuery.fromRequest(GetActorById(id))(actorDataSource)

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

  case class Queries(
      movies: MovieArgs => Load[List[MovieView]],
      movie: UUID => Query[MovieView],
      directors: DirectorArgs => Load[List[DirectorView]],
      director: UUID => Query[DirectorView],
      genres: GenreArgs => Load[List[GenreView]],
      genre: UUID => Query[GenreView],
      actors: ActorArgs => Load[List[ActorView]],
      actor: UUID => Query[ActorView],
  )

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

  //
  //
  //

  // constructing an interpreter is an expensive operation that should only be done once and not for every query
  val api         = graphQL(RootResolver(queries))
  val interpreter = zio.Runtime.default.unsafeRun(api.interpreter)

}
