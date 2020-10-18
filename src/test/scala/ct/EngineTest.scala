package ct

import java.util.UUID

import ct.Engine.{Actor, Director, Genre, Movie}
import ct.sql.Tables
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import liquibase.{Contexts, LabelExpression, Liquibase}
import org.h2.jdbcx.JdbcDataSource
import org.jooq.impl.DSL
import org.jooq.{DSLContext, Record, Table}
import org.scalatest.funsuite.AnyFunSuite

class EngineTest extends AnyFunSuite {

  val h2DataSource = {
    val ds = new JdbcDataSource
    ds.setUrl("jdbc:h2:mem:db;DB_CLOSE_DELAY=-1")
    ds
  }

  private def insert(ctx: DSLContext, t: Table[_ <: Record], data: AnyRef): Unit = {
    ctx.insertInto(t).set(ctx.newRecord(t, data)).execute()
  }

  private def insert(ctx: DSLContext, t: Table[_ <: Record], data: Seq[AnyRef]): Unit = {
    data.foreach(insert(ctx, t, _))
  }

  test("init db") {

    //
    // setup database
    //

    val conn = h2DataSource.getConnection

    val jdbcConn = new JdbcConnection(conn)

    val liquibase = new Liquibase("database.xml", new ClassLoaderResourceAccessor(), jdbcConn)

    liquibase.update(new Contexts(), new LabelExpression())

    //
    // generate test data
    //

    val ctx = DSL.using(conn)

    val directors = for (i <- 0 until 10) yield Director(UUID.randomUUID(), s"director $i")

    insert(ctx, Tables.DIRECTOR, directors)

    val genres = for (i <- 0 until 4) yield Genre(UUID.randomUUID(), s"genre $i")

    insert(ctx, Tables.GENRE, genres)

    val movies = for (i <- 0 until 40)
      yield Movie(UUID.randomUUID(), s"movie $i", directors(i % directors.size).id, 1980 + i % 7, genres(i % genres.size).id)

    insert(ctx, Tables.MOVIE, movies)

    val actors = for (i <- 0 until 20) yield Actor(UUID.randomUUID(), s"actor $i")

    insert(ctx, Tables.ACTOR, actors)

    case class MovieActor(movieId: UUID, actorId: UUID)

    val movieActors = for {
      m <- 0 until movies.size
      a <- m until m + 3
    } yield MovieActor(movies(m).id, actors(a % actors.size).id)

    insert(ctx, Tables.MOVIE_ACTOR, movieActors)

    conn.commit()

    conn.close()

    //
    // execute query
    //

    val runtime = zio.Runtime.default

    // fails --  ValidationError Error: Field 'name' does not exist on type 'ListMovieView'.
    val qry =
      """
        | query {
        |  movies {
        |    name
        |  }
        |}
        |""".stripMargin

    // succeeds
//    val qry =
//      """
//        |query {
//        |   directors {
//        |     name
//        |     movies {
//        |       name
//        |     }
//        |   }
//        |}
//        |""".stripMargin


    val result = Engine.runQuery(h2DataSource, qry)

    println(result.toString)

  }

}
