package eu.swdev.caliban.jooq

import java.util.UUID

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

  val graphQlEngine = GraphQlSetup.createEngine(h2DataSource)

  test("init db") {

    println("API:")
    println(graphQlEngine.render)

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

    val directors = for (i <- 0 until 10) yield DirectorEntity(UUID.randomUUID(), s"director $i")

    insert(ctx, Tables.DIRECTOR, directors)

    val genres = for (i <- 0 until 4) yield GenreEntity(UUID.randomUUID(), s"genre $i")

    insert(ctx, Tables.GENRE, genres)

    val movies = for (i <- 0 until 40)
      yield MovieEntity(UUID.randomUUID(), s"movie $i", directors(i % directors.size).id, 1980 + i % 7, genres(i % genres.size).id)

    insert(ctx, Tables.MOVIE, movies)

    val actors = for (i <- 0 until 20) yield ActorEntity(UUID.randomUUID(), s"actor $i")

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
    // execute queries
    //

    runQuery(
      """
        | query {
        |  movies {
        |    name
        |  }
        |}
        |""".stripMargin
    )
    runQuery(
      """
        | query {
        |  movies {
        |    name
        |    year
        |    actors {
        |      name
        |    }
        |    genre {
        |      name
        |    }
        |  }
        |}
        |""".stripMargin
    )

    runQuery(
      """
        |query {
        |   directors {
        |     name
        |     movies {
        |       name
        |       year
        |     }
        |   }
        |}
        |""".stripMargin
    )

    runQuery(
      """
        |query {
        |  actors {
        |    name
        |    movies {
        |      name
        |      genre {
        |        name
        |      }
        |    }
        |  }
        |}
        |""".stripMargin
    )

    // this query can only be run by the optimizing engine
    runQuery(
      """
        |query {
        |  actors {
        |    name
        |    movies {
        |      name
        |      genre {
        |        name
        |      }
        |      director {
        |        name
        |      }
        |    }
        |  }
        |}
        |""".stripMargin
    )

  }

  def runQuery(qry: String) = {
    val result = graphQlEngine.evaluateQuery(qry, GraphQlSetup.RequestContext("abc"))
    println(result)
  }

}
