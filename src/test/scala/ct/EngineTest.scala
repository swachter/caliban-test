package ct

import java.util.UUID

import ct.Engine.{Director, Genre, Movie}
import ct.sql.Tables
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import liquibase.{Contexts, LabelExpression, Liquibase}
import org.h2.jdbcx.JdbcDataSource
import org.jooq.impl.DSL
import org.jooq.{DSLContext, Record, Table}
import org.scalatest.funsuite.AnyFunSuite

class EngineTest extends AnyFunSuite {

  val h2DataSourcee = {
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
    val conn = h2DataSourcee.getConnection

    val jdbcConn = new JdbcConnection(conn)

    val liquibase = new Liquibase("database.xml", new ClassLoaderResourceAccessor(), jdbcConn)

    liquibase.update(new Contexts(), new LabelExpression())

    val ctx = DSL.using(conn)

    val directors = for (i <- 0 until 10) yield Director(UUID.randomUUID(), s"director $i")

    insert(ctx, Tables.DIRECTOR, directors)

    val genres = for (i <- 0 until 4) yield Genre(UUID.randomUUID(), s"genre $i")

    insert(ctx, Tables.GENRE, genres)

    val movies = for (i <- 0 until 40) yield Movie(UUID.randomUUID(), s"movie $i", directors(i % directors.size).id, 1980 + i % 7, genres(i % genres.size).id)

    insert(ctx, Tables.MOVIE, movies)

    conn.commit()

    conn.close()

    val runtime = zio.Runtime.default

    val qry =
      """
        |{
        |  directors {
        |    id
        |    name
        |    movies
        |  }
        |}
        |""".stripMargin

//    val qry =
//      """
//        |{
//        |  movies {
//        |    name
//        |    genre {
//        |      name
//        |    }
//        |  }
//        |}
//        |""".stripMargin

    val z = Engine.program(h2DataSourcee, qry)

    val result = runtime.unsafeRun(z)

    println(result.toString)

  }


}
