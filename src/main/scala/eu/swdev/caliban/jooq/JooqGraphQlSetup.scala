package eu.swdev.caliban.jooq

import caliban.{GraphQL, GraphQLResponse, ResponseValue}
import eu.swdev.caliban.GraphQlEngine
import izumi.reflect.Tag
import zio.{Chunk, Has, ZIO, ZManaged}
import org.jooq.{Condition, DSLContext, Field, Record, Table}
import javax.sql.{DataSource => JDataSource}
import org.jooq.impl.DSL
import zio.blocking.{Blocking, effectBlocking}
import zio.query.{Request, ZQuery, DataSource => ZDataSource}

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * Helps to setup GraphQL engines that fetch data from relational databases using JOOQ.
  *
  * This class
  *
  *   1. captures the request context type and provides type aliases for a couple of types that are derived therefrom.
  *   1. provides request types for querying entities by their id and querying or counting entities by conditions.
  *   1. provides the [[Loader]] class that allow to create queries for the various request types.
  *   1. allows to create a [[GraphQlEngine]] for a given GraphQL API description and JDBC data source.
  *
  * @tparam ReqCtx
  */
class JooqGraphQlSetup[ReqCtx: Tag] {

  type DataSourceEnv = Has[JDataSource]
  type ReqEnv        = Has[ReqCtx]

  /**
    * The environment type for GraphQL operations and queries.
    *
    * Note that the [[ReqCtx]] is part of the environment, i.e. it can be accessed in operations and queries.
    */
  type GEnv = Blocking with DslEnv with ReqEnv

  /**
    * An operation that requires an environment of type [[GEnv]] may fail with a throwable or
    * succeed with a value of type [[A]].
    */
  type GOp[A] = ZIO[GEnv, Throwable, A]

  /**
    * A query that requires an environment of type [[GEnv]] may fail with a throwable or
    * succeed with a value of type [[A]].
    */
  type GQuery[A] = ZQuery[GEnv, Throwable, A]

  /** An operation that returns a DSLContext using a DataSource from the environment. */
  private val acquireDslContext: ZIO[Blocking with DataSourceEnv, Throwable, DSLContext] = for {
    dataSource <- ZIO.access[DataSourceEnv](_.get)
    connection <- effectBlocking(dataSource.getConnection())
  } yield {
    DSL.using(connection)
  }

  /** An operation that releases the given DSLContext. */
  private def releaseDslContext(dslCtx: DSLContext): ZIO[Blocking, Nothing, Unit] = effectBlocking(dslCtx.close()).orDie

  /** Constructs a [[ZManaged]] instance from an acquire and release operation. */
  private val managedDslContext: ZManaged[Blocking with DataSourceEnv, Throwable, DSLContext] =
    ZManaged.make(acquireDslContext)(releaseDslContext)

  /**
    * A request for a list of entity views.
    *
    * When this request is evaluated then a paginated list of entity views is returned.
    * Selected entities satisfy the condition that is derived from this request
    *
    * @tparam T the table where entities are selected from
    * @tparam V the view of these entities
    */
  trait ListReq[T <: Table[_], V] extends PaginatedReq with Request[Throwable, List[V]] {

    /**
      * An operation that converts this [[ListReq]] into a condition.
      *
      * The operation may consider the [[ReqCtx]] for example for adding authorization.
      */
    def condition(t: T): GOp[Option[Condition]]
  }

  /** A request that given an id of type [[ID]] returns a value of type [[V]]. */
  case class GetByIdReq[ID, V](id: ID) extends Request[Throwable, V]

  /** A request that counts the number of records that satisfy the condition of the given [[ListReq]]. */
  case class CountReq[T <: Table[_]](listReq: ListReq[T, _]) extends Request[Throwable, Int]

  /**
    * Provides queries for loading views of records.
    *
    * Queries are data structures that describe how data is accessed but do not access data right now.
    * These descriptions are "evaluated" by the GraphQL engine when a request is processed.
    *
    * The returned views contain some fields that are directly copied from corresponding persistent fields and
    * contain other fields that are queries for joined information.
    *
    * The class uses multiple parameter lists in order to guide type inference.
    * (cf. https://docs.scala-lang.org/tour/multiple-parameter-lists.html)
    *
    * @tparam D the database type of records (records are fetched into this type)
    * @tparam V the view type of records (records are published by this type)
    * @tparam Id the type of the id column
    */
  case class Loader[D, R <: Record, T <: Table[R], Id, V](
      clazz: Class[D],
      table: T with Table[R]
  )(
      primaryKeyF: T => Field[Id],
      defaultSortF: T => Field[_] = null
  )(
      db2view: D => V
  ) {

    private val primaryKeyField = primaryKeyF(table)

    private val defaultSortField = Option(defaultSortF).map(f => f(table))

    private def rec2view(rec: R): V = db2view(rec.into(clazz))

    /** Determines the number of records that satisfy the given condition. */
    private def count(ctx: DSLContext, condition: Option[Condition]): Int = condition match {
      case Some(c) => ctx.fetchCount(table, c)
      case None    => ctx.fetchCount(table)
    }

    /** Lists the records that satisfy the given condition and pagination. */
    private def list(ctx: DSLContext, condition: Option[Condition], args: PaginatedReq): Iterable[R] = {
      val sel = ctx
        .selectFrom(table)
        .letOpt(condition)((s, o) => s.where(o))
        .letOpt(args.sortOrder.map(table.field(_)).orElse(defaultSortField)) { (s, o) =>
          val orderField = o
            .letOpt(args.sortOrder)((s, o) =>
              o match {
                case "ASC"  => s.asc()
                case "DESC" => s.desc()
                case _      => throw new IllegalArgumentException("Invalid sort order '${p.sortOrder}'")
            })
          s.orderBy(orderField)
        }
        .let { s =>
          (args.offset, args.limit) match {
            case (Some(o), Some(l)) => s.limit(o, l)
            case (Some(o), None)    => s.offset(o)
            case (None, Some(l))    => s.limit(l)
            case (None, None)       => s
          }
        }

      sel.fetch.asScala
    }

    /** An operation that fetches a number of records by their id. */
    private def byIdsLoader(ids: Chunk[Id]): GOp[Iterable[R]] = {
      val cond = primaryKeyField.in(ids: _*)
      for {
        ctx <- dslCtx
        res <- effectBlocking {
                ctx.selectFrom(table).where(cond).fetch().asScala
              }
      } yield {
        res
      }
    }

    //
    // Caliban uses so called data sources for optimized data access.
    // -> Each data access has as its input a request object that describes what data should be retrieved.
    // -> Request objects are used to cache / batch data access operations
    //

    /** processes batches of [[GetByIdReq]]s */
    private val byIdDataSource: ZDataSource[GEnv, GetByIdReq[Id, V]] =
      ZDataSource.fromFunctionBatchedM[GEnv, Throwable, GetByIdReq[Id, V], V](clazz.getName) {
        case requests if requests.isEmpty => ZIO.succeed(Chunk.empty)
        case requests =>
          byIdsLoader(requests.map(_.id)).flatMap { entities =>
            // the order of the returned views must match the order of the batched requests
            // -> build a map of the returned views and map requests into these views
            val entitiesById = entities.map(rec => rec.get(primaryKeyField) -> rec2view(rec)).toMap
            requests.mapM { req =>
              entitiesById.get(req.id) match {
                case Some(s) => ZIO.succeed(s)
                case None    => ZIO.fail(new NoSuchElementException(s"unknown id: ${req.id}"))
              }
            }
          }
      }

    private val countDataSource: ZDataSource[GEnv, CountReq[T]] =
      ZDataSource.fromFunctionM[GEnv, Throwable, CountReq[T], Int](s"${clazz.getName}.count") { countReq =>
        for {
          // an operation that accesses the DSLContext stored in the environment
          dslCtx    <- dslCtx
          condition <- countReq.listReq.condition(table)
          count     <- effectBlocking { count(dslCtx, condition) }
        } yield count
      }

    private val listDataSource: ZDataSource[GEnv, ListReq[T, V]] =
      ZDataSource.fromFunctionM[GEnv, Throwable, ListReq[T, V], List[V]](s"${clazz.getName}.list") { req =>
        for {
          // an operation that accesses the DSLContext stored in the environment
          dslCtx    <- dslCtx
          condition <- req.condition(table)
          list      <- effectBlocking { list(dslCtx, condition, req).map(rec2view).toList }
        } yield list
      }

    //
    // queries are constructed from request objects using the corresponding data sources
    //

    def list(args: ListReq[T, V]): GQuery[List[V]] = ZQuery.fromRequest(args)(listDataSource)

    def page(args: ListReq[T, V]): GQuery[PagedResult[V]] = {
      val countQuery = ZQuery.fromRequest(CountReq(args))(countDataSource)
      // query both, the count and the list; combine the results into a PagedResult
      countQuery.zipWith(list(args)) { (total, list) =>
        PagedResult(total, args.offset.getOrElse(0), list)
      }
    }

    def byId(id: Id): GQuery[V] = ZQuery.fromRequest(GetByIdReq[Id, V](id))(byIdDataSource)

    def byOptId(id: Id): GQuery[Option[V]] = Option(id) match {
      case Some(id) => byId(id).map(Some(_))
      case _        => ZQuery.succeed(None)
    }

  }

  /**
    * Allows to evaluate GraphQL queries and render the API spec.
    */
  case class JooqGraphQLEngine(graphQl: GraphQL[GEnv], dataSource: JDataSource) extends GraphQlEngine[ReqCtx] {

    // evaluate the operation that returns the GraphQl interpreter
    private val interpreter = zio.Runtime.default.unsafeRun(graphQl.interpreter)

    /** Returns the GraphQL API description of this engine. */
    def render: String = graphQl.render

    def evaluateQuery(qry: String, reqCtx: ReqCtx): Either[Throwable, GraphQLResponse[Throwable]] = {

      // first construct the operation that evaluates the query...
      val prg = managedDslContext
        .use { dslCtx =>
          interpreter.execute(qry).provideSome[Blocking with ReqEnv](_.add(dslCtx))
        }
        .provideSome[Blocking with ReqEnv](_.add(dataSource))
        .provideSome[Blocking](_.add(reqCtx))
        .provideLayer(Blocking.live)

      // ...then run the operation
      // -> shuffle an error throwable into the left of an either and a valid response into the right side
      zio.Runtime.default.unsafeRun(prg.either)

    }

  }

  def createGraphQlEngine(graphQl: GraphQL[GEnv], dataSource: JDataSource): GraphQlEngine[ReqCtx] =
    JooqGraphQLEngine(graphQl, dataSource)

}
