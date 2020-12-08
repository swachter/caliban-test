package eu.swdev.caliban

import caliban.GraphQLResponse

/**
 * Provides a GraphQL API description and allows to evaluate queries against that API.
 *
 * @tparam ReqCtx GraphQL queries have a request context that can be used for example to capture authorization
 *                information.
 */
trait GraphQlEngine[ReqCtx] {

  /** Returns the GraphQL API description of this engine. */
  def render: String

  /** Evaluates the given GraphQL query passing with the given request context. */
  def evaluateQuery(qry: String, reqCtx: ReqCtx): Either[Throwable, GraphQLResponse[Throwable]]

}
