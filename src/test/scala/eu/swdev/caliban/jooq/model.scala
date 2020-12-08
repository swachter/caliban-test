package eu.swdev.caliban.jooq

import java.util.UUID

case class MovieEntity(id: UUID, name: String, directorId: UUID, year: Int, genreId: UUID)
case class DirectorEntity(id: UUID, name: String)
case class GenreEntity(id: UUID, name: String)
case class ActorEntity(id: UUID, name: String)
