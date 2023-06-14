package ru.sberbank.bigdata.enki.metadata

object MetadataKey {
  lazy val Description = s"$prefix.description"
  lazy val DisplayName = s"$prefix.displayName"
  lazy val PrimaryKey  = s"$prefix.primaryKey"
  lazy val ForeignKey  = s"$prefix.foreignKey"

  private val prefix = "enki"
}
