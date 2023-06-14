package ru.sberbank.bigdata.enki.metadata

final class primaryKey extends MetadataAnnotation {
  override def key: String   = MetadataKey.PrimaryKey
  override def value: String = "Primary Key"
}
