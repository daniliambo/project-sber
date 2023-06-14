package ru.sberbank.bigdata.enki.metadata

final class foreignKey extends MetadataAnnotation {
  override def key: String   = MetadataKey.ForeignKey
  override def value: String = "Foreign Key"
}
