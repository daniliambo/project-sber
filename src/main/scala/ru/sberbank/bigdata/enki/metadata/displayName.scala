package ru.sberbank.bigdata.enki.metadata

final class displayName(val value: String) extends MetadataAnnotation {
  override def key: String = MetadataKey.DisplayName
}
