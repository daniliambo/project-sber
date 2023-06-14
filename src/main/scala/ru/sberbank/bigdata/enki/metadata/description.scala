package ru.sberbank.bigdata.enki.metadata

final class description(val value: String) extends MetadataAnnotation {
  override val key: String = MetadataKey.Description
}
