package ru.sberbank.bigdata.enki.transformation

object TransformationStatus {

  /** Cast successful.
    */
  final private[transformation] val Success = 0

  /** Conversion error.
    */
  final private[transformation] val Error = 1

  /** Value skipped (column not present in the destination)
    */
  final private[transformation] val Skipped = 2

  /** Default value used to populate column (column not present in the source)
    */
  final private[transformation] val Default = 3
}
