package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql._

/** Generic store with ability to read and write dataset.
  */
trait Store[T] {
  type Location

  def read(location: Location)(implicit session: SparkSession): Dataset[T]

  /** Saves the content of the `Dataset` as table specified in the provided Location.
    *
    * In the case the table already exists, behavior of this function depends on the
    * save mode, specified by the `mode`.
    *
    * @param location Location object with table name
    * @param mode whether contents should be overwritten or appended to existing data.
    * @param data dataset to be written
    */
  def write(location: Location, mode: WriteMode, data: Dataset[T])(implicit session: SparkSession): Unit

  /** Saves the content of the `Dataset` as files in the specified path.
    *
    * In the case the table already exists, behavior of this function depends on the
    * save mode, specified by the `mode`.
    *
    * @param path path where files should be stored
    * @param mode whether contents should be overwritten or appended to existing data.
    * @param data dataset to be written
    */
  def write(path: String, mode: WriteMode, data: Dataset[T])(implicit session: SparkSession): Unit

}
