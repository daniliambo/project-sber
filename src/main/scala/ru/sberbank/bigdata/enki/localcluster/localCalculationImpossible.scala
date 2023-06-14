package ru.sberbank.bigdata.enki.localcluster

import scala.annotation.StaticAnnotation

/** Annotate gen method in views, which fails to be executed on local cluster
  * Instead result of [[ru.sberbank.bigdata.enki.framework.Table.getEmpty]] will be saved.
  *
  * @note make sure that View[Row] has [[ru.sberbank.bigdata.enki.framework.Table.structMap]],
  *       otherwise getEmpty returns table with no columns.
  */
final class localCalculationImpossible extends StaticAnnotation
