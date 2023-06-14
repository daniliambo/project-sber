package ru.sberbank.bigdata.enki.plan

import cats.syntax.option._
import ru.sberbank.bigdata.enki.plan.columns.ColumnReference

import scala.collection.immutable.LongMap

package object nodes {

  type ReferenceMap = LongMap[ColumnReference]

  /** Returns node columns wrapped in [[ColumnReference]] */
  def getColumnReferences(node: Node): Vector[ColumnReference] =
    node match {
      case SourceTableNode(_, tableName, columns) => // use table name as qualifier
        columns.map(ColumnReference.fromColumn(_, tableName.some))

      case aliasedNode: AliasedNode =>
        aliasedNode.columns

      case joinNode: JoinNode =>
        joinNode.columns

      case generatorNode: GeneratorNode =>
        generatorNode.columns

      case otherNode =>
        otherNode.columns.map(ColumnReference.fromColumn(_))
    }

  def getColumnReferencesMap(node: Node): ReferenceMap = {
    val pairs = getColumnReferences(node).map(column => column.id -> column)

    LongMap(pairs: _*)
  }

}
