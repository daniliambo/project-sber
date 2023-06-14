package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.data.State
import cats.syntax.applicative._
import cats.syntax.option._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Generator}
import org.apache.spark.sql.catalyst.plans.logical.Generate
import ru.sberbank.bigdata.enki.plan.columns.{ColumnReference, ExpressionColumn}
import ru.sberbank.bigdata.enki.plan.nodes._

private[converter] object GenerateTransformer extends PlanTransformer[Generate] {

  override def transform(plan: Generate, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] = {
    val Generate(generatorExpr, join, outer, qualifier, output, _) = plan

    for {
      generatorNode <- children.head match {
        case generatorNode: GeneratorNode => generatorNode.pure[ContextState]

        // Cases like
        // FROM table1 JOIN table2 ON ...
        // LATERAL VIEW ...
        case joinNode: JoinNode => GeneratorNode(joinNode, Vector.empty).pure[ContextState]

        case otherNode => wrapInAliased(otherNode).map(GeneratorNode(_, Vector.empty))
      }
      generatorQualifier <- updateQualifier(qualifier)
      aliasContext       <- getContext
    } yield {
      val references = getColumnReferencesMap(generatorNode.mainNode)

      val updatedGenerator = updateExpression(generatorExpr, references, aliasContext).asInstanceOf[Generator] // Safe
      val generatorColumns = convertGenerator(updatedGenerator, generatorQualifier, output, references)

      generatorNode.addGenerator(updatedGenerator, join = true, outer, generatorQualifier, generatorColumns)
    }
  }

  private def updateQualifier(qualifier: Option[String]): ContextState[String] =
    State { aliasContext =>
      qualifier
        .filter(alias => alias != "as" && alias != "")
        .fold(aliasContext.generateName)(aliasContext.update)
    }

  private def convertGenerator(
    generator: Generator,
    qualifier: String,
    generatorOutput: Seq[Attribute],
    references: ReferenceMap
  ): Vector[ColumnReference] = {
    val sourceTableColumns = getSourceTableColumns(generator, references)

    generatorOutput.map { attr =>
      val underlyingColumn = ExpressionColumn(attr.exprId.id, attr.name, generator, sourceTableColumns)
      ColumnReference.fromColumn(underlyingColumn, qualifier.some)
    }.toVector
  }

}
