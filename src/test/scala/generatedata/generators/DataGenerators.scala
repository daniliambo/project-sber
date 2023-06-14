package generatedata.generators

import generatedata.generators.params.paramsFilterJoin.param.{FilterParams, JoinParams}
import generatedata.generators.params.{GlobalParams, SetOpParams}
import generatedata.graph.constraint.Constraint
import generatedata.graph.constraint.constraintOnFields.ConstraintOnCols
import generatedata.graph.constraint.constraintOnTables.{ConstraintFilter, ConstraintJoin, ConstraintSetOperation}
import generatedata.utils.GenerateTypeFunctions
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalacheck.Gen
import ru.sberbank.bigdata.enki.plan.columns.{Column, SourceColumn}
import ru.sberbank.bigdata.enki.plan.nodes.{Node, SourceTableNode}

import scala.collection.mutable

//Оставлять только свою колонку для джойнов для каждой ноды в сурcтейбл ноде.
//Таким образом у нас будут все констрейнты и одна колонка, по которой будем генерировать
//Будем искать сгенерированные данные как обычно, но выкидывать все ненужные колонки джойнов

trait DataGenerators extends GenerateTypeFunctions {

  def generateData(sourceTable: SourceTableNode, params: mutable.Set[(Node, Constraint)], schema: StructType)(
    implicit spark: SparkSession,
    globalParams: GlobalParams
  ): DataFrame = {

    val generatedSoFar = mutable.Map.empty[SourceTableNode, DataFrame]
    //empty generator to accumulate all constraints
    // add constraint data generation

    val cols = generateColumnsFromSchema(schema)

    val generator = rowGenerator(cols)
    val updatedGenerator = params.map {
      case (node: Node, constraint: ConstraintJoin) => {
        val df = globalParams.mapOfGeneratedConstraintsData.get((node, constraint))
        df match {
          case Some(df) => {
            df.foreach { row =>
              row.toSeq.foreach { value => addJoinConstraint(constraint, node, cols, generator) }
              constraint.constraints.foldLeft(generator) { case (l, r) => addFilterConstraint(r, cols, l) }
            }
          }
            generator
          case _ => generator
        }
      }
      case (node: Node, constraint: ConstraintSetOperation) => generator
      case (node: Node, constraint: ConstraintFilter) =>
        constraint.constraints.foldLeft(generator) { case (l, r) => addFilterConstraint(r, cols, l) }
    }

    val data = updatedGenerator.map(getData(_, globalParams.n))

    val rdd = spark.sparkContext.parallelize(result._1)
    val dfFromData = spark.createDataFrame(rdd, result._2)
    dfFromData

  }

  def generateSchema(columns: Vector[Column]): StructType =
    StructType(columns.map { x =>
      StructField(x.name, x.dataType, x.nullable)
    })

  def addFilterConstraint(constraint: ConstraintOnCols, columns: Vector[Column], generator: Gen[Row]): Gen[Row] = {
    val leftColPos = getPosition(constraint.leftCol.name, columns)
    val rightColPos = getPosition(constraint.rightCol.name, columns)
    generator.filter(row =>
      constraint.constraintType.nullSafeEval(row(leftColPos), row(rightColPos)).asInstanceOf[Boolean]
    )
  }

  def addJoinConstraint(constraint: ConstraintJoin, node: Node, columns: Vector[Column], generator: Gen[Row])(implicit globalParams: GlobalParams): Gen[Row] = {
    for (c <- constraint.constraints) {
      c.leftCol.qualifier.mkString(".")
      if (node) 
      val leftColPos = getPosition(constraint.rightCol.name, columns)
      val rightColPos = getPosition(constraint.rightCol.name, columns)
      globalParams.mapOfGeneratedConstraintsData.update((node, constraint), ()
      )
    }
  }

  def getPosition(columnName: String, columns: Vector[Column]): Int =
    columns.indexWhere(_.name == columnName)

  def getData(generator: Gen[Row], n: Int): Seq[Row] =
    (1 to n).map { idx =>
      generator.sample.get
    }

  def generateColumnsFromSchema(schema: StructType): Vector[Column] =
    schema.map { x =>
      SourceColumn(0, x.name, x.dataType, x.nullable)
    }.toVector
}
