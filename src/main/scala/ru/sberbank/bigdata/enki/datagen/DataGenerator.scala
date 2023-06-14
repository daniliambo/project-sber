package ru.sberbank.bigdata.enki.datagen

import cats.effect._
import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark.sql.SparkSession
import ru.sberbank.bigdata.enki.framework.DerivedView
import ru.sberbank.bigdata.enki.test.LocalSession

/** Test Data Generator.
  * For DEV environment only.
  * DataGenerator is designed to generate synthetic source data for projects according to specified final project view.
  */
object DataGenerator
    extends CommandApp(
      name   = "DataGenerator",
      header = "Enki generator for test data",
      main = {

        val views: Opts[Seq[DerivedView[_]]] = Opts
          .option[Seq[DerivedView[_]]](
            "views",
            help    = "Full classpaths to final project view, separated by comma",
            short   = "v",
            metavar = "views"
          )

        val rows: Opts[Int] = Opts
          .option[Int]("row-number", short = "r", help = "Number of rows to generate")

        (views, rows).mapN { (views, rows) =>
          implicit val spark: SparkSession = LocalSession.localSpark

          val program: IO[Unit] = SyntheticDataWriter.writeSourceTables(views, rows)

          program.unsafeRunSync()

          spark.stop()

          System.exit(0)
        }
      },
      version = "1.1.0"
    )
