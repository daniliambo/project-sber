import sbt.Keys._
import sbt._

object Dependencies {

  object Version {
    // cannot upgrade to 2.11.12 as `loadFiles()` in `ILoop` was removed in scala 2.11.12 which breaks Apache Spark
    val scala2v11 = "2.11.8"
    val scala2v12 = "2.12.10"

    val spark = Def.setting {
      scalaVersion.value match {
        case `scala2v11` => "2.2.0"
        case `scala2v12` => "2.4.4"
      }
    }

    // circe 0.11.1 is incompatible with enki-plugins
    val circe = Def.setting {
      scalaVersion.value match {
        case `scala2v11` => "0.11.1"
        case `scala2v12` => "0.13.0"
      }
    }

    val hadoopTest      = "2.6.5"
    val hiveMetaStore   = "1.2.1"
    val scalaTest       = "3.3.0-SNAP3"
    val scalaXml        = "1.2.0"
    val typesafeLogging = "3.9.2"
    val scallop         = "3.4.0"
    val scalajHttp      = "2.4.2"
    val pureConfig      = "0.12.3"
    val macroParadise   = "2.1.0"
    val scalaGraphCore  = "1.12.5"
    val scalaGraphDot   = "1.11.5"
    val enumeratum      = "1.5.15"
    val enumeratumCirce = "1.5.23"
    val circeYaml       = "0.10.0"
    val catsEffect      = "2.0.0"
    val cats            = "2.0.0"
    val http4s          = "0.20.23"
    val scalaMock       = "4.4.0"
    val refined         = "0.9.12"
    val decline         = "1.2.0"
    val mockitoScala    = "1.11.4"
    val zio             = "1.0.0-RC21-2"
    val sttpClient      = "2.2.1"
    val poi             = "4.1.1"
    val graphviz        = "0.14.1"
    val jodaTime        = "2.9.3"
    val jodaConvert     = "1.8"
  }

  def sparkCore(sparkVersion: String)     = "org.apache.spark" %% "spark-core"     % sparkVersion
  def sparkSql(sparkVersion: String)      = "org.apache.spark" %% "spark-sql"      % sparkVersion
  def sparkHive(sparkVersion: String)     = "org.apache.spark" %% "spark-hive"     % sparkVersion
  def sparkCatalyst(sparkVersion: String) = "org.apache.spark" %% "spark-catalyst" % sparkVersion
  def circeCore(circeVersion: String)     = "io.circe"         %% "circe-core"     % circeVersion
  def circeGeneric(circeVersion: String)  = "io.circe"         %% "circe-generic"  % circeVersion
  def circeParser(circeVersion: String)   = "io.circe"         %% "circe-parser"   % circeVersion
  def circeLiteral(circeVersion: String)  = "io.circe"         %% "circe-literal"  % circeVersion

  def http4s(name: String): ModuleID = "org.http4s" %% s"http4s-$name" % Version.http4s

  val cats            = "org.typelevel"                %% "cats-core"        % Version.cats
  val catsEffect      = "org.typelevel"                %% "cats-effect"      % Version.catsEffect
  val circeYaml       = "io.circe"                     %% "circe-yaml"       % Version.circeYaml
  val decline         = "com.monovore"                 %% "decline"          % Version.decline
  val declineEffect   = "com.monovore"                 %% "decline-effect"   % Version.decline
  val declineRefined  = "com.monovore"                 %% "decline-refined"  % Version.decline
  val enumeratum      = "com.beachape"                 %% "enumeratum"       % Version.enumeratum
  val enumeratumCirce = "com.beachape"                 %% "enumeratum-circe" % Version.enumeratumCirce
  val graphvizJava    = "guru.nidi"                     % "graphviz-java"    % Version.graphviz
  val hadoopCommon    = "org.apache.hadoop"             % "hadoop-common"    % Version.hadoopTest
  val hadoopHDFS      = "org.apache.hadoop"             % "hadoop-hdfs"      % Version.hadoopTest
  val hiveMetastore   = "org.apache.hive"               % "hive-metastore"   % Version.hiveMetaStore
  val jodaConvert     = "org.joda"                      % "joda-convert"     % Version.jodaConvert
  val jodaTime        = "joda-time"                     % "joda-time"        % Version.jodaTime
  val mockitoScala    = "org.mockito"                  %% "mockito-scala"    % Version.mockitoScala
  val poi             = "org.apache.poi"                % "poi"              % Version.poi
  val poiOoxml        = "org.apache.poi"                % "poi-ooxml"        % Version.poi
  val pureConfig      = "com.github.pureconfig"        %% "pureconfig"       % Version.pureConfig
  val refined         = "eu.timepit"                   %% "refined"          % Version.refined
  val scalaGraphCore  = "org.scala-graph"              %% "graph-core"       % Version.scalaGraphCore
  val scalaGraphDot   = "org.scala-graph"              %% "graph-dot"        % Version.scalaGraphDot
  val scalaMock       = "org.scalamock"                %% "scalamock"        % Version.scalaMock
  val scalaTest       = "org.scalatest"                %% "scalatest"        % Version.scalaTest
  val scalaXml        = "org.scala-lang.modules"       %% "scala-xml"        % Version.scalaXml
  val scalajHttp      = "org.scalaj"                   %% "scalaj-http"      % Version.scalajHttp
  val scallop         = "org.rogach"                   %% "scallop"          % Version.scallop
  val sttpClientCirce = "com.softwaremill.sttp.client" %% "circe"            % Version.sttpClient
  val sttpClientCore  = "com.softwaremill.sttp.client" %% "core"             % Version.sttpClient
  val typesafeLogging = "com.typesafe.scala-logging"   %% "scala-logging"    % Version.typesafeLogging
  val zio             = "dev.zio"                      %% "zio"              % Version.zio
  val scalaCheck      = "org.scalacheck"               %% "scalacheck"       % "1.11.6"

  // Compile-time only
  val macroParadise = ("org.scalamacros" % "paradise" % Version.macroParadise).cross(CrossVersion.patch)

}
