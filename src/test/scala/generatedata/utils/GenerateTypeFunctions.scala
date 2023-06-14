package generatedata.utils

import generatedata.generators.params.GlobalParams
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalacheck.{Arbitrary, Gen}
import ru.sberbank.bigdata.enki.plan.columns.Column

import java.sql.{Date, Timestamp}
import scala.util.Random

trait GenerateTypeFunctions {

  def generateDataAccordingToDataType(dataType: DataType, generatorConfig: GeneratorConfig): Gen[Any] =
    dataType match {
      case StringType             => generateString()
      case BooleanType            => generateBoolean()
      case ByteType               => generateByte()
      case ShortType              => generateShort()
      case IntegerType            => generateInt()
      case LongType               => generateLong()
      case FloatType              => generateFloat()
      case DoubleType             => generateDouble()
      case vc: VarcharType        => generateVarchar(vc.length)
      case char: CharType         => generateChar(char.length)
      case TimestampType          => generateTimestamp()
      case DateType               => generateDate()
      case structType: StructType => generateStructType(structType, generatorConfig)
      case dt: DecimalType =>
        generateDecimal(generatorConfig.decimalTypeMin,
                        generatorConfig.decimalTypeMax,
                        generatorConfig.decimalTypePrecision
        )
      case arr: ArrayType => generateArray(generateDataAccordingToDataType(arr.elementType, generatorConfig))
      case mt: MapType =>
        generateMap(generateDataAccordingToDataType(mt.keyType, generatorConfig),
                    generateDataAccordingToDataType(mt.valueType, generatorConfig),
                    mt.valueContainsNull
        )
      case _ => throw new Exception("Unsupported data type")

    }

  def generateString(): Gen[String] = Gen.alphaStr

  def generateInt(): Gen[Int] = Gen.choose(10, 20)

  def generateBoolean(): Gen[Boolean] = Gen.oneOf(true, false)

  def generateByte(): Gen[Byte] = Gen.choose(Byte.MinValue, Byte.MaxValue)

  def generateShort(): Gen[Short] = Gen.choose(Short.MinValue, Short.MaxValue)

  def generateLong(): Gen[Long] = Gen.choose(Long.MinValue, Long.MaxValue)

  def generateFloat(): Gen[Float] = Gen.choose(Float.MinValue, Float.MaxValue)

  def generateDouble(): Gen[Double] = Gen.const(20) //Gen.choose(Double.MinValue, Double.MaxValue)

  def generateVarchar(length: Int): Gen[String] = {
    val charGen   = Gen.alphaChar
    val stringGen = Gen.listOfN(length, charGen).map(_.mkString)
    stringGen
  }

  def generateChar(upperBound: Int): Gen[String] = {
    val lengthGen = Gen.choose(1, upperBound) // generate a random length between 1 and 100
    val strGen = for {
      length <- lengthGen
      str <- Gen
        .listOfN(length, Gen.alphaChar)
        .map(_.mkString)           // generate a random string of length less than the generated length
    } yield str.padTo(length, ' ') // pad the string with spaces to match the generated length
    strGen
  }

  def generateDecimal(minValue: BigDecimal, maxValue: BigDecimal, precision: Int): BigDecimal = {
    val range       = maxValue - minValue
    val randomValue = (new Random().nextDouble * range.doubleValue) + minValue.doubleValue
    BigDecimal(randomValue).setScale(precision, BigDecimal.RoundingMode.HALF_UP)
  }

  def generateStructType(structType: StructType, generateConfig: GeneratorConfig): Gen[Seq[Any]] = {
    val generators = structType.map { x =>
      generateDataAccordingToDataType(x.dataType, generateConfig)
    }
    Gen.sequence[Vector, Any](generators.toVector)
  }

  def generateBinary(): Gen[List[Byte]] = {
    val lengthGen = Gen.choose(1, 100) // generate a random length between 1 and 100
    val bytesGen  = Gen.containerOfN[List, Byte](lengthGen.sample.get, Arbitrary.arbByte.arbitrary)
    bytesGen
  }

  def generateTimestamp(): Gen[Timestamp] = {
    val millisGen = Gen.choose(0L, Long.MaxValue)
    millisGen.map(long => new Timestamp(long))
  }

  def generateDate(): Gen[Date] = {
    val millisGen = Gen.choose(0L, Long.MaxValue)
    millisGen.map(long => new Date(long))
  }

  def generateArray(gen: Gen[Any]): Gen[Array[Any]] = {
    val lengthGen = Gen.choose(0, 100) // generate a random length between 0 and 100
    val arrGen = for {
      length <- lengthGen
      arr    <- Gen.listOfN(length, gen)
    } yield arr.toArray
    arrGen
  }

  def generateMap[K, V](keyGen: Gen[K], valueGen: Gen[V], valueContainsNull: Boolean): Gen[Map[K, Any]] = {
    val lengthGen = Gen.choose(0, 100) // generate a random length between 0 and 100
    val mapGen = for {
      length <- lengthGen
      keys   <- Gen.listOfN(length, keyGen)
      values <- Gen.listOfN(length, if (valueContainsNull) Gen.option(valueGen) else valueGen)
    } yield keys.zip(values).toMap
    mapGen
  }

  def rowGenerator(columns: Vector[Column])(implicit globalParams: GlobalParams): Gen[Row] = {
    val generators = columns.map { x =>
      generateDataAccordingToDataType(x.dataType, globalParams.generatorConfig)
    }
    Gen.sequence[Vector, Any](generators).map(Row(_: _*))
  }

}
