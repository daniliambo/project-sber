package generatedata.utils

final case class GeneratorConfig(decimalTypeMin: BigDecimal = 0,
                                 decimalTypeMax: BigDecimal = 100,
                                 decimalTypePrecision: Int  = 10
)
