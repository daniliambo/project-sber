package ru.sberbank.bigdata.enki.logger.layout

import org.apache.log4j.EnhancedPatternLayout
import org.apache.log4j.spi.LoggingEvent

import scala.util.Properties

class SingleLinedEnhancedPatternLayout extends EnhancedPatternLayout {

  @Override
  override def format(event: LoggingEvent): String =
    super.format(event).replace("\n", "") + Properties.lineSeparator
}
