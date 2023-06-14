package ru.sberbank.bigdata.enki.framework

final case class AppendConfiguration(isAppendable: Boolean, ifPartitionExists: AppendExistingAction)
