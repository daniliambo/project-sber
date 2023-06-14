package ru.sberbank.bigdata.enki.dataset

import java.sql.Timestamp

final case class TimePeriod(n: Int, from: Timestamp, to: Timestamp)
