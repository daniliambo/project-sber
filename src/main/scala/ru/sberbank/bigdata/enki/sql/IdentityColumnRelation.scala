package ru.sberbank.bigdata.enki.sql

trait IdentityColumnRelation {
  implicit def identity[X]: TypeRelation[X, X] = new TypeRelation[X, X] {}
}
