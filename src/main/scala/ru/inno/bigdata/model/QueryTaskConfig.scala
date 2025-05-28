package ru.inno.bigdata.model

// Конфигурация считываемых данных из базы данных
case class QueryTaskConfig(cpId: String, groupId: String, query: String, targetTable: String)
