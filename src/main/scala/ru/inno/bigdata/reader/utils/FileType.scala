package ru.inno.bigdata.reader.utils

// Интерфейс для чтения ресурсов
private[reader] trait FileType {
  val extension: String
}