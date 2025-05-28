package ru.inno.bigdata.reader.json.model

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Map[ // Внешний Map по имени запроса
 * String, // "get_users", "get_orders"
 * Map[ // Внутренний Map по группе
 * String, // "akb", "chat"
 * QueryParams // Собственно параметры
 * ]
 * ]
 */

// Корневой контейнер для всего конфига
final case class SQLTemplatesConfig(
                              @JsonProperty("common_params") commonParams: Map[String, String],
                              @JsonProperty("query_specific_params") querySpecificParams: Map[String, Map[String, QueryParams]]
                            )

// Параметры для конкретного запроса и группы (akb/chat)
final case class QueryParams(
                              @JsonProperty("params") params: Map[String, String]
                            )
