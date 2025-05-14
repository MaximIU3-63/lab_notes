package reader.model

case class QueryParamsConfig(
                              common_params: Map[String, String],
                              query_specific_params: Map[String, Map[String, String]]
                            )
