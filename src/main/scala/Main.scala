import Elasticsearch.CsvIngest

object Main extends App {
  val test = new CsvIngest
  test.ingestCitySalesCounts
}
