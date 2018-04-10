import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure, RequestSuccess}

class CsvIngest {
  val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\Sale_Counts_Seas_Adj_County.csv"
  val indexName = "countyseasonallyadjustedsalecounts"

  val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

  def ingest() {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    /**
      * use tototoshi's scala-csv to read csv files in, then put them into elasticsearch
      */
    import com.github.tototoshi.csv._

    // TODO is there a way to customize tototoshi's return values, so that they match the JSON I want right away? Or, can I programmatically handle it so that everything happens the same way?
    val reader: CSVReader = CSVReader.open(fileLocation)
    //  val mapped = reader.allWithHeaders()
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("one").fields(
          nestedField("location").fields(
            intField("RegionID"),
            textField("RegionName"),
            textField("StateName"),
            intField("SizeRank")
          ),
          nestedField("dates").fields(
            dateField("date"),
            doubleField("days")
          )
        ).dateDetection(true).dynamicDateFormats("yyyy-MM")
      )
    }.await // TODO asynchronous calls

    val indexResult = client.execute{
      bulk(
        for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1)
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val test = for {
            (date, days) <- zipped.drop(4)
          } yield {
            Map("date" -> date, "days" -> days)
          }
          val result = Map("location" -> zipped.take(4).toMap, "dates" -> test)
          indexInto(indexName / "one") fields result
        }
      )
    }.await // TODO asynchronous calls
  }
}
