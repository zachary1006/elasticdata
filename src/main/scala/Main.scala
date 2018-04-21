import Elasticsearch.CsvIngest
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient

object Main extends App {
  args.length match {
    case 0 => {
      println("No arguments found; please pass in file location, index name, and index mapping")
      System.exit(1)
    }
    case x if x < 3 => {
      println("Insufficient arguments found; please pass in file location, index name, and index mapping")
      System.exit(1)
    }
    case x if x > 3 => {
      println("Too many arguments found; please pass in file location, index name, and index mapping")
      System.exit(1)
    }
  }
  val Array(fileLocation, indexName, indexMapping) = args

  val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

  val newIngest = new CsvIngest
  val result = newIngest.ingestWithHeaders(fileLocation, indexName, indexMapping, client)

  result match {
    case Right(success) => println("Successful ingestion" + "\n" + success)
    case Left(fail) => println("Failed ingestion" + "\n" + fail)
  }

  client.close()
}
