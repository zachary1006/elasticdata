import Elasticsearch.CsvIngest
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient

object Main extends App {
  // Port Elasticsearch is bound to. Elasticsearch default is 9200
  val port: Int = 9200

  // File name and location. Example: "C:\\Parent Folder\\Child Folder\\Name_Of_File.csv"
  val fileLocation: String = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\Test CSV.csv"

  // Name to give the new index. Must be all lowercase, no spaces
  val indexName: String = "test_csv"

  // Name to give the new mapping. If none is given, "default" is used. Example: Some("my_index")
  val indexMapping: Option[String] = None

  val client = HttpClient(ElasticsearchClientUri("localhost", port))

  val newIngest = new CsvIngest
  val result = newIngest.ingestWithHeaders(fileLocation, indexName, indexMapping, client)

  result match {
    case Right(success) => println("Successful ingestion")
    case Left(fail) => println("Failed ingestion" + "\n" + fail)
  }

  client.close()
}
