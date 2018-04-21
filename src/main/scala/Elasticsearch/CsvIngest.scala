package Elasticsearch

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient

class CsvIngest {
  def ingestCountySalesCounts {
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\Sale_Counts_Seas_Adj_County.csv"
    val indexName = "countyseasonallyadjustedsalecounts"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

    import com.sksamuel.elastic4s.http.ElasticDsl._

    /**
      * use tototoshi's scala-csv to read csv files in, then put them into elasticsearch
      */
    import com.github.tototoshi.csv._

    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("one").fields(
          intField("RegionID"),
          textField("RegionName"),
          dateField("month").format("yyyy-MM"),
          doubleField("numSales")
        )
      )
    }.await // TODO asynchronous calls

    val indexResult = client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1)
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(4)
          } yield {
            Map("month" -> date, "numSales" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m ++ zipped.filter(item => item._1 == "RegionID").toMap ++ zipped.filter(item => item._1 == "RegionName").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    client.close()
  }

  def ingestNipomoRealEstate {
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\RealEstate.csv"
    val indexName = "nipomo_real_estate"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

    import com.sksamuel.elastic4s.http.ElasticDsl._

    /**
      * use tototoshi's scala-csv to read csv files in, then put them into elasticsearch
      */
    import com.github.tototoshi.csv._

    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("nipomo").fields(
          intField("MLS"),
          textField("Location"),
          intField("Price"),
          intField("Bedrooms"),
          intField("Bathrooms"),
          intField("Size"),
          doubleField("Price/SQ.Ft"),
          textField("Status")
        )
      )
    }.await // TODO asynchronous calls

    val indexResult = client.execute {
      bulk(
        for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1)
        } yield {
          val zipped = headers.zip(stringMap).toMap
          indexInto(indexName / "nipomo") fields zipped
        }
      )
    }.await // TODO asynchronous calls

    client.close()
  }


  def ingestAgeOfInventory {
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\AgeOfInventory_Metro_Public.csv"
    val indexName = "age_of_inventory_metro_public"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

    import com.sksamuel.elastic4s.http.ElasticDsl._

    /**
      * use tototoshi's scala-csv to read csv files in, then put them into elasticsearch
      */
    import com.github.tototoshi.csv._

    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("one").fields(
          textField("RegionName"),
          textField("StateFullName"),
          dateField("month").format("yyyy-MM"),
          intField("age_in_days")
        )
      )
    }.await // TODO asynchronous calls

    val indexResult = client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1)
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(4)
          } yield {
            Map("month" -> date, "age_in_days" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m ++ zipped.filter(item => item._1 == "RegionName").toMap ++ zipped.filter(item => item._1 == "StateFullName").toMap )
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    client.close()
  }
}
