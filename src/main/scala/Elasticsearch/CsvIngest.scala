package Elasticsearch

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.github.tototoshi.csv._

class CsvIngest {
  def ingestWithHeaders(fileLocation: String, indexName: String, indexMapping: String, client: HttpClient) = {
    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()

    val indexResult = client.execute {
      bulk(
        for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1)
        } yield {
          val zipped = headers.zip(stringMap).toMap
          indexInto(indexName / indexMapping) fields zipped
        }
      )
    }.await // TODO asynchronous calls

    client.close()

    indexResult
  }

  def ingestCountySalesCounts {
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\Sale_Counts_Seas_Adj_County.csv"
    val indexName = "countyseasonallyadjustedsalecounts"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

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

  def ingestCitySalesCounts {
    // FIXME this does not work, for some unknown reason.
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\Sale_Counts_Seas_Adj_City.csv"
    val indexName = "city_seasonally_adjusted_sale_counts"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("one").fields(
          intField("RegionID"),
          textField("RegionName"),
          textField("StateName"),
          intField("SizeRank"),
          dateField("month").format("yyyy-MM"),
          doubleField("numSales")
        )
      )
    }.await // TODO asynchronous calls

    client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1).take(3000)
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(4)
          } yield {
            Map("month" -> date, "numSales" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionID").toMap
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "StateName").toMap
              ++ zipped.filter(item => item._1 == "SizeRank").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1).drop(3000)
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(4)
          } yield {
            Map("month" -> date, "numSales" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionID").toMap
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "StateName").toMap
              ++ zipped.filter(item => item._1 == "SizeRank").toMap)
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

  def ingestCityMedianListingPrice {
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\City_MedianListingPrice_AllHomes.csv"
    val indexName = "city_median_listing_price_all_homes"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("one").fields(
          keywordField("RegionName"),
          keywordField("State"),
          keywordField("Metro"),
          keywordField("CountyName"),
          intField("SizeRank"),
          dateField("month").format("yyyy-MM"),
          doubleField("median_price")
        )
      )
    }.await // TODO asynchronous calls

    val indexResult = client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1).take(3000) // HACK taking the whole file is too big; we need to index it in chunks
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, price) <- zipped.drop(5)
          } yield {
            Map("month" -> date, "median_price" -> price)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "State").toMap
              ++ zipped.filter(item => item._1 == "Metro").toMap
              ++ zipped.filter(item => item._1 == "CountyName").toMap
              ++ zipped.filter(item => item._1 == "SizeRank").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    val indexResult2 = client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1).drop(3000) // HACK taking the whole file is too big; we need to index it in chunks
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, price) <- zipped.drop(5)
          } yield {
            Map("month" -> date, "median_price" -> price)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "State").toMap
              ++ zipped.filter(item => item._1 == "Metro").toMap
              ++ zipped.filter(item => item._1 == "CountyName").toMap
              ++ zipped.filter(item => item._1 == "SizeRank").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    client.close()
  }

  def ingestCityMedianListingPricePerSqft {
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\City_MedianListingPricePerSqft_AllHomes.csv"
    val indexName = "city_median_listing_price_per_square_foot_all_homes"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("one").fields(
          textField("RegionName"),
          textField("State"),
          textField("Metro"),
          textField("CountyName"),
          intField("SizeRank"),
          dateField("month").format("yyyy-MM"),
          doubleField("median_price")
        )
      )
    }.await // TODO asynchronous calls

    val indexResult = client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1).take(3000) // HACK taking the whole file is too big; we need to index it in chunks
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(5)
          } yield {
            Map("month" -> date, "numSales" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "State").toMap
              ++ zipped.filter(item => item._1 == "Metro").toMap
              ++ zipped.filter(item => item._1 == "CountyName").toMap
              ++ zipped.filter(item => item._1 == "SizeRank").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    val indexResult2 = client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1).drop(3000) // HACK taking the whole file is too big; we need to index it in chunks
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(5)
          } yield {
            Map("month" -> date, "numSales" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "State").toMap
              ++ zipped.filter(item => item._1 == "Metro").toMap
              ++ zipped.filter(item => item._1 == "CountyName").toMap
              ++ zipped.filter(item => item._1 == "SizeRank").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    client.close()
  }

  def ingestCityZhviAllHomes {
    //HACK this request cannot take place at once because it overflows memory. Take it in chunks.
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\City_Zhvi_AllHomes.csv"
    val indexName = "city_zhvi_all_homes_2"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("one").fields(
          intField("RegionID"),
          keywordField("RegionName"),
          keywordField("State"),
          keywordField("Metro"),
          keywordField("CountyName"),
          intField("SizeRank"),
          dateField("month").format("yyyy-MM"),
          doubleField("median_price")
        )
      )
    }.await // TODO asynchronous calls

    val oneThousandSets = for {
      r <- 1 to 12000 by 1000
    } yield {
      mapped.drop(r).take(1000)
    }

    for {
      stringList <- oneThousandSets
    } client.execute {
        bulk(
          (for {
            headers <- mapped.take(1)
            stringMap <- stringList
          } yield {
            val zipped = headers.zip(stringMap)
            val dateMap = for {
              (date, days) <- zipped.drop(6)
            } yield {
              Map("month" -> date, "median_price" -> days)
            }
            dateMap.map(m => {
              indexInto(indexName / "one") fields (m
                ++ zipped.filter(item => item._1 == "RegionID").toMap
                ++ zipped.filter(item => item._1 == "RegionName").toMap
                ++ zipped.filter(item => item._1 == "State").toMap
                ++ zipped.filter(item => item._1 == "Metro").toMap
                ++ zipped.filter(item => item._1 == "CountyName").toMap
                ++ zipped.filter(item => item._1 == "SizeRank").toMap)
            })
          }).flatten
        )
      }.await // TODO asynchronous calls
    
    client.close()
  }

  def ingestDaysOnZillowPublic {
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\DaysOnZillow_Public_County.csv"
    val indexName = "days_on_zillow_public_county"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("one").fields(
          textField("RegionName"),
          textField("StateName"),
          textField("RegionType"),
          textField("CBSA Title"),
          intField("SizeRank"),
          dateField("month").format("yyyy-MM"),
          doubleField("num_days")
        )
      )
    }.await // TODO asynchronous calls

    client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1) // HACK taking the whole file is too big; we need to index it in chunks
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(5)
          } yield {
            Map("month" -> date, "numSales" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "StateName").toMap
              ++ zipped.filter(item => item._1 == "RegionType").toMap
              ++ zipped.filter(item => item._1 == "CBSA Title").toMap
              ++ zipped.filter(item => item._1 == "SizeRank").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    client.close()
  }

  def ingestInventoryMeasureCity {
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\InventoryMeasure_City_Public.csv"
    val indexName = "inventory_measure_city_public"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("one").fields(
          textField("RegionName"),
          textField("RegionType"),
          textField("CountyName"),
          textField("Metro"),
          textField("StateFullName"),
          textField("DataType"),
          dateField("month").format("yyyy-MM"),
          doubleField("num_inventory")
        )
      )
    }.await // TODO asynchronous calls

    client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1).take(3000) // HACK taking the whole file is too big; we need to index it in chunks
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(6)
          } yield {
            Map("month" -> date, "num_inventory" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "RegionType").toMap
              ++ zipped.filter(item => item._1 == "CountyName").toMap
              ++ zipped.filter(item => item._1 == "Metro").toMap
              ++ zipped.filter(item => item._1 == "StateFullName").toMap
              ++ zipped.filter(item => item._1 == "DataType").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1).drop(3000).take(3000) // HACK taking the whole file is too big; we need to index it in chunks
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(6)
          } yield {
            Map("month" -> date, "num_inventory" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "RegionType").toMap
              ++ zipped.filter(item => item._1 == "CountyName").toMap
              ++ zipped.filter(item => item._1 == "Metro").toMap
              ++ zipped.filter(item => item._1 == "StateFullName").toMap
              ++ zipped.filter(item => item._1 == "DataType").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1).drop(6000) // HACK taking the whole file is too big; we need to index it in chunks
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(6)
          } yield {
            Map("month" -> date, "num_inventory" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "RegionType").toMap
              ++ zipped.filter(item => item._1 == "CountyName").toMap
              ++ zipped.filter(item => item._1 == "Metro").toMap
              ++ zipped.filter(item => item._1 == "StateFullName").toMap
              ++ zipped.filter(item => item._1 == "DataType").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    client.close()
  }

  def ingestSalesPricesCity {
    val fileLocation = "D:\\Cal State Fullerton MSE Program\\CPSC 597 II Graduate Project\\Sale_Prices_City.csv"
    val indexName = "sale_prices_city"

    val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

    val reader: CSVReader = CSVReader.open(fileLocation)
    val mapped: List[List[String]] = reader.all()
    reader.close()


    client.execute {
      createIndex(indexName).mappings(
        mapping("one").fields(
          intField("RegionID"),
          textField("RegionName"),
          keywordField("StateName"),
          intField("SizeRank"),
          dateField("month").format("yyyy-MM"),
          doubleField("avg_sales_prices")
        )
      )
    }.await // TODO asynchronous calls

    client.execute {
      bulk(
        (for {
          headers <- mapped.take(1)
          stringMap <- mapped.drop(1) // HACK taking the whole file is too big; we need to index it in chunks
        } yield {
          val zipped = headers.zip(stringMap)
          // break out last ones, and put in their own map
          val dateMap = for {
            (date, days) <- zipped.drop(4)
          } yield {
            Map("month" -> date, "avg_sales_prices" -> days)
          }
          dateMap.map(m => {
            indexInto(indexName / "one") fields (m
              ++ zipped.filter(item => item._1 == "RegionID").toMap
              ++ zipped.filter(item => item._1 == "RegionName").toMap
              ++ zipped.filter(item => item._1 == "StateName").toMap
              ++ zipped.filter(item => item._1 == "SizeRank").toMap
              ++ zipped.filter(item => item._1 == "DataType").toMap)
          })
        }).flatten
      )
    }.await // TODO asynchronous calls

    client.close()
  }
}
