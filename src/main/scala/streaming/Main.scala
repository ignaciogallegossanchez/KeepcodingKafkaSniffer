package streaming

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Main {

  /**
   * Auxiliar objects
   */
  val decrypter = new AESEncryption(Array[Byte]('s','E','c','R','e','T','c','L','0','a','c','a','l','a','n','d'))
  val notifier = new Notifier()


  /**
   * UDF to decrypt a given message
   * @return decrypted message
   */
  def decrypt = (message: String) => {
    decrypter.decrypt(message)
  }


  /**
   * UDF that:
   *    - Lowercase the input
   *    - Split words
   * @return array of splitted words(strings) in lowercase
   */
  def splitMessage = (message: String) => {
    message.toLowerCase.split("\\W+")
  }


  /**
   * Main function
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // Configure logging
    Logger.getLogger("org").setLevel(Level.ERROR)


    // Configure sparkSession. Will use al cores of local server ("local[*]")
    val spark = SparkSession
      .builder()
      .appName("Cloacalandia")
      .master("local[*]")
      .config("spark.io.compression.codec", "snappy") // Avoids LZ4BlockInputStream.init exception when grouping
      .getOrCreate()

    import spark.implicits._


    // Register UDF's for later use
    val decrypter = spark.udf.register("decrypter", decrypt)
    val splitter = spark.udf.register("splitter",splitMessage)


    // Read from Kafka and prepare input
    val df1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "keepcoding") // Kafka topic
      .load()
      .select(decrypter(col("value")).as("decryptedValue"))
      .select(from_json(col("decryptedValue").cast("string"), Schemas.twitterPartialSchema).as("data"))
      .select(
          expr("cast(cast(data.timestamp_ms as double)/1000 as timestamp) as timestamp"),
          col("data.id").as("MsgID"),
          col("data.text").as("MsgText"),
          col("data.user.id").as("UserID"),
          col("data.user.name").as("UserName"),
          col("data.user.location").as("UserLocation")
        )
      .withWatermark("timestamp", "1 hour")


    // Filtered words definition
    val preposiciones = Set("a", "ante", "bajo", "cabe", "con", "contra", "de", "desde", "durante", "en", "entre", "hacia", "hasta", "mediante", "para", "por", "según", "sin", "so", "sobre", "tras", "versus", "vía").toSeq
    val conjuncionCoordinada = Set("e", "empero", "mas", "ni", "o", "ora", "pero", "sino", "siquiera", "u", "y").toSeq
    val conjuncionSubordinada = Set("aunque", "como", "conque","cuando","donde", "entonces","ergo", "incluso", "luego", "mientras", "porque", "pues", "que", "sea", "si", "ya").toSeq
    val articulos = Set("el", "la", "los", "las", "un", "uno", "unos", "unas", "al", "del").toSeq


    // Convert to a list of words, filtered and clean, and without dupplicated messages, and sorted
    val dfWords = df1
      .dropDuplicates("MsgID")
      .select(
        explode(splitter(col("MsgText"))).as("Word")//,
      )
      .filter(row => {
        val word = row.getAs[String]("Word")
        word.size > 1 && !( articulos.contains(word) ||
          preposiciones.contains(word) ||
          conjuncionSubordinada.contains(word) ||
          conjuncionCoordinada.contains(word))
      })
      .groupBy($"Word").count( )
      .orderBy($"count".desc)
      .select($"Word")


    // Put table in memory ready for requests
    val query = dfWords.writeStream
      .trigger(Trigger.ProcessingTime("1 hour"))
      .format("memory")
      .outputMode("complete")
      .queryName("resultTable")
      .start()


    // Blacklisted words
    val blacklist = Seq(
      ("dato"),
      ("intellij"),
      ("bleh")
    ).toDF("BlacklistWord").as("black")


    // Check if top-10 words are blacklisted, and notify
    while(true){
      val topTen = spark.sql("SELECT * from resultTable LIMIT 10").as("result")
      val result = topTen.join(blacklist, $"result.Word"=== col("black.BlacklistWord"), "inner")
      if (result.count() > 0)
        notifier.notify("ALARMA AL GOBIERNO DE CLOACALANDIA")
      Thread.sleep(3600*1000)
    }
  }
}
