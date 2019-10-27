package streaming

import org.apache.spark.sql.types.StructType

object Schemas {

  val twitterPartialSchema = new StructType()
    .add("id", "long")
    .add("timestamp_ms", "string")
    .add("text", "string")
    .add("user" , new StructType()
      .add("id", "long")
      .add("name", "string")
      .add("location", "string")
    )

}
