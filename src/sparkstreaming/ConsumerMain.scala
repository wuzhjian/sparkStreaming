package sparkstreaming

import kafka.serializer.StringDecoder
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ConsumerMain extends Serializable {
  @transient lazy val log = LogManager.getRootLogger;

  def functionToCreateContext(): StreamingContext={
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer")
      .setMaster("spark://localhost:7070")
      .set("spark.local.dir","~/tmp")
      .set("spark.streaming.kafka.maxRatePerPartition","1000")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topicsSet = "yourTopics".split(",").toSet

    val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> "yourBrokerList", "auto.offset.reset" -> "smallest", "group.id" -> "yourGroup")
    val km = new KafkaManager(kafkaParams)

    val kafkaDirectStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    log.warn(s"Initial Done***>>>")
    kafkaDirectStream.cache()

    // do something

    // 更新zk中的offset
    kafkaDirectStream.foreachRDD(rdd =>{
      if (!rdd.isEmpty())
        km.updateZKOffsets(rdd)
    })
    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc = functionToCreateContext()
    ssc.start()
    ssc.awaitTermination()
  }
}
