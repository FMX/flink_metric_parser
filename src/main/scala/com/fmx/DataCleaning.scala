package com.fmx


import java.io.File

import com.alibaba.fastjson.JSON
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object DataCleaning {
  def main(args: Array[String]): Unit = {
    var inputFile = "/Users/fmx/workfiles/kafka_data/rawevent/30_245.log"
    if (args.length == 1) {
      inputFile = args(0)
    }
    var outputFile = inputFile + ".processed"
    val tmpFile = new File(outputFile)
    if (tmpFile.exists()) {
      tmpFile.delete()
    }
    val conf: Configuration = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    env.setParallelism(1)

    val esdSource = env.readTextFile(inputFile)
    val esdTrans = esdSource.filter(str => {
      try {
        val json = JSON.parseObject(str)
        true
      } catch {
        case _ => {
          false
        }
      }
    }).map(str => {
      val json = JSON.parseObject(str)
      if (json.containsKey("rawEvent")) {
        json.remove("rawEvent")
      }
      if (json.containsKey("responseMsg")) {
        json.remove("responseMsg")
      }
      json.toJSONString
    })
    esdTrans.writeAsText(outputFile)

    println(env.getExecutionPlan)
    // execute program
    env.execute("Metric Parser Job")
  }


}
