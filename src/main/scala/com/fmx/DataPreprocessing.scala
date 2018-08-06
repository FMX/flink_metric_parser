package com.fmx

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import java.io.File

import com.alibaba.fastjson.JSON
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Skeleton for a Flink Job.
  *
  * For a full example of a Flink Job, see the WordCountJob.scala file in the
  * same package/directory or have a look at the website.
  *
  * You can also generate a .jar file that you can submit on your Flink
  * cluster. Just type
  * {{{
  *   sbt clean assembly
  * }}}
  * in the projects root directory. You will find the jar in
  * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
  *
  */

import org.apache.flink.configuration.ConfigConstants

object DataPreprocessing {
  def main(args: Array[String]) {
    // set up the execution environment
    var inputFile = "/Users/fmx/workfiles/metrics_data/es2.data"
    if (args.length == 1) {
      inputFile = args(0)
    }
    var outputFile = inputFile + ".processed"
    val tmpFile=new File(outputFile)
    if (tmpFile.exists()) {
      tmpFile.delete()
    }
    val conf: Configuration = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    env.setParallelism(1)

    val esdSource = env.readTextFile(inputFile)
    val esdTrans = esdSource.filter(str => {
      val json = JSON.parseObject(str)
      json.containsKey("message")
    }).map(str => {
      val json = JSON.parseObject(str)
      json.getString("message")
    })
    esdTrans.writeAsText(outputFile)

    println(env.getExecutionPlan)
    // execute program
    env.execute("Metric Parser Job")
  }
}
