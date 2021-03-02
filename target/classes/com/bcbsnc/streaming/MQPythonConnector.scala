package com.bcbsnc.streaming

import org.apache.spark.streaming.api.java.{JavaDStream, JavaStreamingContext}
import com.bcbsnc.streaming.IBMMQHelper._


private class MQPythonConnector {

  def createStream(jssc : JavaStreamingContext,
                  host: String = null,
                   port: Int = -1,
                   qmgrName: String = null,
                   channel: String =  null,
                   queueName: String = null,
                   userName: String = null,
                  password: String = null,
                   waitInterval: String = null,
                  keepMessages: String = null,
                   mqRateLimit: String = "-1",
                   mqCCSID: Int = 0) : JavaDStream[String] = {



    val jstream = jssc.receiverStream(new IBMMQHelper(host,port,
      qmgrName,channel,queueName,userName,password,waitInterval,keepMessages,mqRateLimit,mqCCSID))


    return jstream

  }

  def commitMessage() : Unit = {
    try {
      commitFromPython()
    }
    catch {
      case e: Exception => println(e)
    }
  }

  def rollback() : Unit = {
    try {
      backoutFromPython()
    }
    catch {
      case e: Exception => println(e)
    }
  }

}
