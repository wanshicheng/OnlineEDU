package com.skillip.onlineedu.common

import java.text.DecimalFormat

object MyTest {
  def main(args: Array[String]): Unit = {
    val df = new DecimalFormat("0.0")
    println(df.format(10.33))
  }
}
