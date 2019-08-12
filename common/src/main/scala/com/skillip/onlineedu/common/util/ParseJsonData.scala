package com.skillip.onlineedu.common.util

import com.alibaba.fastjson.{JSON, JSONObject}

object ParseJsonData {
  def getJsonData(data: String)  = {
    val obj = JSON.parseObject(data)
    if (obj.isInstanceOf[JSONObject]) Some(obj)
    else None
  }
}
