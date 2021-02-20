package com.project.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
  def load(propertyName: String): Properties = {
    val properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader
      .getResourceAsStream(propertyName),"UTF-8"))
    properties
  }
}
