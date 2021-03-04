package com.project.app

object Test {
  def main(args: Array[String]): Unit = {
    val ints = List(1,2,3)
    val strings: Iterator[String] = ints.iterator.map(
      i => {
        println(i)
        "s"
      }
    ) // 如果下面没有调用next()，那么什么都不会打印
    while(strings.hasNext){
      strings.next()
    }
  }
}
