package cep

object ScalaDemo {
  def main(args: Array[String]): Unit = {
    var map = Map[String, String]("name" -> "jason", "age" -> "500", "test_100" -> "test_100", "test_101" -> "test_101") //引用可变,支持读写操作;
    map += ("city" -> "北京") //新增
    println(map)
  }
}
