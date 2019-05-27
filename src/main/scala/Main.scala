object Main {

  def main(args: Array[String]): Unit = {

    val connection = new ConnectionBBDD

    connection.connectionToMySql()

  }

}
