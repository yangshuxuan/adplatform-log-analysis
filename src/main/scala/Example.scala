package Example
/**
  * Created by Administrator on 2017/3/21.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object ExampleApp {
  def main(args:Array[String]) {

    implicit val sc = {
      val conf = new SparkConf().setMaster("local[2]").setAppName("CT FOTA SALES COUNT Application")
      new SparkContext(conf)
  }
    //implicit val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //val Array(pt) = args

    sc.stop()

}

}
