package utils.cli

import scala.annotation.tailrec
import scala.sys.exit

object ProcessorUtils {
  def parseInputParams(args: Array[String]): Map[String, Any] = {
    if (args.length == 0) {
      throw new Exception("Empty input parameters found")
    }

    @tailrec
    def iterateArgs(map: Map[String, Any], list: List[String]): Map[String, Any] = {
      list match {
        case Nil => map
        case "--inputPath" :: value :: tail =>
          iterateArgs(map ++ Map("inputPath" -> value), tail)
        case unknown :: _ =>
          println("Unknown option " + unknown)
          exit(1)
      }
    }

    iterateArgs(Map(), args.toList)
  }
}
