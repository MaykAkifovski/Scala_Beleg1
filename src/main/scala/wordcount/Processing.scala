package wordcount

class Processing {

  /** ********************************************************************************************
    *
    * Aufgabe 1
    *
    * ********************************************************************************************
    */
  def getWords(line: String): List[String] = {
    line
      .toLowerCase()
      .replaceAll("[^a-z]", " ")
      //      .replaceAll(" +", " ")
      .split(" +")
      .toList
  }

  def getAllWords(l: List[(Int, String)]): List[String] = {
    l.flatMap(list => getWords(list._2))
      .filter(word => !word.isEmpty)
  }

  def countTheWords(l: List[String]): List[(String, Int)] = {
    l.foldLeft(Map.empty[String, Int]) {
      (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
    }
      .toList
  }

  /** ********************************************************************************************
    *
    * Aufgabe 2
    *
    * ********************************************************************************************
    */

  def mapReduce[S, B, R](mapFun: (S => B),
                         redFun: (R, B) => R,
                         base: R,
                         l: List[S]
                        ): R =

    l.map(mapFun).foldLeft(base)(redFun)

  def countTheWordsMR(l: List[String]): List[(String, Int)] = {
    mapReduce[String, (String, Int), Map[String, Int]](
      (word) => (word, 1),
      (list, tuple) => list + (tuple._1 -> (list.getOrElse(tuple._1, 0) + 1)),
      Map.empty[String, Int],
      l
    ).toList
  }


  /** ********************************************************************************************
    *
    * Aufgabe 3
    *
    * ********************************************************************************************
    */

  def getAllWordsWithIndex(l: List[(Int, String)]): List[(Int, String)] = {
    l.foldLeft(List.empty[(Int, String)])((list, tuple) => list ++ getWords(tuple._2)
      .filter(word => !word.isEmpty)
      .map(word => (tuple._1, word)))
  }

  def createInverseIndex(l: List[(Int, String)]): Map[String, List[Int]] = {

    getAllWordsWithIndex(l)
      .groupBy(_._2)
      .mapValues(value => value.map(_._1))
  }

  def andConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {

    words
      .foldLeft(List.empty[List[Int]])((list, word) => list ++ List(invInd.getOrElse(word, List(-1))))
      .distinct
      .map(_.toSet)
      .reduce(_.intersect(_))
      .filter(_ != List(-1))
      .toList
  }

  def orConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {

    words
      .foldLeft(List.empty[Int])((list, word) => list ++ invInd.getOrElse(word, List(-1)))
      .distinct
      .filter(_ != -1)
  }
}


object Processing {

  def getData(filename: String): List[(Int, String)] = {

    val url = getClass.getResource("/" + filename).getPath
    val src = scala.io.Source.fromFile(url)
    val iter = src.getLines()
    var c = -1
    val result = (for (row <- iter) yield {
      c = c + 1; (c, row)
    }).toList
    src.close()
    result
  }
}