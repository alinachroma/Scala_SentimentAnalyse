/*
    @author: Alina Khairullina, Nataliia Pidhorodetska
    @version: 2020.06.15
 */

package wordcount

class Processing {

  /** ********************************************************************************************
   *
   * Aufgabe 1
   *
   * ********************************************************************************************
   */

  /*
   * Extracts all words from a line
   *
   * 1. Removes all characters which are not letters (A-Z or a-z)
   * 2. Shifts all words to lower case
   * 3. Extracts all words and put them into a list of strings
   */
  def getWords(line: String): List[String] =
    line.toLowerCase.replaceAll("[^a-z]", " ").split(" ").filter(_ != "").toList


  /*
   * Extracts all words from a List containing line number and line tuples
   * The words should be in the same order as they occur in the source document
   *
   * Hint: Use the flatMap function
   */
  def getAllWords(l: List[(Int, String)]): List[String] =
    l.flatMap(x => getWords(x._2))


  /*
  *  Gets a list of words and counts the occurrences of the individual words
  */
  def countWords(l: List[String]): List[(String, Int)] =
    l.groupBy(elem => elem).map(t => (t._1, t._2.length)).toList



  //in one line
  def countWordsMR(l: List[String]): List[(String, Int)] =
    l.map( x => (x, 1)).foldLeft(Map(): Map[String, Int])((map, el) => map.updated(el._1, el._2 + map.getOrElse(el._1, 0))).toList


  def countWordsMR2(l: List[String]): List[(String, Int)] = {
    def mapReduce[S, B, R](mapFun: S => B, redFun: (R, B) => R, base: R, list: List[S]): R =
      list.map(mapFun).foldLeft(base)(redFun)

    def insertList(map: Map[String, Int], el: (String, Int)): Map[String, Int] =
      map.updated(el._1, el._2 + map.getOrElse(el._1, 0))

    mapReduce[String, (String, Int), Map[String, Int]](
      x => (x, 1),
      insertList,
      Map(): Map[String, Int],
      l).toList
  }


  /** ********************************************************************************************
   *
   * Aufgabe 2
   *
   * ********************************************************************************************
   */

  def getAllWordsWithIndex(l: List[(Int, String)]): List[(Int, String)] =
    l.flatMap(x => getWords(x._2).map(y => (x._1, y)))


  def createInverseIndex(l: List[(Int, String)]): Map[String, List[Int]] =
    l.groupBy(elem=>elem._2).map { case (k,v) => (k, v.map(x => x._1)) }


  def orConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] =
    invInd.filter(elem => words.contains(elem._1)).flatMap(x=>x._2).toSet.toList



  def andConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {
    invInd.filter(mapElem => words.contains(mapElem._1)) //1
      .flatMap(_._2).toList    //2
      .groupBy(el=>el)          //3
      .filter(_._2.length==words.length)  //4
      .map(_._1).toList  //5

  }

  // andConjunction
  // words = (hi, my);  invInd = Map(hi -> List(0,2), my -> List(0), foo -> List(2,0))
  // 1. Map(hi -> List(0,2), my -> List(0))
  // 2. List(0,2,0)
  // 3. Map(0 -> List(0,0), 2 -> List(2))
  // 4. Map(0 -> List(0,0))
  // 5. List (0)

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
