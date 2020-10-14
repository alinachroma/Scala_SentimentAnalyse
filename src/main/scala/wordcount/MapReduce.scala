/*
    @author: Alina Khairullina, Nataliia Pidhorodetska
    @version: 2020.06.15
 */

package wordcount
import java.text.SimpleDateFormat


object MapReduce {

  val dateFormatPattern = "yyyy.MM.dd G 'at' HH:mm:ss z"
  val sdf = new SimpleDateFormat(dateFormatPattern)
  val testFormat = new SimpleDateFormat("yyyy-MM-yy")


  def mapReduceKV[S, B, R](mapFun: (S => List[B]), redFun: (R, B) => R, base: R, l: List[S]): R =
    l.flatMap(mapFun).
      foldLeft[R](base)(redFun)

  /** ********************************************************************************************
   *
   * Aufgabe 4
   *
   * ********************************************************************************************
   */

  /*
    Write a function that determines how many jobs each user sumbmitted
    Result: Map (key:user, value: number)
   */
  def numberOfJobsPerUser(l: List[(String, String, String, Int)]): Map[String, Int] =
  mapReduceKV[(String, String, String, Int), (String, Int), Map[String, Int]](
    x => List((x._2, 1)), // List (
    (map, el) => map.updated(el._1, el._2 + map.getOrElse(el._1, 0)),
    Map(): Map[String, Int],
    l
  )

  /*
  Write a function that determines how many times a job name was used from each user
  Result: Map (key:(user,Job), value: number)
 */
  def numberOfJobsPerUserUsingACertainName(l: List[(String, String, String, Int)]): Map[(String, String), Int] =
    mapReduceKV[(String, String, String, Int), ((String, String), Int), Map[(String, String), Int]](
      x => List(((x._2, x._3), 1)),
      (map, el) => map.updated(el._1, el._2 + map.getOrElse(el._1, 0)),
      Map(): Map[(String, String), Int],
      l
    )


  /*
    Write a function that determines all job names (without duplicates)
    Result: List(jobnames)
*/
  def distinctNamesOfJobs(l: List[(String, String, String, Int)]): List[String] =
    l.map(_._3).toSet.toList.reverse


  /*
    Write a function that determines how many jobs lasted more than 20sec
    Result: Map (key:("more" or "less"), value: number)
  */
  def moreThanXSeconds(l: List[(String, String, String, Int)]): Map[String, Int] =

    mapReduceKV[(String, String, String, Int), (String, Int), Map[String, Int]](
      x => List((x._4)).map(el => (if (el > 20) ("more", 1) else ("less", 1))), //List(("more", 1), ("less",1),("less",1), ("less",1),("more", 1) .... )
      (map, el) => map.updated(el._1, el._2 + map.getOrElse(el._1, 0)),
      Map(): Map[String, Int],
      l
    )

  //(List((false, 0, ""))


  /*
    Write a function that determines the number of were submitted per day
    Result: Map (key:day- format "YYYY-MM-dd" , value: number)
  */
  def numberOfJobsPerDay(l: List[(String, String, String, Int)]): Map[String, Int] = ???
}
