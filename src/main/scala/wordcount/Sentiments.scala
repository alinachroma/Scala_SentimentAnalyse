package wordcount

import java.awt.{Color, GridLayout}

import org.jfree.chart.{ChartPanel, JFreeChart}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYDotRenderer
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}
import org.jfree.ui.ApplicationFrame
import org.jfree.util.ShapeUtilities


/**
  * @author hendrik
  * modified by akarakochev
  *
  * also modified by Alina Khairullina, Nataliia Pidhorodetska during the Exercise
  */
class Sentiments(sentiFile: String) {

  val sentiments: Map[String, Int] = getSentiments(sentiFile)

  val proc = new Processing()

  /** ********************************************************************************************
    *
    * Aufgabe 3
    *
    * ********************************************************************************************
    */
  def mapReduce[S, B, R](mapFun: S => B, redFun: (R, B) => R, base: R, list: List[S]): R =
    list.map(mapFun).foldLeft(base)(redFun)



  def getDocumentGroupedByCounts(filename: String, wordCount: Int): List[(Int, List[String])] =
    proc.getAllWords(Processing.getData(filename)).grouped(wordCount).toList.zip(LazyList from 1).map(x => x.swap)


  def getDocumentSplitByPredicate(filename: String, predicate: String => Boolean): List[(Int, List[String])] = {
    def toSpan(list: List[String])(predicate: String => Boolean): List[List[String]] = list match {
      case Nil => Nil
      case _ => list.span(predicate) match {case (head,tail) => head::toSpan(tail)(x => !predicate(x))}
    }
    //https://stackoverflow.com/questions/581665/scala-splitting-a-list-using-a-predicate-for-sublists
    //here we saw the idea with span, but there were still lots of job to do after using that :)

    var spannedList = toSpan(Processing.getData(filename).map(x => (x._2)))(predicate)
    for(i <- spannedList) {
      i match {
        case (head :: tail) => if (predicate(head) && i.contains(head)) spannedList = spannedList.filter(x => !x.equals(i));
        case Nil => spannedList = spannedList.filter(x => !x.equals(Nil))}}

    spannedList.map(x => proc.getWords(x.mkString)).drop(1).zip(LazyList from 1).map(x => x.swap)
  }


def analyseSentiments(l: List[(Int, List[String])]): List[(Int, Double, Double)] = {
  def sentimentswerte (sum: Double, elem: String): Double = sum + sentiments.getOrElse(elem, 0)
  def count (l: List[Int], elem: String): List[Int] = sentiments.getOrElse(elem, 0) :: l

  l.foldLeft(List(): List[(Int, Double, Double)])((list, el) =>
    ( el._1-1, //Abschnittsnummer
      el._2.foldLeft(0.0)(sentimentswerte) / el._2.foldLeft(List(): List[Int])(count).filter(_ != 0).length.toDouble, //Sentimentwert
      el._2.foldLeft(List(): List[Int])(count).filter(_ != 0).length.toDouble / el._2.length.toDouble) :: list) // relativen Anzahl von Wörtern
    .reverse
}
  /** ********************************************************************************************
  *
  * Helper Functions
  *
  * ********************************************************************************************
  */

def getSentiments(filename: String): Map[String, Int] = {
  val url = getClass.getResource("/" + filename).getPath
  val src = scala.io.Source.fromFile(url)
  val iter = src.getLines()
  val result: Map[String, Int] = (for (row <- iter) yield {
    val seg = row.split("\t"); (seg(0) -> seg(1).toInt)
  }).toMap
  src.close()
  result
}

def createGraph(data: List[(Int, Double, Double)], xlabel:String="Abschnitt", title:String="Sentiment-Analyse"): Unit = {

  //create xy series
  val sentimentsSeries: XYSeries = new XYSeries("Sentiment-Werte")
  data.foreach { case (i, sentimentValue, _) => sentimentsSeries.add(i, sentimentValue) }
  val relWordsSeries: XYSeries = new XYSeries("Relative Haeufigkeit der erkannten Worte")
  data.foreach { case (i, _, relWordsValue) => relWordsSeries.add(i, relWordsValue) }

  //create xy collections
  val sentimentsDataset: XYSeriesCollection = new XYSeriesCollection()
  sentimentsDataset.addSeries(sentimentsSeries)
  val relWordsDataset: XYSeriesCollection = new XYSeriesCollection()
  relWordsDataset.addSeries(relWordsSeries)

  //create renderers
  val relWordsDot: XYDotRenderer = new XYDotRenderer()
  relWordsDot.setDotHeight(5)
  relWordsDot.setDotWidth(5)
  relWordsDot.setSeriesShape(0, ShapeUtilities.createDiagonalCross(3, 1))
  relWordsDot.setSeriesPaint(0, Color.BLUE)

  val sentimentsDot: XYDotRenderer = new XYDotRenderer()
  sentimentsDot.setDotHeight(5)
  sentimentsDot.setDotWidth(5)

  //create xy axis
  val xax: NumberAxis = new NumberAxis(xlabel)
  val y1ax: NumberAxis = new NumberAxis("Sentiment Werte")
  val y2ax: NumberAxis = new NumberAxis("Relative Haeufigfkeit")

  //create plots
  val plot1: XYPlot = new XYPlot(sentimentsDataset, xax, y1ax, sentimentsDot)
  val plot2: XYPlot = new XYPlot(relWordsDataset, xax, y2ax, relWordsDot)

  val chart1: JFreeChart = new JFreeChart(plot1)
  val chart2: JFreeChart = new JFreeChart(plot2)
  val frame: ApplicationFrame = new ApplicationFrame(title)
  frame.setLayout(new GridLayout(2,1))

  val chartPanel1: ChartPanel = new ChartPanel(chart1)
  val chartPanel2: ChartPanel = new ChartPanel(chart2)

  frame.add(chartPanel1)
  frame.add(chartPanel2)
  frame.pack()
  frame.setVisible(true)

  println("Please press enter....")
  System.in.read()
  frame.setVisible(false)
  frame.dispose
}
}
