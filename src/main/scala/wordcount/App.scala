package wordcount

/**
  * @author hendrik
  * modified by akarakochev
  */
object App {
  val sentiAnalyse = new Sentiments("AFINN-112.txt")

  /*
   * 
   *   Diese Anwendung fuehrt eine Sentiment-Analyse mit
   *   dem Buch "Robinson Crusoe" von Daniel Defoe (1660-1731) durch
   *
   *   Es werden zwei Graphen erstellt. Der eine beinhaltet die Stimmung
   *   in den ueber 10000 Woertern erzeugten Absaetzen.
   *   Der zweite Graph enthaelt die relative Haeufigkeit der erkannten Woerter,
   *   d.h. die Anzahl der Woerter, die für die Analyse der Stimmung herangezogen wurden.
   *
   *  Testen Sie die App mit verschiedenen Absatzgroessen
   *
   *
   *  Kommentieren sie die Dritte Zeile der Main Methode raus.
   *
   *  So werden wieder zwei Graphen erstellt.
   *  Der erste beinhaltet die Stimmung ueber die Kapitel.
   *  Wie davor enthaelt der zweite Graph die relative Haeufigkeit der erkannten Woerter,
   *  d.h. die Anzahl der Woerter, die für die Analyse der Stimmung herangezogen wurden.
   *  Besonders interessant sind Kapitel 7 (CHAPTER VII—AGRICULTURAL EXPERIENCE ),
   *  was eine eher positive Stimmung haben sollte und z.b
   *  6 (CHAPTER VI—ILL AND CONSCIENCE-STRICKEN) und
   *  12(CHAPTER XII—A CAVE RETREAT), die eher negativ sind.
   */
  def main(args: Array[String]) = {
    val fileName = "Robinson.txt"
    showAnalysisPerSegment(10000, fileName)
    showAnalysisPerChapter(fileName)
  }

  def showAnalysisPerSegment(n: Int, fileName: String): Unit = {
    val book = sentiAnalyse.getDocumentGroupedByCounts(fileName, n)
    val data = sentiAnalyse.analyseSentiments(book)
    sentiAnalyse.createGraph(data, title = s"Sentiment-Analyse: Abschnitte je $n Wörter")
  }

  def showAnalysisPerChapter(fileName: String): Unit = {
    val chapterRegex = "CHAPTER [MDCLXVI]+".r

    val book = sentiAnalyse.getDocumentSplitByPredicate(fileName,
      x => chapterRegex.findFirstIn(x).isDefined)
    val data = sentiAnalyse.analyseSentiments(book)
    sentiAnalyse.createGraph(data, "Kapitel", "Sentiment-Analyse: Kapitel")
  }
}