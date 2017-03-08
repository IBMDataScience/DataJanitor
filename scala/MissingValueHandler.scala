/** This class provides a set of functions for handling missing values in 
  * Spark DataFrames
*/
object MissingValueHandler {
  
  /** This function computes the names of the columns which have more than
   *  a certain ratio of null values. This is given by parameter threshold 
  */
  def getIncompleteColumns(df:Dataset[_], threshold:Double) : Array[String] = {
    val ratios = MissingValueHandler.getIncompleteRatios(df)
    val indices = Array(ratios.zipWithIndex.filter(_._1 > threshold).map(_._2))
    return indices(0).map(df.columns)
  }

  /** This function computes the ratios of null values on each column of a 
   * Spark DataFrame
   */
  def getIncompleteRatios(df:Dataset[_]) : Array[Double] = {
    val isNullDF = df.select(df.columns.map(c => org.apache.spark.sql.functions.
        when(org.apache.spark.sql.functions.col(c).isNull, 1).otherwise(0).    
        alias(c)): _*)
    
    val exprs = isNullDF.columns.map((_ -> "mean")).toMap
    val ratios = isNullDF.agg(exprs).collect()(0)
    ratios.toSeq.toArray.map(_.asInstanceOf[{ def toDouble: Double }].toDouble)
    
  } 
}
