object DummyCoder {
  // This function dummycodes all String columns in a given dataset
  // and returns an array of transformations to be plugged into a Pipeline
  def encodeTransf(df:Dataset[_]) : Array[OneHotEncoder] = {
      var encoded = df
      
      // List of transformations
      val transf = ListBuffer[OneHotEncoder]()
      
      // Get the list of String columns
      val cols = getStringCols(df)
      for (col <- cols) {
        val indCol = col + "_index"
        val dummyCol = col + "_dummy_"
        
        // Before dummy-coding, recoding needs to be done
        val indexer = new StringIndexer()
          .setInputCol(col)
          .setOutputCol(indCol)
          .fit(encoded)
        val indexed = indexer.transform(encoded)
        
        // Dummy-coding
        val encoder = new OneHotEncoder()
          .setInputCol(indCol)
          .setOutputCol(dummyCol)
        encoded = encoder.transform(indexed)
        
        // Remove indexed column
        encoded = encoded.drop(encoded.col(indCol))
        
        // Remove original column
        encoded = encoded.drop(encoded.col(col))
        
        // Append transformation  
        transf += encoder
      }
      return(transf.toArray)
  }
    
  def encodeDataset(df:Dataset[_]) : Dataset[_] = {
      var encoded = df
      
      // Get the list of String columns
      val cols = getStringCols(df)
      for (col <- cols) {
        val indCol = col + "_index"
        val dummyCol = col + "_dummy_"
        
        // Before dummy-coding, recoding needs to be done
        val indexer = new StringIndexer()
          .setInputCol(col)
          .setOutputCol(indCol)
          .fit(encoded)
        val indexed = indexer.transform(encoded)
        
        // Dummy-coding
        val encoder = new OneHotEncoder()
          .setInputCol(indCol)
          .setOutputCol(dummyCol)
        encoded = encoder.transform(indexed)
        
        // Remove indexed column
        encoded = encoded.drop(encoded.col(indCol))
        
        // Remove original column
        encoded = encoded.drop(encoded.col(col))
      }
      return(encoded)
  }
  
  // This function returns an Array with the names of the String columns in a Dataset
  def getStringCols(df:Dataset[_]) : Array[String] = {
      val stringCols = ListBuffer[String]()
      for (x <- df.schema.toArray) {
          if (x.dataType.toString == "StringType") {
              stringCols += x.name
          }
      }
      stringCols.toArray
  }
}
