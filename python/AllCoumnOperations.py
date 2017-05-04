# Computes the mean of all columns in a Spark DataFrame
def allMeans(df):
    from pyspark.sql.functions import col, mean
    exprs = [mean(c) for c in df.columns]
    return (df.agg(*exprs).toPandas()).transpose()

# Computes the standard deviation of all columns in a Spark DataFrame
def allSD(df):
    from pyspark.sql.functions import col, mean
    exprs = [sd(c) for c in df.columns]
    return (df.agg(*exprs).toPandas()).transpose()

# Normalizes all columns in a Spark DataFrame using z-score
def zScore(df):
    mu = list(allMeans(fullFeatureSet)[0])
    sigma = list(allSDs(fullFeatureSet)[0])
    
    from pyspark.sql.functions import col
    exprs = [(df[c] - mu[index]) / sigma[index] for index, c in enumerate(df.columns)]
    return (df.select(*exprs))

# Applies the log function with and adds a constant to all columns of a Spark DataFrame
def logTransform(df, c):
    from pyspark.sql.functions import col, log
    exprs = [log(column) + c for index, column in enumerate(df.columns)]
    return (df.select(*exprs))
