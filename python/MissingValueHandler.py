# This class provides a set of functions for handling missing values in 
# Spark DataFrames

# This function computes how many NULL values are in a certain Spark Column
def countNulls(c):
    return sum(col(c).isNull().cast("integer")).alias(c)

# This function computes the ratios of null values on each column of a 
# Spark DataFrame
def getIncompleteRatios(df):
    from pyspark.sql.functions import col, count, sum
    exprs = [countNulls(c) for c in df.columns]
    return (df.agg(*exprs).toPandas() / df.count()).transpose()

# This function computes the names of the columns which have more than
# a certain ratio of null values. The ratio is given by parameter threshold 
def getIncompleteColumns(df, threshold):
    r = getIncompleteRatios(df)
    return list(r[r[0] > threshold].transpose().columns)

# This function computes the names of the columns which have less than
# a certain ratio of null values. The ratio is given by parameter threshold 
def getCompleteColumns(df, threshold):
    r = getIncompleteRatios(df)
    return list(r[r[0] < threshold].transpose().columns)

# This function removes the columns which have more than
# a certain ratio of null values. The ratio is given by parameter threshold 
def removeIncompleteColumns(df, threshold):
    return df.select(getCompleteColumns(df, threshold))
