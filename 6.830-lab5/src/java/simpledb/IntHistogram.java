package simpledb;


/**
 * A class to represent a fixed-width histogram over a single Long-based field.
 */
public class IntHistogram {
  
  private static final double EPSILON = 0.00001;

  private final int min;
  private final int max;
  private final int numBuckets;
  private final double averageBucketWidth;
  private int totalCount;
  private final long[] bucketCounts;
  private final long[] bucketMins;
  private final long[] bucketWidths;

  /**
   * Create a new IntHistogram.
   * 
   * This IntHistogram should maintain a histogram of Long values that it
   * receives. It should split the histogram into "buckets" buckets.
   * 
   * The values that are being histogrammed will be provided one-at-a-time
   * through the "addValue()" function.
   * 
   * Your implementation should use space and have execution time that are both
   * constant with respect to the number of values being histogrammed. For
   * example, you shouldn't simply store every value that you see in a sorted
   * list.
   * 
   * @param buckets The number of buckets to split the input value into.
   * @param min The minimum Long value that will ever be passed to this class
   *          for histogramming
   * @param max The maximum Long value that will ever be passed to this class
   *          for histogramming
   */
  public IntHistogram(int buckets, int min, int max) {
    this.min = min;
    this.max = max + 1;
    this.numBuckets = buckets;
    this.totalCount = 0;
    averageBucketWidth = (this.max - this.min + 0.0) / numBuckets;
    bucketCounts = new long[numBuckets];
    bucketMins = new long[numBuckets];
    bucketWidths = new long[numBuckets];
    bucketCounts[0] = 0;
    bucketMins[0] = min;
    bucketWidths[0] = 1;
    for (int i = 1; i < numBuckets; i++) {
      bucketCounts[i] = 0;
      bucketMins[i] = ((int) (min + averageBucketWidth * (i + 1 - EPSILON)));
      if (bucketMins[i] > bucketMins[i - 1]) {
        bucketWidths[i] = 1;
        bucketWidths[i - 1] += bucketMins[i] - bucketMins[i - 1] - 1;
      } else {
        bucketWidths[i] = 0;
      }
    }
  }

  /**
   * Add a value to the set of values that you are keeping a histogram of.
   * 
   * @param v Value to add to the histogram
   */
  public void addValue(int v) {
    increment(getBucketIndex(v));
    totalCount++;
  }

  private void increment(int bucketIndex) {
    bucketCounts[bucketIndex] = bucketCounts[bucketIndex] + 1;
  }

  private int getBucketIndex(int v) {
    return Math.min((int) ((v - min) / averageBucketWidth), numBuckets - 1);
  }

  private long getApproximateNumberOfLesserValuesInBucket(int v) {
    int bucketIndex = getBucketIndex(v);
    long bucketWidth = bucketWidths[bucketIndex];
    if (bucketWidth == 0) {
      return 0;
    }
    long bucketMin = bucketMins[bucketIndex];
    return (bucketCounts[bucketIndex] * (v - bucketMin)) / (bucketWidth);
  }

  /**
   * Estimate the selectivity of a particular predicate and operand on this
   * table.
   * 
   * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate
   * of the fraction of elements that are greater than 5.
   * 
   * @param op Operator
   * @param v Value
   * @return Predicted selectivity of this particular operator and value
   */
  public double estimateSelectivity(Predicate.Op op, int v) {
    if (v < min) {
      switch (op) {
      case GREATER_THAN:
      case GREATER_THAN_OR_EQ:
      case NOT_EQUALS:
        return 1.0;
      default:
        return 0.0;
      }
    }
    if (v > max) {
      switch (op) {
      case LESS_THAN:
      case LESS_THAN_OR_EQ:
      case NOT_EQUALS:
        return 1.0;
      default:
        return 0.0;
      }
    }
    int bucketIndex = getBucketIndex(v);
    long bucketWidth = bucketWidths[bucketIndex];
    long equalValuesEstimate = (bucketWidth == 0) ? 0 : bucketCounts[bucketIndex] / bucketWidth;
    long sufficientValuesEstimate = 0;
    switch (op) {
    case LESS_THAN_OR_EQ:
    case EQUALS:
      sufficientValuesEstimate += equalValuesEstimate;
      break;
    case GREATER_THAN:
      sufficientValuesEstimate -= equalValuesEstimate;
      break;
    default:
      break;
    }
    switch (op) {
    case GREATER_THAN_OR_EQ:
    case GREATER_THAN:
      sufficientValuesEstimate += bucketCounts[bucketIndex]
          - getApproximateNumberOfLesserValuesInBucket(v);
      for (int i = bucketIndex + 1; i < numBuckets; i++) {
        sufficientValuesEstimate += bucketCounts[i];
      }
      break;
    case LESS_THAN_OR_EQ:
    case LESS_THAN:
      sufficientValuesEstimate += getApproximateNumberOfLesserValuesInBucket(v);
      for (int i = 0; i < bucketIndex; i++) {
        sufficientValuesEstimate += bucketCounts[i];
      }
      break;
    case NOT_EQUALS:
      sufficientValuesEstimate += totalCount - equalValuesEstimate;
      break;
    case LIKE:
      return 1.0;
    default:
      break;
    }
    return (sufficientValuesEstimate * 1.0) / totalCount;
  }

  /**
   * @return the average selectivity of this histogram.
   * 
   *         This is not an indispensable method to implement the basic join
   *         optimization. It may be needed if you want to implement a more
   *         efficient optimization
   * */
  public double avgSelectivity() {
    // some code goes here
    return 1.0;
  }

  /**
   * @return A string describing this histogram, for debugging purposes
   */
  @Override
  public String toString() {
    // some code goes here
    return null;
  }
}
