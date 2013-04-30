package simpledb;


/**
 * A class to represent a fixed-width histogram over a single Long-based field.
 */
public class IntHistogram {

  private final int min;
  private final int max;
  private final int numBuckets;
  private final double bucketWidth;
  private int numberOfValues;
  private final long[] bucketSizes;

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
    this.max = max;
    this.numBuckets = buckets;
    this.bucketWidth = (max - min + 0.0) / numBuckets;
    this.numberOfValues = 0;
    bucketSizes = new long[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      bucketSizes[i] = 0;
    }
  }

  /**
   * Add a value to the set of values that you are keeping a histogram of.
   * 
   * @param v Value to add to the histogram
   */
  public void addValue(int v) {
//    System.out.println("min: " + min + ", max: " + (min + numBuckets * bucketWidth) + ", v: " + v);
    increment(getBucketIndex(v));
    numberOfValues++;
  }

  private void increment(int bucketIndex) {
    bucketSizes[bucketIndex] = bucketSizes[bucketIndex] + 1;
  }

  private int getBucketIndex(int v) {
    return Math.min((int) ((v - min) / bucketWidth), numBuckets - 1);
  }

  private long getBucketMin(int v) {
    return ((long) (getBucketIndex(v) * bucketWidth + min)) + 1;
  }

  private long getApproximateNumberOfLesserValuesInBucket(int v) {
    return ((long) ((bucketSizes[getBucketIndex(v)] * (v - getBucketMin(v))) / bucketWidth));
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
    if (v <= min) {
      switch (op) {
      case GREATER_THAN:
      case GREATER_THAN_OR_EQ:
      case NOT_EQUALS:
        return 1.0;
      default:
        return 0.0;
      }
    }
    if (v >= max) {
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
    long equalValuesEstimate = ((long) (bucketSizes[bucketIndex] / bucketWidth));
    long sufficientValuesEstimate = 0;
    switch (op) {
    case GREATER_THAN_OR_EQ:
    case LESS_THAN_OR_EQ:
    case EQUALS:
      sufficientValuesEstimate += equalValuesEstimate;
      break;
    default:
      break;
    }
    switch (op) {
    case GREATER_THAN_OR_EQ:
    case GREATER_THAN:
      sufficientValuesEstimate += bucketSizes[bucketIndex]
          - getApproximateNumberOfLesserValuesInBucket(v);
      for (int i = bucketIndex + 1; i < numBuckets; i++) {
        sufficientValuesEstimate += bucketSizes[i];
      }
      break;
    case LESS_THAN_OR_EQ:
    case LESS_THAN:
      sufficientValuesEstimate += getApproximateNumberOfLesserValuesInBucket(v);
      for (int i = 0; i < bucketIndex; i++) {
        sufficientValuesEstimate += bucketSizes[i];
      }
      break;
    case NOT_EQUALS:
      sufficientValuesEstimate += numberOfValues - equalValuesEstimate;
      break;
    case LIKE:
      return 1.0;
    default:
      break;
    }
    return (sufficientValuesEstimate * 1.0) / numberOfValues;
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
