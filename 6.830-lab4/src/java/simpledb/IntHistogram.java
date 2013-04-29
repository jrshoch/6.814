package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * A class to represent a fixed-width histogram over a single Long-based
 * field.
 */
public class IntHistogram {

  private final int min;
  private final int numBuckets;
  private final double bucketWidth;
  private int numberOfValues;
  private final Map<Long, Long> bucketSizes;

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
    this.numBuckets = buckets;
    this.bucketWidth = (max - min + 0.0) / numBuckets;
    this.numberOfValues = 0;
    bucketSizes = new HashMap<Long, Long>();
    for (int i = 0; i < numBuckets; i++) {
      bucketSizes.put(Long.valueOf(i), Long.valueOf(0));
    }
  }

  /**
   * Add a value to the set of values that you are keeping a histogram of.
   * 
   * @param v Value to add to the histogram
   */
  public void addValue(int v) {
    increment(getBucketIndex(v));
    numberOfValues++;
  }

  private void increment(Long bucketIndex) {
    bucketSizes.put(bucketIndex, Long.valueOf(bucketSizes.get(bucketIndex).longValue() + 1));
  }

  private Long getBucketIndex(int v) {
    return Long.valueOf(Math.round((v - min) / bucketWidth));
  }

  private long getBucketMin(int v) {
    return ((v - min) / bucketWidth) * bucketWidth + min;
  }

  private long getApproximateNumberOfLesserValuesInBucket(int v) {
    return (bucketSizes.get(getBucketIndex(v)).longValue() * v - getBucketMin(v)) / bucketWidth;
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
    Long bucketIndex = getBucketIndex(v);
    int equalValuesEstimate = bucketSizes.get(bucketIndex).longValue() / bucketWidth;
    int sufficientValuesEstimate = 0;
    switch (op) {
    case GREATER_THAN_OR_EQ:
    case LESS_THAN_OR_EQ:
    case EQUALS:
      sufficientValuesEstimate += equalValuesEstimate;
      break;
    case GREATER_THAN:
      sufficientValuesEstimate += bucketSizes.get(bucketIndex).longValue() - getApproximateNumberOfLesserValuesInBucket(v);
      for (int i = bucketIndex.longValue() + 1; i < numBuckets; i++) {
        sufficientValuesEstimate += bucketSizes.get(Long.valueOf(i)).longValue();
      }
      break;
    case LESS_THAN:
      sufficientValuesEstimate += getApproximateNumberOfLesserValuesInBucket(v);
      for (int i = 0; i < bucketIndex.longValue(); i++) {
        sufficientValuesEstimate += bucketSizes.get(Long.valueOf(i)).longValue();
      }
      break;
    case NOT_EQUALS:
      sufficientValuesEstimate += numberOfValues - equalValuesEstimate;
      break;
    case LIKE:
      return 1.0;
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
