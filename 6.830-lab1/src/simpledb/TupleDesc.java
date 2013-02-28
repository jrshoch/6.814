package simpledb;

import java.lang.reflect.Array;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.collect.Maps;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

  private final int size;
  private final String stringFormat;
  private final Type[] typeArray;
  private final String[] fieldArray;
  private final Map<String, Integer> nameToIdMap;

  /**
   * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
   * with the first td1.numFields coming from td1 and the remaining from td2.
   * 
   * @param td1 The TupleDesc with the first fields of the new TupleDesc
   * @param td2 The TupleDesc with the last fields of the TupleDesc
   * @return the new TupleDesc
   */
  public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
    return new TupleDesc(concatenate(Type.class, td1.typeArray, td2.typeArray), concatenate(
        String.class, td1.fieldArray, td2.fieldArray));
  }

  @SuppressWarnings("unchecked")
  private static <T> T[] concatenate(Class<T> clazz, T[] array1, T[] array2) {
    int length1 = array1.length;
    int length2 = array2.length;
    T[] newArray = (T[]) Array.newInstance(clazz, length1 + length2);
    System.arraycopy(array1, 0, newArray, 0, length1);
    System.arraycopy(array2, 0, newArray, length1, length2);
    return newArray;
  }

  /**
   * Create a new TupleDesc with typeAr.length fields with fields of the
   * specified types, with associated named fields.
   * 
   * @param typeAr array specifying the number of and types of fields in this
   *          TupleDesc. It must contain at least one entry.
   * @param fieldAr array specifying the names of the fields. Note that names
   *          may be null.
   */
  public TupleDesc(Type[] typeAr, String[] fieldAr) {
    this.typeArray = typeAr;
    this.fieldArray = fieldAr;
    this.nameToIdMap = Maps.newHashMap();
    int sizeCalculation = 0;
    for (Type type : typeArray) {
      sizeCalculation += type.getLen();
    }
    this.size = sizeCalculation;
    this.stringFormat = getStringFormat(typeAr, fieldAr);
  }

  /**
   * Constructor. Create a new tuple desc with typeAr.length fields with fields
   * of the specified types, with anonymous (unnamed) fields.
   * 
   * @param typeAr array specifying the number of and types of fields in this
   *          TupleDesc. It must contain at least one entry.
   */
  public TupleDesc(Type[] typeAr) {
    this(typeAr, new String[typeAr.length]);
  }

  /**
   * @return the number of fields in this TupleDesc
   */
  public int numFields() {
    return typeArray.length;
  }

  /**
   * Gets the (possibly null) field name of the ith field of this TupleDesc.
   * 
   * @param i index of the field name to return. It must be a valid index.
   * @return the name of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public String getFieldName(int i) throws NoSuchElementException {
    if (i >= 0 && i < numFields()) {
      return fieldArray[i];
    }
    throw new NoSuchElementException();
  }

  /**
   * Find the index of the field with a given name.
   * 
   * @param name name of the field.
   * @return the index of the field that is first to have the given name.
   * @throws NoSuchElementException if no field with a matching name is found.
   */
  public int nameToId(String name) throws NoSuchElementException {
    if (nameToIdMap.containsKey(name)) {
      return nameToIdMap.get(name).intValue();
    }
    for (int i = 0; i < numFields(); i++) {
      if (fieldArray[i] == name) {
        nameToIdMap.put(name, new Integer(i));
        return i;
      }
    }
    throw new NoSuchElementException();
  }

  /**
   * Gets the type of the ith field of this TupleDesc.
   * 
   * @param i The index of the field to get the type of. It must be a valid
   *          index.
   * @return the type of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public Type getType(int i) throws NoSuchElementException {
    if (i >= 0 && i < numFields()) {
      return typeArray[i];
    }
    throw new NoSuchElementException();
  }

  /**
   * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note
   *         that tuples from a given TupleDesc are of a fixed size.
   */
  public int getSize() {
    return size;
  }

  /**
   * Compares the specified object with this TupleDesc for equality. Two
   * TupleDescs are considered equal if they are the same size and if the n-th
   * type in this TupleDesc is equal to the n-th type in td.
   * 
   * @param o the Object to be compared for equality with this TupleDesc.
   * @return true if the object is equal to this TupleDesc.
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TupleDesc)) {
      return false;
    }
    TupleDesc otherTuple = (TupleDesc) o;
    if (numFields() != otherTuple.numFields()) {
      return false;
    }
    for (int i = 0; i < numFields(); i++) {
      if (getType(i) != otherTuple.getType(i)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    // If you want to use TupleDesc as keys for HashMap, implement this so
    // that equal objects have equals hashCode() results
    throw new UnsupportedOperationException("unimplemented");
  }

  /**
   * Returns a String describing this descriptor. It should be of the form
   * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the
   * exact format does not matter.
   * 
   * @return String describing this descriptor.
   */
  @Override
  public String toString() {
    return stringFormat;
  }

  private static String getStringFormat(Type[] typeArray, String[] fieldArray) {
    String result = "";
    for (int i = 0; i < typeArray.length; i++) {
      result += typeArray[i].toString() + "(" + fieldArray[i] + ")";
    }
    return result;
  }
}
