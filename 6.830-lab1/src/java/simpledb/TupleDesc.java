package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

  /**
   * A helper class to facilitate organizing the information of each field
   * */
  public static class TDItem implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The type of the field
     * */
    public final Type fieldType;

    /**
     * The name of the field
     * */
    public final String fieldName;

    public TDItem(Type t, String n) {
      this.fieldName = n;
      this.fieldType = t;
    }

    @Override
    public String toString() {
      return fieldName + "(" + fieldType + ")";
    }
    
    public static List<TDItem> getListFrom(Type[] typeAr, String[] fieldAr) {
      List<TDItem> list = new ArrayList<TDItem>();
      for (int i = 0; i < typeAr.length; i++) {
        list.add(new TDItem(typeAr[i], fieldAr[i]));
      }
      return list;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
      result = prime * result + ((fieldType == null) ? 0 : fieldType.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TDItem other = (TDItem) obj;
      if (fieldName == null) {
        if (other.fieldName != null)
          return false;
      } else if (!fieldName.equals(other.fieldName))
        return false;
      if (fieldType != other.fieldType)
        return false;
      return true;
    }
  }

  /**
   * @return An iterator which iterates over all the field TDItems that are
   *         included in this TupleDesc
   * */
  public Iterator<TDItem> iterator() {
    // some code goes here
    return null;
  }

  private static final long serialVersionUID = 1L;
  
  private final List<TDItem> tdItems;
  private final Map<String, Integer> nameToIdMap;
  private final int size;
  private final String stringFormat;

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
    this(TDItem.getListFrom(typeAr, fieldAr));
  }
  
  private TupleDesc(List<TDItem> tdItems) {
    this.tdItems = new ArrayList<TDItem>(tdItems);
    this.nameToIdMap = new HashMap<String, Integer>();
    int sizeCalculation = 0;
    for (TDItem tdItem : tdItems) {
      sizeCalculation += tdItem.fieldType.getLen();
    }
    this.size = sizeCalculation;
    this.stringFormat = getStringFormat(tdItems);
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
    return tdItems.size();
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
      return tdItems.get(i).fieldName;
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
  public Type getFieldType(int i) throws NoSuchElementException {
    if (i >= 0 && i < numFields()) {
      return tdItems.get(i).fieldType;
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
  public int fieldNameToIndex(String name) throws NoSuchElementException {
    if (nameToIdMap.containsKey(name)) {
      return nameToIdMap.get(name).intValue();
    }
    for (int i = 0; i < numFields(); i++) {
      String field = getFieldName(i);
      if (field != null && field.equals(name)) {
        nameToIdMap.put(name, new Integer(i));
        return i;
      }
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
   * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
   * with the first td1.numFields coming from td1 and the remaining from td2.
   * 
   * @param td1 The TupleDesc with the first fields of the new TupleDesc
   * @param td2 The TupleDesc with the last fields of the TupleDesc
   * @return the new TupleDesc
   */
  public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    List<TDItem> newTdItems = new ArrayList<TDItem>(td1.tdItems);
    newTdItems.addAll(td2.tdItems);
    return new TupleDesc(newTdItems);
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
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TupleDesc other = (TupleDesc) obj;
    if (tdItems == null) {
      if (other.tdItems != null)
        return false;
    } else if (!tdItems.equals(other.tdItems))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((tdItems == null) ? 0 : tdItems.hashCode());
    return result;
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
  
  private static String getStringFormat(List<TDItem> tdItems) {
    String result = "";
    for (TDItem tdItem : tdItems) {
      result += tdItem.toString();
    }
    return result;
  }
}
