package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

  private static final int MAX_PAGES_PER_TABLE = 1048573; // Prime near 2 ** 20

  private final int tableId;
  private final int pageNumber;

  /**
   * Constructor. Create a page id structure for a specific page of a specific
   * table.
   * 
   * @param tableId The table that is being referenced
   * @param pgNo The page number in that table.
   */
  public HeapPageId(int tableId, int pgNo) {
    this.tableId = tableId;
    this.pageNumber = pgNo;
  }

  /** @return the table associated with this PageId */
  @Override
  public int getTableId() {
    return tableId;
  }

  /**
   * @return the page number in the table getTableId() associated with this
   *         PageId
   */
  @Override
  public int pageNumber() {
    return pageNumber;
  }

  /**
   * @return a hash code for this page, represented by the concatenation of the
   *         table number and the page number (needed if a PageId is used as a
   *         key in a hash table in the BufferPool, for example.)
   * @see BufferPool
   */
  @Override
  public int hashCode() {
    return tableId * MAX_PAGES_PER_TABLE + pageNumber + 31;
  }

  /**
   * Compares one PageId to another.
   * 
   * @param o The object to compare against (must be a PageId)
   * @return true if the objects are equal (e.g., page numbers and table ids are
   *         the same)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null)
      return false;
    if (getClass() != o.getClass())
      return false;
    HeapPageId otherHeapPageId = (HeapPageId) o;
    if (otherHeapPageId.pageNumber != this.pageNumber)
      return false;
    if (otherHeapPageId.tableId != this.tableId)
      return false;
    return true;
  }

  /**
   * Return a representation of this object as an array of integers, for writing
   * to disk. Size of returned array must contain number of integers that
   * corresponds to number of args to one of the constructors.
   */
  @Override
  public int[] serialize() {
    int data[] = new int[2];

    data[0] = getTableId();
    data[1] = pageNumber();

    return data;
  }

}
