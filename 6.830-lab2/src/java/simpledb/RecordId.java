package simpledb;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {

  private final PageId pageId;
  private final int tupleNumber;

  /**
   * Creates a new RecordId referring to the specified PageId and tuple number.
   * 
   * @param pid the pageid of the page on which the tuple resides
   * @param tupleno the tuple number within the page.
   */
  public RecordId(PageId pid, int tupleno) {
    this.pageId = pid;
    this.tupleNumber = tupleno;
  }

  /**
   * @return the tuple number this RecordId references.
   */
  public int tupleno() {
    return this.tupleNumber;
  }

  /**
   * @return the page id this RecordId references.
   */
  public PageId getPageId() {
    return this.pageId;
  }

  /**
   * Two RecordId objects are considered equal if they represent the same tuple.
   * 
   * @return True if this and o represent the same tuple
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RecordId other = (RecordId) obj;
    if (pageId == null) {
      if (other.pageId != null)
        return false;
    } else if (!pageId.equals(other.pageId))
      return false;
    if (tupleNumber != other.tupleNumber)
      return false;
    return true;
  }

  /**
   * You should implement the hashCode() so that two equal RecordId instances
   * (with respect to equals()) have the same hashCode().
   * 
   * @return An int that is the same for equal RecordId objects.
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((pageId == null) ? 0 : pageId.hashCode());
    result = prime * result + tupleNumber;
    return result;
  }

}
