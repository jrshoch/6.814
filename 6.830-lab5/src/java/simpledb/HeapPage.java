package simpledb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 * 
 * @see HeapFile
 * @see BufferPool
 * 
 */
public class HeapPage implements Page {

  private static final int BYTE_SIZE = 8;

  private final PageId heapPageId;
  private final TupleDesc tupleDesc;
  private final byte header[];
  private final Tuple tuples[];
  private final int numberOfTupleSlots;

  private byte[] oldData;

  private final Byte oldDataLock = new Byte((byte) 0);
  
  private boolean isDirty;
  private TransactionId dirtyingTransactionId;

  /**
   * Create a HeapPage from a set of bytes of data read from disk. The format of
   * a HeapPage is a set of header bytes indicating the slots of the page that
   * are in use, some number of tuple slots. Specifically, the number of tuples
   * is equal to:
   * <p>
   * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
   * <p>
   * where tuple size is the size of tuples in this database table, which can be
   * determined via {@link Catalog#getTupleDesc}. The number of 8-bit header
   * words is equal to:
   * <p>
   * ceiling(no. tuple slots / 8)
   * <p>
   * 
   * @see Database#getCatalog
   * @see Catalog#getTupleDesc
   * @see BufferPool#getPageSize()
   */
  public HeapPage(PageId id, byte[] data) throws IOException {
    this.heapPageId = id;
    this.tupleDesc = Database.getCatalog().getTupleDesc(id.getTableId());
    this.numberOfTupleSlots = getNumTuples(this.tupleDesc);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

    // allocate and read the header slots of this page
    header = new byte[getHeaderSize(this.numberOfTupleSlots)];
    for (int i = 0; i < header.length; i++)
      header[i] = dis.readByte();

    // allocate and read the actual records of this page
    tuples = new Tuple[numberOfTupleSlots];
    try {
      for (int i = 0; i < tuples.length; i++)
        tuples[i] = readNextTuple(dis, i);
    } catch (NoSuchElementException e) {
      e.printStackTrace();
    }
    dis.close();

    this.isDirty = false;
    this.dirtyingTransactionId = null;
    
    setBeforeImage();
  }

  /**
   * Retrieve the number of tuples on this page.
   * 
   * @return the number of tuples on this page
   */
  private static int getNumTuples(TupleDesc tupleDesc) {
    return (int) Math.floor((BufferPool.getPageSize() * 8)
        / ((double) (tupleDesc.getSize() * 8 + 1)));
  }

  /**
   * Computes the number of bytes in the header of a page in a HeapFile with
   * each tuple occupying tupleSize bytes
   * 
   * @return the number of bytes in the header of a page in a HeapFile with each
   *         tuple occupying tupleSize bytes
   */
  private static int getHeaderSize(int numberOfTupleSlots) {
    return (int) Math.ceil(numberOfTupleSlots / ((double) 8));
  }

  /**
   * Return a view of this page before it was modified -- used by recovery
   */
  @Override
  public HeapPage getBeforeImage() {
    try {
      byte[] oldDataRef = null;
      synchronized (oldDataLock) {
        oldDataRef = oldData;
      }
      return new HeapPage(heapPageId, oldDataRef);
    } catch (IOException e) {
      e.printStackTrace();
      // should never happen -- we parsed it OK before!
      System.exit(1);
    }
    return null;
  }

  @Override
  public void setBeforeImage() {
    synchronized (oldDataLock) {
      oldData = getPageData().clone();
    }
  }

  /**
   * @return the PageId associated with this page.
   */
  @Override
  public PageId getId() {
    return heapPageId;
  }

  /**
   * Suck up tuples from the source file.
   */
  private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
    // if associated bit is not set, read forward to the next tuple, and
    // return null.
    if (!isSlotUsed(slotId)) {
      for (int i = 0; i < tupleDesc.getSize(); i++) {
        try {
          dis.readByte();
        } catch (IOException e) {
          throw new NoSuchElementException("error reading empty tuple");
        }
      }
      return null;
    }

    // read fields in the tuple
    Tuple t = new Tuple(tupleDesc);
    RecordId rid = new RecordId(heapPageId, slotId);
    t.setRecordId(rid);
    try {
      for (int j = 0; j < tupleDesc.numFields(); j++) {
        Field f = tupleDesc.getFieldType(j).parse(dis);
        t.setField(j, f);
      }
    } catch (java.text.ParseException e) {
      e.printStackTrace();
      throw new NoSuchElementException("parsing error!");
    }

    return t;
  }

  /**
   * Generates a byte array representing the contents of this page. Used to
   * serialize this page to disk.
   * <p>
   * The invariant here is that it should be possible to pass the byte array
   * generated by getPageData to the HeapPage constructor and have it produce an
   * identical HeapPage object.
   * 
   * @see #HeapPage
   * @return A byte array correspond to the bytes of this page.
   */
  @Override
  public byte[] getPageData() {
    int len = BufferPool.getPageSize();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
    DataOutputStream dos = new DataOutputStream(baos);

    // create the header of the page
    for (int i = 0; i < header.length; i++) {
      try {
        dos.writeByte(header[i]);
      } catch (IOException e) {
        // this really shouldn't happen
        e.printStackTrace();
      }
    }

    // create the tuples
    for (int i = 0; i < tuples.length; i++) {

      // empty slot
      if (!isSlotUsed(i)) {
        for (int j = 0; j < tupleDesc.getSize(); j++) {
          try {
            dos.writeByte(0);
          } catch (IOException e) {
            e.printStackTrace();
          }

        }
        continue;
      }

      // non-empty slot
      for (int j = 0; j < tupleDesc.numFields(); j++) {
        Field f = tuples[i].getField(j);
        try {
          f.serialize(dos);

        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    // padding
    int zerolen = BufferPool.getPageSize() - (header.length + tupleDesc.getSize() * tuples.length); // -
    // numSlots
    // *
    // td.getSize();
    byte[] zeroes = new byte[zerolen];
    try {
      dos.write(zeroes, 0, zerolen);
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      dos.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return baos.toByteArray();
  }

  /**
   * Static method to generate a byte array corresponding to an empty HeapPage.
   * Used to add new, empty pages to the file. Passing the results of this
   * method to the HeapPage constructor will create a HeapPage with no valid
   * tuples in it.
   * 
   * @return The returned ByteArray.
   */
  public static byte[] createEmptyPageData() {
    int len = BufferPool.getPageSize();
    return new byte[len]; // all 0
  }

  /**
   * Delete the specified tuple from the page; the tuple should be updated to
   * reflect that it is no longer stored on any page.
   * 
   * @throws DbException if this tuple is not on this page, or tuple slot is
   *           already empty.
   * @param t The tuple to delete
   */
  public void deleteTuple(Tuple t) throws DbException {
    if (t.getRecordId() == null || !(t.getRecordId().getPageId().equals(heapPageId))) {
      throw new DbException("Tuple can only be deleted from its page.");
    }
    int tupleNumber = t.getRecordId().tupleno();
    if (!isSlotUsed(tupleNumber)) {
      throw new DbException("Tuple's slot is already empty.");
    }
    markSlotUsed(tupleNumber, false);
    t.setRecordId(null);
    tuples[tupleNumber] = null;
  }

  /**
   * Adds the specified tuple to the page; the tuple should be updated to
   * reflect that it is now stored on this page.
   * 
   * @throws DbException if the page is full (no empty slots) or tupledesc is
   *           mismatch.
   * @param t The tuple to add.
   */
  public void insertTuple(Tuple t) throws DbException {
    if (!(t.getTupleDesc().equals(tupleDesc))) {
      throw new DbException("Cannot insert a Tuple with a different TupleDesc.");
    }
    int emptySlotIndex = getFirstEmptyTupleIndex();
    if (emptySlotIndex == numberOfTupleSlots) {
      throw new DbException("No empty slots available for Tuple insertion.");
    }
    t.setRecordId(new RecordId(heapPageId, emptySlotIndex));
    markSlotUsed(emptySlotIndex, true);
    tuples[emptySlotIndex] = t;
  }

  /**
   * Marks this page as dirty/not dirty and record that transaction that did the
   * dirtying
   */
  @Override
  public void markDirty(boolean dirty, TransactionId tid) {
    this.isDirty = dirty;
    this.dirtyingTransactionId = isDirty ? tid : null;
  }

  /**
   * Returns the tid of the transaction that last dirtied this page, or null if
   * the page is not dirty
   */
  @Override
  public TransactionId isDirty() {
    return isDirty ? dirtyingTransactionId : null;
  }

  /**
   * Abstraction to fill or clear a slot on this page.
   */
  private void markSlotUsed(int i, boolean value) {
    setSlot(i, value);
  }

  /**
   * Returns the number of empty slots on this page.
   */
  public int getNumEmptySlots() {
    int count = 0;
    for (int i = 0; i < header.length; i++) {
      int bytePresentCount = 0;
      int headerByte = header[i];
      if (headerByte < 0) {
        bytePresentCount++;
        headerByte += 1 << (BYTE_SIZE - 1);
      }
      for (; headerByte != 0; headerByte = headerByte >> 1) {
        if (headerByte % 2 == 1)
          bytePresentCount++;
      }
      count += (8 - bytePresentCount);
    }
    return count;
  }

  /**
   * Returns true if associated slot on this page is filled.
   */
  public boolean isSlotUsed(int i) {
    int headerByte = i / 8;
    int byteBit = i % 8;
    return (header[headerByte] & (1 << byteBit)) != 0;
  }

  /**
   * Abstraction to fill or clear a slot on this page.
   */
  private void setSlot(int i, boolean value) {
    int headerByte = i / 8;
    int byteBit = i % 8;
    if (value != isSlotUsed(i)) {
      header[headerByte] ^= (1 << byteBit);
    }
  }

  protected int getFirstUsedTupleIndex(int maxIndex) {
    return getNextUsedTupleIndex(0, maxIndex);
  }
  
  protected int getFirstEmptyTupleIndex() {
    return getNextTupleIndexOfValue(0, numberOfTupleSlots, false);
  }

  protected int getNextUsedTupleIndex(int startIndex, int maxIndex) {
    return getNextTupleIndexOfValue(startIndex, maxIndex, true);
  }
  
  protected int getNextTupleIndexOfValue(int startIndex, int maxIndex, boolean value) {
    int nextTupleIndex = startIndex;
    while (nextTupleIndex < maxIndex && (isSlotUsed(nextTupleIndex) != value)) {
      nextTupleIndex++;
    }
    return nextTupleIndex;
  }
  
  protected Tuple[] getTuples() {
    return tuples;
  }

  /**
   * @return an iterator over all tuples on this page (calling remove on this
   *         iterator throws an UnsupportedOperationException) (note that this
   *         iterator shouldn't return tuples in empty slots!)
   */
  public Iterator<Tuple> iterator() {
    return new Iterator<Tuple>() {

      int maxIndex = getTuples().length;
      int currentIndex = getFirstUsedTupleIndex(maxIndex);

      @Override
      public boolean hasNext() {
        return currentIndex < maxIndex;
      }

      @Override
      public Tuple next() {
        Tuple tuple = getTuples()[currentIndex];
        currentIndex = getNextUsedTupleIndex(currentIndex + 1, maxIndex);
        return tuple;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

    };
  }

}
