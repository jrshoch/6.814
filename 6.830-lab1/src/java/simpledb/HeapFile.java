package simpledb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

  private final File backingFile;
  private final TupleDesc tupleDesc;
  private final int numberOfPages;

  /**
   * Constructs a heap file backed by the specified file.
   * 
   * @param f the file that stores the on-disk backing store for this heap file.
   */
  public HeapFile(File f, TupleDesc td) {
    this.backingFile = f;
    this.tupleDesc = td;
    this.numberOfPages = (int) backingFile.length() / BufferPool.getPageSize();
  }

  /**
   * Returns the File backing this HeapFile on disk.
   * 
   * @return the File backing this HeapFile on disk.
   */
  public File getFile() {
    return backingFile;
  }

  /**
   * Returns an ID uniquely identifying this HeapFile. Implementation note: you
   * will need to generate this tableid somewhere ensure that each HeapFile has
   * a "unique id," and that you always return the same value for a particular
   * HeapFile. We suggest hashing the absolute file name of the file underlying
   * the heapfile, i.e. f.getAbsoluteFile().hashCode().
   * 
   * @return an ID uniquely identifying this HeapFile.
   */
  @Override
  public int getId() {
    return backingFile.getAbsoluteFile().hashCode();
  }

  /**
   * Returns the TupleDesc of the table stored in this DbFile.
   * 
   * @return TupleDesc of this DbFile.
   */
  @Override
  public TupleDesc getTupleDesc() {
    return tupleDesc;
  }

  // see DbFile.java for javadocs
  @Override
  public Page readPage(PageId pid) {
    int offset = BufferPool.getPageSize() * pid.pageNumber();
    try {
      FileChannel fileChannel = new FileInputStream(backingFile).getChannel();
      ByteBuffer readData = ByteBuffer.allocate(BufferPool.getPageSize());
      int numberOfBytesRead = fileChannel.read(readData, offset);
      if (numberOfBytesRead == BufferPool.getPageSize()) {
        return new HeapPage(pid, readData.array());
      }
      throw new RuntimeException("Did not read entire page successfully.");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // see DbFile.java for javadocs
  @Override
  public void writePage(Page page) throws IOException {
    // some code goes here
    // not necessary for lab1
  }

  /**
   * Returns the number of pages in this HeapFile.
   */
  public int numPages() {
    return numberOfPages;
  }

  // see DbFile.java for javadocs
  @Override
  public ArrayList<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException,
      TransactionAbortedException {
    // some code goes here
    return null;
    // not necessary for lab1
  }

  // see DbFile.java for javadocs
  @Override
  public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
      TransactionAbortedException {
    // some code goes here
    return null;
    // not necessary for lab1
  }

  // see DbFile.java for javadocs
  @Override
  public DbFileIterator iterator(TransactionId transactionId) {
    return HeapFileIterator.create(getId(), numberOfPages, transactionId);
  }
}