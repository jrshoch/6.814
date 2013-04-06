package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

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
  private final RandomAccessFile accessFile;
  private final TupleDesc tupleDesc;
  private AtomicInteger numberOfPages;

  /**
   * Constructs a heap file backed by the specified file.
   * 
   * @param f the file that stores the on-disk backing store for this heap file.
   */
  public HeapFile(File f, TupleDesc td) {
    this.backingFile = f;
    try {
      this.accessFile = new RandomAccessFile(f, "rw");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.tupleDesc = td;
    this.numberOfPages = new AtomicInteger((int) backingFile.length() / BufferPool.getPageSize());
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
    int pageSize = BufferPool.getPageSize();
    int offset = pageSize * pid.pageNumber();
    try {
      byte[] readData = new byte[pageSize];
      accessFile.seek(offset);
      int numberOfBytesRead = accessFile.read(readData);
      if (numberOfBytesRead == BufferPool.getPageSize()) {
        return new HeapPage(pid, readData);
      }
      System.out.println("numberOfBytesRead: " + numberOfBytesRead + ", offest: " + offset + ", backingFile.size(): " + backingFile.length() + ", page size: " + BufferPool.getPageSize() + ", page number; " + pid.pageNumber());
      throw new RuntimeException("Did not read entire page successfully.");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // see DbFile.java for javadocs
  @Override
  public void writePage(Page page) throws IOException {
    int pageSize = BufferPool.getPageSize();
    int offset = pageSize * page.getId().pageNumber();
    System.out.println("page size before write: " + backingFile.length());
    try {
      accessFile.seek(offset);
      accessFile.write(page.getPageData());
      System.out.println("Wrote to page number " + page.getId().pageNumber() + " at offset: " + offset + " to create resulting file size: " + backingFile.length());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the number of pages in this HeapFile.
   */
  public int numPages() {
    return numberOfPages.get();
  }

  // see DbFile.java for javadocs
  @Override
  public ArrayList<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException,
      TransactionAbortedException {
    HeapPage insertedPage = null;
    for (int pageNumber = 0; pageNumber < numberOfPages.get(); pageNumber++) {
      PageId pageId = new HeapPageId(getId(), pageNumber);
      HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId,
          Permissions.READ_ONLY);
      int numberOfEmptySlots = heapPage.getNumEmptySlots();
      if (numberOfEmptySlots > 0) {
        heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        insertedPage = heapPage;
        break;
      }
      Database.getBufferPool().releasePage(tid, pageId);
    }
    if (insertedPage == null) {
      HeapPage newHeapPage = new HeapPage(new HeapPageId(getId(), numberOfPages.getAndIncrement()),
          HeapPage.createEmptyPageData());
      newHeapPage.insertTuple(t);
      writePage(newHeapPage);
      insertedPage = newHeapPage;
    }
    ArrayList<Page> affectedPages = new ArrayList<Page>();
    affectedPages.add(insertedPage);
    return affectedPages;
  }

  // see DbFile.java for javadocs
  @Override
  public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
      TransactionAbortedException {
    PageId pageId = t.getRecordId().getPageId();
    if (pageId == null || pageId.getTableId() != getId()) {
      throw new DbException("File cannot delete tuple that it does not contain.");
    }
    HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId,
        Permissions.READ_WRITE);
    heapPage.deleteTuple(t);
    return heapPage;
  }

  // see DbFile.java for javadocs
  @Override
  public DbFileIterator iterator(TransactionId transactionId) {
    return HeapFileIterator.create(getId(), numberOfPages.get(), transactionId);
  }
}
