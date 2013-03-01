package simpledb;

import java.util.Iterator;

/**
 * Implements a DbIterator by wrapping an Iterable<Tuple>.
 */
public class TupleIterator implements DbIterator {
  Iterator<Tuple> i = null;
  TupleDesc td = null;
  Iterable<Tuple> tuples = null;

  /**
   * Constructs an iterator from the specified Iterable, and the specified
   * descriptor.
   * 
   * @param tuples The set of tuples to iterate over
   */
  public TupleIterator(TupleDesc td, Iterable<Tuple> tuples) {
    this.td = td;
    this.tuples = tuples;

    // check that all tuples are the right TupleDesc
    for (Tuple t : tuples) {
      if (!t.getTupleDesc().equals(td))
        throw new IllegalArgumentException("incompatible tuple in tuple set");
    }
  }

  @Override
  public void open() {
    i = tuples.iterator();
  }

  @Override
  public boolean hasNext() {
    return i.hasNext();
  }

  @Override
  public Tuple next() {
    return i.next();
  }

  @Override
  public void rewind() {
    close();
    open();
  }

  @Override
  public TupleDesc getTupleDesc() {
    return td;
  }

  @Override
  public void close() {
    i = null;
  }
}
