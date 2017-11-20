package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
                           QueryOperator rightSource,
                           String leftColumnName,
                           String rightColumnName,
                           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }


  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class SortMergeIterator extends JoinIterator {
    private final RecordIterator leftRecordIterator;
    private final RecordIterator rightRecordIterator;
    private Record nextRecord;
    private Record leftRecord;
    private Record rightRecord;
    private boolean markTrue;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();
      final SortOperator sortOperator = new SortOperator(getTransaction(), getLeftTableName(), new LeftRecordComparator());
      final String sort = sortOperator.sort();
      final SortOperator sortOperator1 = new SortOperator(getTransaction(), getLeftTableName(), new RightRecordComparator());
      final String sort1 = sortOperator1.sort();
      leftRecordIterator = getRecordIterator(sort);
      rightRecordIterator = getRecordIterator(sort1);
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if(nextRecord != null){
        return true;
      }
      while (true) {
        if (leftRecord == null) {
          if (leftRecordIterator.hasNext()) {
            leftRecord = leftRecordIterator.next();
            if(markTrue){
              rightRecordIterator.reset();
              this.rightRecord = null;
            }
          } else {
            return false;
          }
        }
        if (rightRecord == null) {
          if (rightRecordIterator.hasNext()) {
            rightRecord = rightRecordIterator.next();
          } else if(this.leftRecord != null){
            rightRecordIterator.reset();
            this.leftRecord = null;
            continue;
          } else {
            return false;
          }
        }
        final int compareLeftToRight = compareLeftToRight(leftRecord, rightRecord);
        if (compareLeftToRight < 0) {
          leftRecord = null;
        }else if(compareLeftToRight > 0){
          rightRecord = null;
        }else{
          if(!markTrue){
            markTrue = true;
            this.rightRecordIterator.mark();
          }
          List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
          List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
          leftValues.addAll(rightValues);
          this.nextRecord = new Record(leftValues);
          this.rightRecord = null;
          return true;
        }
      }
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (hasNext()) {
        final Record nextRecord = this.nextRecord;
        this.nextRecord = null;
        return nextRecord;
      }
      throw new NoSuchElementException("there are no more Records to yield");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
          o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private int compareLeftToRight(Record o1, Record o2) {
      return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex())
        .compareTo(o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
          o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
