package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import sun.font.GlyphLayout;

import javax.xml.crypto.Data;

public class BNLJOperator extends JoinOperator {

  protected int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }


  /**
   * An implementation of Iterator that provides
   * an iterator interface for this operator.
   */
  private class BNLJIterator extends JoinIterator {
    public static final int MAX_PAGES = 10;
    private BacktrackingIterator<Record> leftBlockIterator;
    private BacktrackingIterator<Record> rightBlockIterator;
    private BacktrackingIterator<Page> leftPageIterator;
    private BacktrackingIterator<Page> rightPageIterator;
    private Record nextRecord;
    private Record leftRecord;
    private Record rightRecord;
    // add any member variables here

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftPageIterator = BNLJOperator.this.getPageIterator(getLeftTableName());
      this.leftPageIterator.next();
      this.leftPageIterator.mark();
      this.rightPageIterator = BNLJOperator.this.getPageIterator(getRightTableName());
      this.rightPageIterator.next();
      this.rightPageIterator.mark();
      this.leftBlockIterator = BNLJOperator.this.getBlockIterator(getLeftTableName(), leftPageIterator, MAX_PAGES);
      this.rightBlockIterator = BNLJOperator.this.getBlockIterator(getRightTableName(),rightPageIterator,MAX_PAGES);

      this.rightBlockIterator.next();
      this.rightBlockIterator.mark();
      this.rightBlockIterator.reset();
    }

    public boolean hasNext() {
      if(this.nextRecord!=null){
        return true;
      }

      while (true){
        if(this.leftRecord == null){
          if(this.leftBlockIterator.hasNext()){
            this.leftRecord = leftBlockIterator.next();
            this.rightBlockIterator.reset();
          }else {
            if(rightPageIterator.hasNext()){
              leftBlockIterator.reset();
              leftRecord = leftBlockIterator.next();
              try {
                rightBlockIterator = BNLJOperator.this.getBlockIterator(getRightTableName(), rightPageIterator, MAX_PAGES);
              } catch (DatabaseException e) {
                return false;
              }
            }else if(leftPageIterator.hasNext()) {// 重新下一轮右块遍历
              try {
                this.leftBlockIterator = BNLJOperator.this.getBlockIterator(getLeftTableName(), leftPageIterator, MAX_PAGES);
                this.rightPageIterator.reset();
                this.rightBlockIterator = BNLJOperator.this.getBlockIterator(getRightTableName(), rightPageIterator, MAX_PAGES);
                this.rightBlockIterator.next();

                this.leftBlockIterator.mark();
                this.rightBlockIterator.mark();
                this.rightBlockIterator.reset(); // 撤销上面的next
              } catch (DatabaseException e) {
                return false;
              }
            }else {
              return false;
            }
          }
        }else{
          while (rightBlockIterator.hasNext()){
            final Record rightRecord = rightBlockIterator.next();
            DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
            DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
            if (leftJoinValue.equals(rightJoinValue)) {
              List<DataBox> leftValue = new ArrayList<>(this.leftRecord.getValues());
              List<DataBox> rightValue = new ArrayList<>(rightRecord.getValues());
              leftValue.addAll(rightValue);
              this.nextRecord = new Record(leftValue);
              return true;
            }
          }

          // 运行到这说明右块遍历好了
          this.leftRecord = null;
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
      if(hasNext()){
        final Record nextRecord = this.nextRecord;
        this.nextRecord = null;
        return nextRecord;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
