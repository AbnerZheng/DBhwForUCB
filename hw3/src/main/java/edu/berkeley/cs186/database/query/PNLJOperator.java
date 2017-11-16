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
import edu.berkeley.cs186.database.table.Table;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator extends JoinIterator {
    private final BacktrackingIterator<Page> leftPageIterator;
    private final BacktrackingIterator<Page> rightPageIterator;
    private final Record nextRecord;
    private Page leftPage;
    private Page rightPage;
    // add any member variables here

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftPageIterator = PNLJOperator.this.getPageIterator(getLeftTableName());
      this.rightPageIterator = PNLJOperator.this.getPageIterator(getRightTableName());
      this.rightPageIterator.mark();
      this.nextRecord = null;
    }

    public boolean hasNext() {
      if(nextRecord!=null){
        return true;
      }
      while (true) {
        if (leftPage == null) {
          if (this.leftPageIterator.hasNext()) {
            leftPage = leftPageIterator.next();
            Page[] leftPageBlocks = new Page[1];
            leftPageBlocks[0] = leftPage;
            try {

              PNLJOperator.this.getBlockIterator(getLeftTableName(),leftPageBlocks);
            } catch (DatabaseException e) {
              e.printStackTrace();
            }
            rightPageIterator.reset();
          } else {
            return false;
          }
        }
        if (rightPage == null) {
          if (rightPageIterator.hasNext()){
            rightPage = rightPageIterator.next();
          }else{
            return false;
          }
        }
//        while()
      }
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      throw new UnsupportedOperationException("hw3: TODO");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
