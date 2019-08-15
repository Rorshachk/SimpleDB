package simpledb;

import java.io.*;
import java.util.*;

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

    private static final long serialVersionUID = 1L;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    File file;
    TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    //    throw new UnsupportedOperationException("implement this");
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPageId hpid = (HeapPageId) pid;
        RandomAccessFile rf;
        byte[] data = new byte[BufferPool.PAGE_SIZE];
        try{
            rf = new RandomAccessFile(this.file, "r");
            rf.seek(BufferPool.PAGE_SIZE * pid.pageNumber());
            rf.read(data);
            rf.close();
            return new HeapPage(hpid, data);
        } catch (FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(this.file.length() * 1.0 / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        class Itr implements DbFileIterator{
            private static final long serialVersionUID = 1L;
			private TransactionId tid;
            private Iterator<Tuple> iterator;
            private int pageCursor;
            private boolean opened;

            public Itr(TransactionId tid){
                this.tid = tid;
                this.pageCursor = 0;
                this.iterator = null;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException{
                opened = true;
                HeapPageId pid = new HeapPageId(getId(), pageCursor);
                HeapPage page = (HeapPage)(Database.getBufferPool()).getPage(this.tid, pid, Permissions.READ_ONLY);
                iterator = page.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException{
                if(opened){
                    if(iterator == null) return false;
                    if(iterator.hasNext()) return true;
                    while(pageCursor < numPages() - 1){
                        pageCursor++;
                        HeapPageId pid = new HeapPageId(getId(), pageCursor);
                        HeapPage page = (HeapPage)(Database.getBufferPool()).getPage(this.tid, pid, Permissions.READ_ONLY);
                        iterator = page.iterator();
                        if(iterator.hasNext()) return true;
                    }
                    return false;
                }else return false;
            }
            
            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
                if(hasNext()) return iterator.next();
                else throw new NoSuchElementException();
            }
            
            @Override
            public void close(){
                opened = false;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException{
                pageCursor = 0;
                open();
            }
        }
        return new Itr(tid);
    }

}

