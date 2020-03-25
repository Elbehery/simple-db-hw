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
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private TupleDesc tupleDesc;
    private File dataFile;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        if (f == null || td == null)
            throw new NullPointerException("Input to HeapFile Can't be Null");

        this.tupleDesc = td;
        this.dataFile = f;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.dataFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.dataFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {

        int pageNumber = pid.getPageNumber();

        if (pageNumber < 0 || pageNumber > numPages())
            throw new IllegalArgumentException("Page ID is not valid, Page doesn't exist");

        int readOffset = (pid.getPageNumber()) * BufferPool.getPageSize();
        byte[] buffer = HeapPage.createEmptyPageData();
        Page requestedPage = null;

        try {
            FileInputStream fis = new FileInputStream(this.dataFile);
            fis.skip(readOffset);
            fis.read(buffer);
            requestedPage = new HeapPage((HeapPageId) pid, buffer);
        } catch (IOException e) {
            System.err.println(String.format("Page number %d from TableFile %d is corrupted && Skipped.", pid.getPageNumber(), pid.getTableId()));
            e.printStackTrace();
        }

        return requestedPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO : currentPage++ && write the page.
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (this.dataFile.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {

            int currentPage = 0;
            Iterator<Tuple> currentPageIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                currentPageIterator = ((HeapPage) Database.getBufferPool().getPage(new TransactionId(), new HeapPageId(getId(), currentPage), Permissions.READ_ONLY)).iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (currentPageIterator == null)
                    return false;

                if (currentPageIterator.hasNext())
                    return true;

                if (currentPage < numPages()) {
                    currentPage++;
                    open();
                    return currentPageIterator.hasNext();
                }

                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext())
                    throw new NoSuchElementException("No Next in the iterator, please use hasNext()");

                return currentPageIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                currentPage = 0;
                open();
            }

            @Override
            public void close() {
                currentPageIterator = null;
            }
        };
    }

}

