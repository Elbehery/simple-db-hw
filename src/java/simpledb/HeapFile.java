package simpledb;

import javax.xml.crypto.Data;
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
    private int currentPages;

    // TODO : keep track of pages with empty slots

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
        this.currentPages = 1;
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
        Page requestedPage = null;

        if (pageNumber < 0)
            throw new IllegalArgumentException("Page ID is not valid, Page doesn't exist");

        // add new page if required and return it
        if (pageNumber > numPages()) {
            requestedPage = addPageToFile();
            return requestedPage;
        }

        // read existing page from DISK, using Pointer/Offset arithmetic
        int readOffset = pageNumber * BufferPool.getPageSize();
        byte[] buffer = HeapPage.createEmptyPageData();

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

    private Page addPageToFile() {
        HeapPageId newPageId = new HeapPageId(this.getId(), numPages() + 1);
        this.currentPages++;
        HeapPage newHeapPage = null;
        try {
            newHeapPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
        } catch (IOException e) {
            System.err.println(String.format("Error while adding a new Page to file %d", getId()));
            System.exit(0);
        }
        return newHeapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // write page to DISK, using Pointer/Offset arithmetic
        int pageNumber = page.getId().getPageNumber();
        int writeOffset = pageNumber * BufferPool.getPageSize();
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(this.dataFile);
            fos.write(page.getPageData(), writeOffset, BufferPool.getPageSize());
            fos.flush();
        } catch (IOException e) {
            System.err.println(String.format("Page number %d from TableFile %d is could not be flushed to disk.", pageNumber, page.getId().getTableId()));
            e.printStackTrace();
        } finally {
            fos.close();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return Math.max((int) (this.dataFile.length() / BufferPool.getPageSize()), currentPages);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // retrieve the page from the BufferPool
        // The BufferPool will instruct the heapfile to read from Desk if the page is not cached.
        HeapPageId newHeapPageId = new HeapPageId(getId(), numPages());
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, newHeapPageId, Permissions.READ_WRITE);
        // if the page is full
        if (heapPage.getNumEmptySlots() < 1) {
            newHeapPageId = new HeapPageId(getId(), numPages() + 1);
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, newHeapPageId, Permissions.READ_WRITE);
        }
        // update record metadata
        RecordId newRecordId = new RecordId(newHeapPageId, t.getRecordId().getTupleNumber());
        t.setRecordId(newRecordId);
        // insert the tuple and mark page as dirty to be flushed later
        heapPage.insertTuple(t);
        heapPage.markDirty(true, tid);
        ArrayList<Page> modifiedPages = new ArrayList<>();
        modifiedPages.add(heapPage);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // retrieve the page from the BufferPool
        // The BufferPool will instruct the heapfile to read from Desk if the page is not cached.
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        // update page metadata
        heapPage.markDirty(true, tid);
        ArrayList<Page> modifiedPages = new ArrayList<>();
        modifiedPages.add(heapPage);
        return modifiedPages;
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

