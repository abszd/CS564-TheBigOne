// Arian Abbaszadeh - 9083678194
// Glenn Hadcock - 9085633288
// Akshaya Somasundaram - 9085417682
// creates the heapfile class and functions to interact with it
#include "heapfile.h"
#include "error.h"

/**
 * @param fileName - name fo the file to be created
 * @return status of the function
 * Creates a file as long as it does not already exist, then create a header page and the first page in the file.
 * Initialize both of these, unpin pages and mark as dirty
 */
const Status createHeapFile(const string fileName)
{
    File *file;
    Status status;
    FileHdrPage *hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page *newPage;

    status = db.openFile(fileName, file);
    if (status == OK)
    {
        db.closeFile(file);
        return FILEEXISTS;
    }

    status = db.createFile(fileName);
    if (status != OK)
    {
        return status;
    }

    status = db.openFile(fileName, file);
    if (status != OK)
    {
        return status;
    }

    status = bufMgr->allocPage(file, hdrPageNo, newPage);
    if (status != OK)
    {
        db.closeFile(file);
        return status;
    }

    hdrPage = (FileHdrPage *)newPage;
    strcpy(hdrPage->fileName, fileName.c_str());
    hdrPage->firstPage = -1;
    hdrPage->lastPage = -1;
    hdrPage->pageCnt = 0;
    hdrPage->recCnt = 0;

    status = bufMgr->allocPage(file, newPageNo, newPage);
    if (status != OK)
    {
        db.closeFile(file);
        return status;
    }

    newPage->init(newPageNo);
    hdrPage->firstPage = newPageNo;
    hdrPage->lastPage = newPageNo;
    hdrPage->pageCnt = 1;

    status = bufMgr->unPinPage(file, newPageNo, true);
    if (status != OK)
    {
        db.closeFile(file);
        return status;
    }

    status = bufMgr->unPinPage(file, hdrPageNo, true);
    if (status != OK)
    {
        db.closeFile(file);
        return status;
    }

    status = bufMgr->flushFile(file);
    if (status != OK)
    {
        db.closeFile(file);
        return status;
    }

    status = db.closeFile(file);
    return OK;
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
    return (db.destroyFile(fileName));
}

/**
 * Constructor class
 * Opens the file and fills in the header page and first data page
 */
HeapFile::HeapFile(const string &fileName, Status &returnStatus)
{
    Status status;
    Page *pagePtr;

    cout << "opening file " << fileName << endl;

    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        status = filePtr->getFirstPage(headerPageNo);
        if (status != OK)
        {
            returnStatus = status;
            return;
        }

        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if (status != OK)
        {
            returnStatus = status;
            return;
        }

        headerPage = (FileHdrPage *)pagePtr;
        hdrDirtyFlag = false;

        curPageNo = headerPage->firstPage;
        curRec = NULLRID;

        if (curPageNo == -1)
        {
            curPage = NULL;
            curDirtyFlag = false;
        }
        else
        {
            status = bufMgr->readPage(filePtr, curPageNo, pagePtr);
            if (status != OK)
            {
                returnStatus = status;
                return;
            }
            curPage = pagePtr;
            curDirtyFlag = false;
        }

        returnStatus = OK;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK)
            cerr << "error in unpin of date page\n";
    }

    // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK)
        cerr << "error in unpin of header page\n";

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
    return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID &rid, Record &rec)
{
    Status status;

    if (curPage == NULL)
    {
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK)
        {
            return status;
        }
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        status = curPage->getRecord(rid, rec);
        if (status == OK)
        {
            curRec = rid;
        }
        return status;
    }

    if (curPageNo == rid.pageNo)
    {
        status = curPage->getRecord(rid, rec);
        if (status == OK)
        {
            curRec = rid;
        }
        return status;
    }

    status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    if (status != OK)
    {
        return status;
    }
    status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
    if (status != OK)
    {
        curPage = NULL;
        return status;
    }

    curPageNo = rid.pageNo;
    curDirtyFlag = false;

    status = curPage->getRecord(rid, rec);
    if (status == OK)
    {
        curRec = rid;
    }
    return status;
}

HeapFileScan::HeapFileScan(const string &name,
                           Status &status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                                     const int length_,
                                     const Datatype type_,
                                     const char *filter_,
                                     const Operator op_)
{
    if (!filter_)
    { // no filtering requested
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int) || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false; // it will be clean
    }
    else
        curRec = markedRec;
    return OK;
}

/**
 * @return RID via outRid
 * check the current page - if it's null read in the first page of the file
 * then loop through the rest of the records, if we are at EOF attempt to change
 * the page and continue looping through the records
 */
const Status HeapFileScan::scanNext(RID &outRid)
{

    Status status = OK;
    RID nextRid;
    Record rec;

    if (curPageNo == -1)
    {
        return FILEEOF;
    }

    if (curPage == NULL)
    {
        curPageNo = headerPage->firstPage;
        if (curPageNo == -1)
        {
            return FILEEOF;
        }

        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
        {
            return status;
        }

        curDirtyFlag = false;

        status = curPage->firstRecord(curRec);
        if (status != OK)
        {
            int nextPageNo;
            curPage->getNextPage(nextPageNo);

            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
            {
                return status;
            }

            curPage = NULL;
            if (nextPageNo == -1)
            {
                curPageNo = -1;
                return FILEEOF;
            }

            curPageNo = nextPageNo;
            return scanNext(outRid);
        }

        status = curPage->getRecord(curRec, rec);
        if (status != OK)
        {
            return status;
        }

        if (matchRec(rec))
        {
            outRid = curRec;
            return OK;
        }
    }

    while (true)
    {
        status = curPage->nextRecord(curRec, nextRid);

        if (status == OK)
        {
            curRec = nextRid;
            status = curPage->getRecord(curRec, rec);
            if (status != OK)
            {
                return status;
            }

            if (matchRec(rec))
            {
                outRid = curRec;
                return OK;
            }
        }
        else
        {
            int nextPageNo;
            curPage->getNextPage(nextPageNo);

            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
            {
                return status;
            }

            curPage = NULL;

            if (nextPageNo == -1)
            {
                curPageNo = -1;
                return FILEEOF;
            }

            curPageNo = nextPageNo;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK)
            {
                return status;
            }

            curDirtyFlag = false;

            status = curPage->firstRecord(curRec);
            if (status != OK)
            {
                continue;
            }

            status = curPage->getRecord(curRec, rec);
            if (status != OK)
            {
                return status;
            }

            if (matchRec(rec))
            {

                outRid = curRec;
                return OK;
            }
            curRec = tmpRid;
        }
    }

    // If we get here, we need to move to the next page
    nextPageNo = curPage->getNextPage();
    
    // Unpin current page
    status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    if (status != OK) return status;
    
    // If no next page, we're at the end of the file
    if (nextPageNo == -1) {
        curPage = NULL;
        curPageNo = -1;
        curDirtyFlag = false;
        return FILEEOF;
    }
    
    // Read the next page
    status = bufMgr->readPage(filePtr, nextPageNo, (Page*&)curPage);
    if (status != OK) return status;
    
    curPageNo = nextPageNo;
    curDirtyFlag = false;
    curRec = NULLRID;  // Reset curRec for new page
    
    // Recursively scan the new page
    return scanNext(outRid);
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record &rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}

// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record &rec) const
{
    // no filtering requested
    if (!filter)
        return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length - 1) >= rec.length)
        return false;

    float diff = 0; // < 0 if attr < fltr
    switch (type)
    {

    case INTEGER:
        int iattr, ifltr; // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr; // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch (op)
    {
    case LT:
        if (diff < 0.0)
            return true;
        break;
    case LTE:
        if (diff <= 0.0)
            return true;
        break;
    case EQ:
        if (diff == 0.0)
            return true;
        break;
    case GTE:
        if (diff >= 0.0)
            return true;
        break;
    case GT:
        if (diff > 0.0)
            return true;
        break;
    case NE:
        if (diff != 0.0)
            return true;
        break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string &name,
                               Status &status) : HeapFile(name, status)
{
    // Do nothing. Heapfile constructor will bread the header page and the first
    //  data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK)
            cerr << "error in unpin of data page\n";
    }
}

/**
 * @param rec
 * @return RID via outRid
 * if the current page is null, attempt to add the record to the last page, if the curpage is not the last page
 * move it there. If there is not enough space on the current page we allocate a new one, allocate the record,
 * and set it as the last page along with some initializtion.
 */
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid)
{
    Page *newPage;
    int newPageNo;
    Status status;

    // check for very large records
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (curPage == NULL || curPageNo != headerPage->lastPage)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
        }
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false;
    }

    status = curPage->insertRecord(rec, outRid);
    if (status != OK)
    {
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK)
        {
            return status;
        }
        newPage->init(newPageNo);

        curPage->setNextPage(newPageNo);
        newPage->setNextPage(-1);

        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        if (status != OK)
        {
            return status;
        }

        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = false;

        status = curPage->insertRecord(rec, outRid);
        if (status != OK)
        {
            return status;
        }
    }

    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curDirtyFlag = true;

    return OK;
}