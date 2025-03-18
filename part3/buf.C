#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

const Status BufMgr::allocBuf(int &frame)
{
    int checked = 0;
    while (checked < numBufs)
    {
        advanceClock();
        BufDesc *curBuf = &bufTable[clockHand];

        if (!curBuf->valid)
        {
            frame = clockHand;
            return OK;
        }

        checked++;
        if (curBuf->refbit)
        {
            curBuf->refbit = false;
            continue;
        }

        if (curBuf->pinCnt > 0)
        {
            continue;
        }

        if (curBuf->dirty)
        {
            Status writeStatus = curBuf->file->writePage(curBuf->pageNo, &bufPool[clockHand]);
            if (writeStatus != OK)
            {
                return UNIXERR;
            }
        }

        if (curBuf->valid)
        {
            Status removeStatus = hashTable->remove(curBuf->file, curBuf->pageNo);
            if (removeStatus != OK)
            {
                return removeStatus;
            }
        }
        frame = clockHand;
        curBuf->Clear();
        return OK;
    }
    return BUFFEREXCEEDED;
}

const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    int framePtr = 0;
    Status pageStatus = hashTable->lookup(file, PageNo, framePtr);

    if (pageStatus == HASHNOTFOUND)
    {
        Status allocStatus = allocBuf(framePtr);
        if (allocStatus != OK)
        {
            return allocStatus;
        }
        Status readStatus = file->readPage(PageNo, &bufPool[framePtr]);
        if (readStatus != OK)
        {
            return UNIXERR;
        }

        Status hashInsertStatus = hashTable->insert(file, PageNo, framePtr);
        if (hashInsertStatus != OK)
        {
            return HASHTBLERROR;
        }
        bufTable[framePtr].Set(file, PageNo);

        page = &bufPool[framePtr];
        return OK;
    }
    else
    {
        BufDesc &curFrame = bufTable[framePtr];
        curFrame.refbit = !curFrame.refbit;
        curFrame.pinCnt++;
        page = &bufPool[framePtr];
        return OK;
    }
}

const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    int framePtr = 0;
    Status pageStatus = hashTable->lookup(file, PageNo, framePtr);

    if (pageStatus != OK)
    {
        return HASHNOTFOUND;
    }

    BufDesc &curFrame = bufTable[framePtr];
    if (curFrame.pinCnt > 0)
    {
        curFrame.pinCnt--;
        curFrame.dirty = curFrame.dirty | dirty;
        return OK;
    }
    return PAGENOTPINNED;
}

const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    Status pageAllocStatus = file->allocatePage(pageNo);
    if (pageAllocStatus != OK)
    {
        return UNIXERR;
    }
    int framePtr = 0;
    Status allocBufStatus = allocBuf(framePtr);
    if (allocBufStatus != OK)
    {
        return allocBufStatus;
    }

    Status hashInsertStatus = hashTable->insert(file, pageNo, framePtr);
    if (hashInsertStatus != OK)
    {
        return HASHTBLERROR;
    }
    bufTable[framePtr].Set(file, pageNo);
    page = &bufPool[framePtr];
    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
