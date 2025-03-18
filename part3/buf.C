// Arian Abbaszadeh - 9083678194
// Glenn Hadcock -
// Akshaya Somasundaram -
// Sets up the buffer manager class and functions to interact with the buffer manager

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
    // if we've checked all frames twice to allow for the reference bit then we're at capacity and checked is being incremented here
    while (checked++ < numBufs * 2)
    {
        advanceClock();
        BufDesc *curBuf = &bufTable[clockHand]; // current frame we're checking

        if (!curBuf->valid) // if we have an invalid frame we can just allocoate
        {
            frame = clockHand;
            curBuf->Clear();
            return OK;
        }

        if (curBuf->refbit) // if the refbit is 1 set to 0
        {
            curBuf->refbit = false;
            continue;
        }

        if (curBuf->pinCnt > 0) // try next frame if frame is pinned
        {
            continue;
        }

        if (curBuf->dirty) // if weve modified the page flush changes
        {
            if (curBuf->file->writePage(curBuf->pageNo, &bufPool[clockHand]) != OK)
            {
                return UNIXERR;
            }
        }

        if (curBuf->valid) // Safety check for validity
        {
            if (hashTable->remove(curBuf->file, curBuf->pageNo) != OK) // Then remove from buffer
            {
                return HASHTBLERROR;
            }
        }
        // Set frame ptr and clear other variables
        frame = clockHand;
        curBuf->Clear();
        return OK;
    }
    return BUFFEREXCEEDED; // If we reach this weve tried every frame and allowed for the reference bit to switch
}

const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    int framePtr = 0;
    if (hashTable->lookup(file, PageNo, framePtr) == HASHNOTFOUND) // if the page isn't found we need to allocate a new page
    {
        Status allocStatus = allocBuf(framePtr); // allocate a frame for this page
        if (allocStatus != OK)
        {
            return allocStatus;
        }
        if (file->readPage(PageNo, &bufPool[framePtr]) != OK) // Read it from disk
        {
            return UNIXERR;
        }
        if (hashTable->insert(file, PageNo, framePtr) != OK) // Insert the page into the buffer pool
        {
            return HASHTBLERROR;
        }
        // Set the new frame up
        bufTable[framePtr].Set(file, PageNo);
    }
    else // Otherwise, we can just increment the pincount for the frame with the page in it
    {
        BufDesc &curFrame = bufTable[framePtr];
        curFrame.refbit = !curFrame.refbit; // Also change the refernce bit
        curFrame.pinCnt++;
    }
    // Set the page pointer
    page = &bufPool[framePtr];
    return OK;
}

const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    int framePtr = 0;
    Status pageStatus = hashTable->lookup(file, PageNo, framePtr);

    if (pageStatus != OK) // can't find it
    {
        return HASHNOTFOUND;
    }

    BufDesc &curFrame = bufTable[framePtr];
    if (curFrame.pinCnt > 0)
    {
        // updat the pincount and dirty bit
        curFrame.pinCnt--;
        curFrame.dirty = curFrame.dirty | dirty;
        return OK;
    }
    return PAGENOTPINNED; // We don't want to increment the count if its not pinned, this could cause allocation issues
}

const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    if (file->allocatePage(pageNo) != OK) // Make the page
    {
        return UNIXERR;
    }
    int framePtr = 0;
    Status allocBufStatus = allocBuf(framePtr); // We find an open spot for it in the buffer pool
    if (allocBufStatus != OK)
    {
        return allocBufStatus;
    }

    if (hashTable->insert(file, pageNo, framePtr) != OK) // Insert into the pool
    {
        return HASHTBLERROR;
    }
    // Set up the frame and set page pointer
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
