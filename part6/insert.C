// #include "catalog.h"
// #include "query.h"

// /*
//  * Inserts a record into the specified relation.
//  *
//  * Returns:
//  * 	OK on success
//  * 	an error code otherwise
//  */

// const Status QU_Insert(const string &relation,
// 					   const int attrCnt,
// 					   const attrInfo attrList[])
// {
// 	// part 6
// 	Status status = OK;
// 	int relcnt;
// 	AttrDesc *rels = new AttrDesc[attrCnt];
// 	status = attrCat->getRelInfo(relation, relcnt, rels);
// 	if (status)
// 	{
// 		return status;
// 	}
// 	if (relcnt < attrCnt)
// 	{
// 		return ATTRTOOLONG;
// 	}
// 	int reccnt = 0;
// 	for (int i = 0; i < relcnt; i++)
// 	{
// 		reccnt += rels[i].attrLen;
// 	}
// 	char *buf = new char[reccnt];
// 	for (int i = 0; i < relcnt; i++)
// 	{
// 		bool found = false;
// 		for (int j = 0; j < attrCnt; j++)
// 		{
// 			if (attrList[j].attrValue == NULL)
// 			{
// 				return ATTRNOTFOUND;
// 			}
// 			if (!strcmp(rels[i].attrName, attrList[j].attrName)) // if names are the same
// 			{
// 				memcpy(buf + rels[i].attrOffset,
// 					   attrList[j].attrValue,
// 					   rels[i].attrLen);
// 				found = true;
// 				break;
// 			}
// 		}
// 		if (!found)
// 		{
// 			return ATTRNOTFOUND;
// 		}
// 	}
// 	InsertFileScan *ifs = new InsertFileScan(relation, status);
// 	if (status)
// 	{
// 		return status;
// 	}

// 	Record rec;
// 	rec.data = buf;
// 	rec.length = reccnt;

// 	RID rid;
// 	return ifs->insertRecord(rec, rid);
// }

/* insert.C */
#include "catalog.h"
#include "query.h"
#include <cstring>
#include <cstdlib>

/**
 * Inserts a record into the specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */
const Status QU_Insert(
    const string &relation,
    const int attrCnt,
    const attrInfo attrList[])
{
    Status status;
    int relCnt = 0;
    AttrDesc *relAttrs = nullptr;

    // 1) Retrieve schema information for the relation
    status = attrCat->getRelInfo(relation, relCnt, relAttrs);
    if (status != OK) return status;

    // Allocate array for attribute descriptors
    relAttrs = new AttrDesc[relCnt];
    status = attrCat->getRelInfo(relation, relCnt, relAttrs);
    if (status != OK) {
        delete[] relAttrs;
        return status;
    }
    if (relCnt < attrCnt) {
        delete[] relAttrs;
        return ATTRTOOLONG;
    }

    // 2) Build and zero-initialize the record buffer
    int recLen = 0;
    for (int i = 0; i < relCnt; i++) {
        recLen += relAttrs[i].attrLen;
    }
    char *buffer = new char[recLen];
    memset(buffer, 0, recLen);

    // 3) Populate buffer fields from attrList
    for (int i = 0; i < relCnt; i++) {
        bool found = false;
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(relAttrs[i].attrName, attrList[j].attrName) == 0) {
                switch (relAttrs[i].attrType) {
                    case INTEGER: {
                        int iv = atoi(attrList[j].attrValue);
                        memcpy(buffer + relAttrs[i].attrOffset,
                               &iv,
                               sizeof(int));
                        break;
                    }
                    case FLOAT: {
                        float fv = atof(attrList[j].attrValue);
                        memcpy(buffer + relAttrs[i].attrOffset,
                               &fv,
                               sizeof(float));
                        break;
                    }
                    case STRING: {
                        memcpy(buffer + relAttrs[i].attrOffset,
                               attrList[j].attrValue,
                               relAttrs[i].attrLen);
                        break;
                    }
                }
                found = true;
                break;
            }
        }
        if (!found) {
            delete[] relAttrs;
            delete[] buffer;
            return ATTRNOTFOUND;
        }
    }

    // 4) Insert the new record
    InsertFileScan ifs(relation, status);
    if (status != OK) {
        delete[] relAttrs;
        delete[] buffer;
        return status;
    }
    Record rec;
    rec.data = buffer;
    rec.length = recLen;
    RID rid;
    status = ifs.insertRecord(rec, rid);

    // 5) Clean up
    delete[] relAttrs;
    delete[] buffer;
    return status;
}