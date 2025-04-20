#include "catalog.h"
#include "query.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation,
					   const int attrCnt,
					   const attrInfo attrList[])
{
	// part 6
	Status status;
	int relcnt;
	AttrDesc *rels = new AttrDesc[attrCnt];
	status = attrCat->getRelInfo(relation, relcnt, rels);
	if (status)
	{
		return status;
	}
	if (relcnt < attrCnt)
	{
		return ATTRTOOLONG;
	}
	int reccnt = 0;
	for (int i = 0; i < relcnt; i++)
	{
		reccnt += rels[i].attrLen;
	}
	char *buf = new char[reccnt];
	for (int i = 0; i < relcnt; i++)
	{
		bool found = false;
		for (int j = 0; j < attrCnt; j++)
		{
			if (attrList[j].attrValue == NULL)
			{
				return ATTRNOTFOUND;
			}
			if (!strcmp(rels[i].attrName, attrList[j].attrName)) // if names are the same
			{
				memcpy(buf + rels[i].attrOffset,
					   attrList[j].attrValue,
					   rels[i].attrLen);
				found = true;
				break;
			}
		}
		if (!found)
		{
			return ATTRNOTFOUND;
		}
	}
	InsertFileScan *ifs = new InsertFileScan(relation, status);
	if (status)
	{
		return status;
	}

	Record rec;
	rec.data = buf;
	rec.length = reccnt;

	RID rid;
	return ifs->insertRecord(rec, rid);
}
