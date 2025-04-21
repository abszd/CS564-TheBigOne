// Arian Abbaszadeh - 9083678194
// Glenn Hadcock - 9085633288
// Akshaya Somasundaram - 9085417682
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
	AttrDesc *rels;
	status = attrCat->getRelInfo(relation, relcnt, rels);
	if (status)
	{
		return status;
	}
	if (relcnt < attrCnt)
	{
		return ATTRTOOLONG;
	}

	int reclen = 0;
	for (int i = 0; i < attrCnt; i++)
	{
		reclen += rels[i].attrLen;
	}

	char *buf = new char[reclen];
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
				Datatype type = (Datatype)attrList[j].attrType;
				char *val = (char *)attrList[j].attrValue;
				int ival;
				float fval;
				if (type == INTEGER)
				{
					ival = atoi(val);
					val = (char *)&ival;
				}
				else if (type == FLOAT)
				{
					fval = atof(val);
					val = (char *)&fval;
				}
				memcpy(buf + rels[i].attrOffset,
					   val,
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

	InsertFileScan ifs(relation, status);
	if (status)
	{
		return status;
	}

	Record rec;
	rec.data = buf;
	rec.length = reclen;
	RID rid;
	return ifs.insertRecord(rec, rid);
}
