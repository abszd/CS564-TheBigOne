// Arian Abbaszadeh - 9083678194
// Glenn Hadcock - 9085633288
// Akshaya Somasundaram - 9085417682
#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string &relation,
					   const string &attrName,
					   const Operator op,
					   const Datatype type,
					   const char *attrValue)
{
	// part 6
	Status status;
	HeapFileScan hfs(relation, status);

	if (attrName.empty())
	{
		status = hfs.startScan(0, 0, STRING, NULL, EQ);
	}
	else
	{
		AttrDesc attrrec;
		status = attrCat->getInfo(relation, attrName, attrrec);

		char *fltr = (char *)attrValue;
		int ifltr;
		float ffltr;
		if (type == INTEGER)
		{
			ifltr = atoi(attrValue);
			fltr = (char *)&ifltr;
		}
		else if (type == FLOAT)
		{
			ffltr = atof(attrValue);
			fltr = (char *)&ffltr;
		}
		status = hfs.startScan(attrrec.attrOffset, attrrec.attrLen,
							   type, fltr, op);
	}

	RID rid;
	while (!(status = hfs.scanNext(rid)))
	{
		status = hfs.deleteRecord();
		if (status)
		{
			return status;
		}
	}
	if (status == FILEEOF)
	{
		status = OK;
	}
	hfs.endScan();
	return status;
}
