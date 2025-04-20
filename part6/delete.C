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
	AttrDesc attrrec;

	HeapFileScan *hfs = new HeapFileScan(relation, status);
	status = attrCat->getInfo(relation, attrName, attrrec);
	if (status)
	{
		return status;
	}

	float ffltr;
	int ifltr;
	char *fltr;
	fltr = (char *)attrValue;
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
	hfs->startScan(attrrec.attrOffset, attrrec.attrLen, type, fltr, op);

	Record rec;
	RID rid;
	while (!(status = hfs->scanNext(rid)))
	{
		status = hfs->deleteRecord();
		if (status)
		{
			return status;
		}
	}
	if (status == FILEEOF)
	{
		status = OK;
	}
	hfs->endScan();
	return status;
}
