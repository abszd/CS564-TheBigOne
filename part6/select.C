#include "catalog.h"
#include "query.h"

// forward declaration
const Status ScanSelect(const string &result,
						const int projCnt,
						const AttrDesc projNames[],
						const AttrDesc *attrDesc,
						const Operator op,
						const char *filter,
						const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string &result,
					   const int projCnt,
					   const attrInfo projNames[],
					   const attrInfo *attr,
					   const Operator op,
					   const char *attrValue)
{
	// Qu_Select sets up things and then calls ScanSelect to do the actual work
	cout << "Doing QU_Select " << endl;

	Status status = OK;  // Initialize status
	AttrDesc *projAttrs = new AttrDesc[projCnt];
	if (status)
	{
		return status;
	}
	int total = 0;
	for (int i = 0; i < projCnt; i++)
	{
		attrInfo obj = projNames[i];
		status = attrCat->getInfo(obj.relName, obj.attrName, projAttrs[i]);
		if (status)
		{
			return status;
		}
		total += projAttrs[i].attrLen;
	}

	AttrDesc *slctAttr = new AttrDesc;
	status = attrCat->getInfo(attr->relName, attr->attrName, *slctAttr);
	if (status)
	{
		return status;
	}

	return ScanSelect(result, projCnt, projAttrs, slctAttr, op, attrValue, total);
}

const Status ScanSelect(const string &result,
#include "stdio.h"
#include "stdlib.h"
						const int projCnt,
						const AttrDesc projNames[],
						const AttrDesc *attrDesc,
						const Operator op,
						const char *filter,
						const int reclen)
{
	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Status status;
	HeapFileScan *hfs = new HeapFileScan(attrDesc->relName, status);
	if (status != OK) {
		return status;
	}
	float ffltr;
	int ifltr;
	char *fltr;
	fltr = (char *)filter;
	Datatype type = (Datatype)attrDesc->attrType;
	
	if (type == INTEGER) {
		ifltr = atoi(filter);
		fltr = (char *)&ifltr;
	}
	else if (type == FLOAT) {
		ffltr = atof(filter);
		fltr = (char *)&ffltr;
	}
	status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, type, fltr, op);
	if (status != OK) {
		delete hfs;
		return status;
	}
	InsertFileScan *ifs = new InsertFileScan(result, status);
	if (status != OK) {
		delete hfs; //CHECK
		return status;
	}

	RID rid;
	Record rec;
	char *buf = new char[reclen];

	while ((status = hfs->scanNext(rid)) == OK) {
		status = hfs->getRecord(rec);
		if (status != OK) {
			delete[] buf;
			delete hfs;
			delete ifs;
			return status;
		}
		int offset = 0;
		for (int i = 0; i < projCnt; i++) {
			memcpy(buf + offset,
				   (char *)rec.data + projNames[i].attrOffset,
				   projNames[i].attrLen);
			offset += projNames[i].attrLen;
		}

		rec.data = buf;
		rec.length = reclen;

		status = ifs->insertRecord(rec, rid);
		if (status != OK) {
			delete[] buf;
			delete hfs;
			delete ifs;
			return status;
		}
	}

	// Clean up
	delete[] buf;
	delete ifs;
	hfs->endScan();
	delete hfs;

	// Only convert FILEEOF to OK at the end
	return (status == FILEEOF) ? OK : status;
}