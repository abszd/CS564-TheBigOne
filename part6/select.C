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

	// Allocate these on heap to ensure they stay valid
	float* ffltr = nullptr;
	int* ifltr = nullptr;
	const char* fltr = nullptr;
	
	Datatype type = (Datatype)attrDesc->attrType;
	
	if (filter != nullptr) {
		switch(type) {
			case INTEGER:
				ifltr = new int(atoi(filter));
				fltr = (char*)ifltr;
				break;
			case FLOAT:
				ffltr = new float(atof(filter));
				fltr = (char*)ffltr;
				break;
			case STRING:
				fltr = filter;  // For string type, use the original filter
				break;
		}
	}

	status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, type, fltr, op);
	if (status != OK) {
		delete ffltr;
		delete ifltr;
		delete hfs;
		return status;
	}

	InsertFileScan *ifs = new InsertFileScan(result, status);
	if (status != OK) {
		delete ffltr;
		delete ifltr;
		delete hfs;
		return status;
	}

	RID rid;
	Record rec;
	char *buf = new char[reclen];

	while ((status = hfs->scanNext(rid)) == OK) {
		status = hfs->getRecord(rec);
		if (status != OK) break;

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
		if (status != OK) break;
	}

	// Clean up
	delete[] buf;
	delete ifs;
	delete ffltr;
	delete ifltr;
	hfs->endScan();
	delete hfs;

	return (status == FILEEOF) ? OK : status;
}