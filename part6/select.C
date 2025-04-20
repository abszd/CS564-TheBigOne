// NOT THIS ONE
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
	cout << "Doing QU_Select " << endl;
	Status status = OK;

	// Input validation
	if (projCnt <= 0 || !projNames) {
		return BADCATPARM;
	}

	// Use vector for automatic cleanup
	vector<AttrDesc> projDescs(projCnt);
	
	// Get projection attributes
	for (int i = 0; i < projCnt; i++) {
		status = attrCat->getInfo(projNames[i].relName,
								 projNames[i].attrName,
								 projDescs[i]);
		if (status != OK) {
			return status;
		}
	}

	// Handle filtering attribute
	AttrDesc attrDesc;
	if (attr != nullptr) {
		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
		if (status != OK) {
			return status;
		}
	}

	// Calculate output record length
	int reclen = 0;
	for (int i = 0; i < projCnt; i++) {
		reclen += projDescs[i].attrLen;
	}

	// Call ScanSelect with appropriate parameters
	if (attr == nullptr) {
		return ScanSelect(result, projCnt, projDescs.data(), 
						 &projDescs[0], EQ, nullptr, reclen);
	}
	return ScanSelect(result, projCnt, projDescs.data(), 
					 &attrDesc, op, attrValue, reclen);
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
	int resultTupCnt = 0;

	// Use unique_ptr for automatic cleanup
	unique_ptr<char[]> outputData(new char[reclen]);
	
	// Create result relation
	InsertFileScan resultRel(result, status);
	if (status != OK) return status;

	// Start scan
	HeapFileScan scan(attrDesc->relName, status);
	if (status != OK) return status;

	// Handle filtering
	if (filter == nullptr) {
		status = scan.startScan(attrDesc->attrOffset, 
							   attrDesc->attrLen, 
							   STRING, nullptr, op);
	} else {
		// Use union to safely handle different types
		union {
			int intVal;
			float floatVal;
			char *strVal;
		} convertedFilter;

		Datatype type;
		void *filterPtr;

		switch(attrDesc->attrType) {
			case STRING:
				type = STRING;
				filterPtr = (void*)filter;
				break;
			case INTEGER:
				type = INTEGER;
				convertedFilter.intVal = atoi(filter);
				filterPtr = &convertedFilter.intVal;
				break;
			case FLOAT:
				type = FLOAT;
				convertedFilter.floatVal = atof(filter);
				filterPtr = &convertedFilter.floatVal;
				break;
			default:
				return ATTRTYPEMISMATCH;
		}

		status = scan.startScan(attrDesc->attrOffset, 
							   attrDesc->attrLen, 
							   type, 
							   (char*)filterPtr, 
							   op);
	}
	if (status != OK) return status;

	// Set up output record
	Record outputRec;
	outputRec.data = outputData.get();
	outputRec.length = reclen;

	// Process matching records
	RID rid;
	while (scan.scanNext(rid) == OK) {
		Record rec;
		status = scan.getRecord(rec);
		if (status != OK) continue;

		// Copy projected attributes
		int outputOffset = 0;
		for (int i = 0; i < projCnt; i++) {
			memcpy(outputData.get() + outputOffset,
				   (char*)rec.data + projNames[i].attrOffset,
				   projNames[i].attrLen);
			outputOffset += projNames[i].attrLen;
		}

		// Insert result record
		RID outRID;
		status = resultRel.insertRecord(outputRec, outRID);
		if (status != OK) return status;
		resultTupCnt++;
	}

	printf("Select produced %d result tuples\n", resultTupCnt);
	return OK;
}