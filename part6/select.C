// #include "catalog.h"
// #include "query.h"

// // forward declaration
// const Status ScanSelect(const string &result,
// 						const int projCnt,
// 						const AttrDesc projNames[],
// 						const AttrDesc *attrDesc,
// 						const Operator op,
// 						const char *filter,
// 						const int reclen);

// /*
//  * Selects records from the specified relation.
//  *
//  * Returns:
//  * 	OK on success
//  * 	an error code otherwise
//  */

// const Status QU_Select(const string &result,
// 					   const int projCnt,
// 					   const attrInfo projNames[],
// 					   const attrInfo *attr,
// 					   const Operator op,
// 					   const char *attrValue)
// {
// 	// Qu_Select sets up things and then calls ScanSelect to do the actual work
// 	cout << "Doing QU_Select " << endl;

// 	Status status;
// 	AttrDesc *projAttrs = new AttrDesc[projCnt];
// 	if (status)
// 	{
// 		return status;
// 	}
// 	int total = 0;
// 	for (int i = 0; i < projCnt; i++)
// 	{
// 		attrInfo obj = projNames[i];
// 		status = attrCat->getInfo(obj.relName, obj.attrName, projAttrs[i]);
// 		if (status)
// 		{
// 			return status;
// 		}
// 		total += projAttrs[i].attrLen;
// 	}

// 	AttrDesc *slctAttr = new AttrDesc;
// 	status = attrCat->getInfo(attr->relName, attr->attrName, *slctAttr);
// 	if (status)
// 	{
// 		return status;
// 	}

// 	return ScanSelect(result, projCnt, projAttrs, slctAttr, op, attrValue, total);
// }

// const Status ScanSelect(const string &result,
// #include "stdio.h"
// #include "stdlib.h"
// 						const int projCnt,
// 						const AttrDesc projNames[],
// 						const AttrDesc *attrDesc,
// 						const Operator op,
// 						const char *filter,
// 						const int reclen)
// {
// 	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
// 	Status status;
// 	HeapFileScan *hfs = new HeapFileScan(attrDesc->relName, status);
// 	if (status)
// 	{
// 		return status;
// 	}
// 	float ffltr;
// 	int ifltr;
// 	char *fltr;
// 	fltr = (char *)filter;
// 	Datatype type = (Datatype)attrDesc->attrType;
// 	if (type == INTEGER)
// 	{
// 		ifltr = atoi(filter);
// 		fltr = (char *)&ifltr;
// 	}
// 	else if (type == FLOAT)
// 	{
// 		ffltr = atof(filter);
// 		fltr = (char *)&ffltr;
// 	}
// 	status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, type, fltr, op);
// 	if (status)
// 	{
// 		return status;
// 	}
// 	InsertFileScan *ifs = new InsertFileScan(result, status);
// 	if (status)
// 	{
// 		return status;
// 	}

// 	RID rid;
// 	Record rec;
// 	char *buf = new char[reclen];
// 	while (!(status = hfs->scanNext(rid)))
// 	{
// 		status = hfs->getRecord(rec);
// 		if (status)
// 		{
// 			return status;
// 		}
// 		int offset = 0;
// 		for (int i = 0; i < projCnt; i++)
// 		{
// 			memcpy(buf + offset,
// 				   (char *)rec.data + projNames[i].attrOffset,
// 				   projNames[i].attrLen);
// 			offset += projNames[i].attrLen;
// 		}

// 		rec.data = buf;
// 		rec.length = reclen;

// 		status = ifs->insertRecord(rec, rid);
// 		if (status)
// 		{
// 			return status;
// 		}
// 	}
// 	if (status == FILEEOF)
// 	{
// 		status = OK;
// 	}

// 	hfs->endScan();
// 	return status;
// }

#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result,
			const int projCnt,
			const AttrDesc projNames[],
			const AttrDesc *attrDesc,
			const Operator op,
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation using a
 * HeapFileScan. The project list is defined by 
 * the parameters projCnt and projNames and is done on-the-fly.
 * <i>The search value is always supplied as the character string attrValue.
 * If attr is NULL, an unconditional scan of the input table should be performed.</i>
 *
 * @param result
 * @param projCnt
 * @param projNames[]
 * @param attr
 * @param op
 * @param attrValue
 * @return: OK on success
 * an error code otherwise
 */
const Status QU_Select(const string & result,
        const int projCnt,
        const attrInfo projNames[],
        const attrInfo *attr,
        const Operator op,
        const char *attrValue)
{
    Status status;
    AttrDesc attrDesc;
    AttrDesc* projNamesDesc;
    int reclen;
    Operator myOp;
    const char* filter;

    reclen = 0;
    //an array of attrDesc to hold all the descriptions for the projection
    projNamesDesc = new AttrDesc[projCnt];
    //tabulate the total length of the projection record
    for (int i = 0; i < projCnt; i++) {
        Status status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projNamesDesc[i]);
        reclen += projNamesDesc[i].attrLen;
        if (status != OK) { return status; }
    }
    //implementation of select operation
    if (attr != NULL) { //SELECT ALL
        //get info for attr
        //e.g. attr op attrvalue
        status = attrCat->getInfo(string(attr->relName), string(attr->attrName), attrDesc);

        int tmpInt;
        float tmpFloat;
        //convert to proper data type
        switch (attr->attrType) {
            case INTEGER:
                tmpInt = atoi(attrValue);
                filter = (char*)&tmpInt;
                break;
            case FLOAT:
                tmpFloat = atof(attrValue);
                filter = (char*)&tmpFloat;
                break;
            case STRING:
                filter = attrValue;
                break;
        }
        myOp = op;
    //if null than there is no where clause
    //select all tuple for projection attributes
    } else {
        //default attrDesc for an unconditional scan
        strcpy(attrDesc.relName, projNames[0].relName);
        strcpy(attrDesc.attrName, projNames[0].attrName);
        attrDesc.attrOffset = 0;
        attrDesc.attrLen = 0;
        attrDesc.attrType = STRING;
        filter = NULL;
        myOp = EQ;
    }
    //now that the book keeping is done call ScanSelect to actually build the result table
    status = ScanSelect(result, projCnt, projNamesDesc, &attrDesc, myOp, filter, reclen);
    if (status != OK) { return status; }

    return OK;
}


#include "stdio.h"
#include "stdlib.h"
/**
 * This function scans the relation for tuples
 * that match the filter predicate and copy these
 * tuples into a relation named result.
 *
 * @param result
 * @param projCnt
 * @param projNames[]
 * @param attrDesc
 * @param op
 * @param filter
 * @param reclen
 * @return: OK on success
 * an error code otherwise
**/
const Status ScanSelect(const string & result,
        const int projCnt,
        const AttrDesc projNames[],
        const AttrDesc *attrDesc,
        const Operator op,
        const char *filter,
        const int reclen)
{
    Status status;
    //used to keep track of how many total tuples are selected
    int resultTupCnt = 0;

    // open the result table
    InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }
    //initialize pointer a location of size reclen
    char outputData[reclen];
    //the record to be copied to later
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;
    // start scan on outer table
    HeapFileScan relScan(attrDesc->relName, status);
    if (status != OK) { return status; }

    status = relScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, filter, op);
    if (status != OK) { return status; }

    // scan outer table
    RID relRID;
    Record relRec;
    while (relScan.scanNext(relRID) == OK) {
        status = relScan.getRecord(relRec);
        ASSERT(status == OK);

        // we have a match, copy data into the output record
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(outputData + outputOffset, (char *)relRec.data + projNames[i].attrOffset, projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
       
        } // end copy attrs

        // add the new record to the output relation
        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
        ASSERT(status == OK);
        resultTupCnt++;
    }
    //before returning print out the total number of tuples in this relation
    printf("selected %d result tuples \n", resultTupCnt);
    return OK;
}