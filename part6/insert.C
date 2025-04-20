// NOT THIS ONE
#include "catalog.h"
#include "query.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
                      const int attrCnt, 
                      const attrInfo attrList[])
{
    Status status;
    int cnt;
    AttrDesc *attrDescArray = nullptr;
    char *outputData = nullptr;
    InsertFileScan *resultRel = nullptr;

    try {
        // Get relation info
        if ((status = attrCat->getRelInfo(relation, cnt, attrDescArray)) != OK) {
            return status;
        }

        // Calculate record length
        int reclen = 0;
        for (int i = 0; i < cnt; i++) {
            reclen += attrDescArray[i].attrLen;
        }

        // Allocate output buffer
        outputData = new char[reclen];
        
        // Build record
        int outputOffset = 0;
        for (int i = 0; i < cnt; ++i) {
            bool found = false;
            for (int j = 0; j < attrCnt; ++j) {
                if (!attrList[j].attrValue) {
                    delete[] outputData;
                    delete[] attrDescArray;
                    return ATTRNOTFOUND;
                }
                
                if (strcmp(attrDescArray[i].attrName, attrList[j].attrName) == 0) {
                    // Handle different types
                    char tempBuffer[attrDescArray[i].attrLen];
                    void *ptr = tempBuffer;
                    
                    switch(attrList[j].attrType) {
                        case STRING:
                            ptr = attrList[j].attrValue;
                            break;
                        case INTEGER: {
                            int value = atoi((char*)attrList[j].attrValue);
                            memcpy(tempBuffer, &value, sizeof(int));
                            break;
                        }
                        case FLOAT: {
                            float value = atof((char*)attrList[j].attrValue);
                            memcpy(tempBuffer, &value, sizeof(float));
                            break;
                        }
                        default:
                            delete[] outputData;
                            delete[] attrDescArray;
                            return ATTRTYPEMISMATCH;
                    }
                    
                    memcpy(outputData + outputOffset, ptr, attrDescArray[i].attrLen);
                    outputOffset += attrDescArray[i].attrLen;
                    found = true;
                    break;
                }
            }
            if (!found) {
                delete[] outputData;
                delete[] attrDescArray;
                return ATTRNOTFOUND;
            }
        }

        // Insert the record
        Record outputRec;
        outputRec.data = outputData;
        outputRec.length = reclen;

        resultRel = new InsertFileScan(relation, status);
        if (status != OK) {
            delete[] outputData;
            delete[] attrDescArray;
            return status;
        }

        RID outRID;
        status = resultRel->insertRecord(outputRec, outRID);

        // Cleanup
        delete[] outputData;
        delete[] attrDescArray;
        delete resultRel;
        
        return status;

    } catch (...) {
        delete[] outputData;
        delete[] attrDescArray;
        delete resultRel;
        throw;
    }
}
