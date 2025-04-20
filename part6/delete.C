// NOT THIS ONE
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
	Status status;
	HeapFileScan *hfs = nullptr;

	try {
		// Create heap file scan
		hfs = new HeapFileScan(relation, status);
		if (status != OK) {
			return status;
		}

		if (attrName.empty()) {
			// No condition given - scan all records
			status = hfs->startScan(0, 0, STRING, nullptr, op);
		} else {
			// Get attribute info for filtering
			AttrDesc attrDesc;
			status = attrCat->getInfo(relation, attrName, attrDesc);
			if (status != OK) {
				delete hfs;
				return status;
			}

			// Handle type conversion
			union {
				int intVal;
				float floatVal;
			} convertedValue;
			void* filterPtr = nullptr;

			switch(type) {
				case STRING:
					filterPtr = (void*)attrValue;
					break;
				case INTEGER:
					if (attrValue) {
						convertedValue.intVal = atoi(attrValue);
						filterPtr = &convertedValue.intVal;
					}
					break;
				case FLOAT:
					if (attrValue) {
						convertedValue.floatVal = atof(attrValue);
						filterPtr = &convertedValue.floatVal;
					}
					break;
				default:
					delete hfs;
					return ATTRTYPEMISMATCH;
			}

			status = hfs->startScan(attrDesc.attrOffset,
								  attrDesc.attrLen,
								  type,
								  (char*)filterPtr,
								  op);
		}

		if (status != OK) {
			delete hfs;
			return status;
		}

		// Delete matching records
		RID rid;
		while ((status = hfs->scanNext(rid)) == OK) {
			status = hfs->deleteRecord();
			if (status != OK) {
				break;
			}
		}

		// Convert FILEEOF to OK
		if (status == FILEEOF) {
			status = OK;
		}

		// Cleanup
		hfs->endScan();
		delete hfs;
		return status;

	} catch (...) {
		if (hfs) {
			hfs->endScan();
			delete hfs;
		}
		throw;
	}
}
