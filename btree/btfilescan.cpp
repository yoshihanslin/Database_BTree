#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btfilescan.h"

//-------------------------------------------------------------------
// BTreeFileScan::~BTreeFileScan
//
// Input   : None
// Output  : None
// Purpose : Clean Up the B+ tree scan.
//-------------------------------------------------------------------

BTreeFileScan::~BTreeFileScan ()
{
	if (lowKey) delete lowKey;
	if (highKey) delete highKey;
}


//-------------------------------------------------------------------
// BTreeFileScan::GetNext
//
// Input   : None
// Output  : rid  - record id of the scanned record.
//           keyPtr - and a pointer to it's key value.
// Purpose : Return the next record from the B+-tree index.
// Return  : OK if successful, DONE if no more records to read.
//-------------------------------------------------------------------
Status BTreeFileScan::GetNext (RecordID & rid, char* keyPtr)
{
	BTLeafPage *page;
	RecordID dataRid;
	Status s= OK;
	if (pid == INVALID_PAGE) {
		return DONE;
	}

	char *key = new char[MAX_KEY_SIZE];

	s = MINIBASE_BM->PinPage(pid, (Page *&)page);
	if (firstTime) {
		s = page->GetCurrent(crid, key, dataRid);
		firstTime = false;
	} else {
		s = page->GetNext(crid, key, dataRid);
	}

	if (s == DONE) {
		if (page->GetNextPage() == INVALID_PAGE) {
			s = MINIBASE_BM->UnpinPage(pid, CLEAN);
			delete key;
			return DONE;
		} else {
			PageID oldPid = pid;
			pid = page->GetNextPage();
			s = MINIBASE_BM->UnpinPage(oldPid, CLEAN);
			s = MINIBASE_BM->PinPage(pid, (Page *&)page);
			s = page->GetFirst(crid, key, dataRid);
		}
	}

	if (highKey == NULL || strcmp(key, highKey) <= 0) {
		rid = dataRid;
		strcpy(keyPtr, key);
		s = MINIBASE_BM->UnpinPage(pid, CLEAN);
		delete key;
		return OK;
	} else {
		s = MINIBASE_BM->UnpinPage(pid, CLEAN);
		delete key;
		return DONE;
	}

	return OK;
}
