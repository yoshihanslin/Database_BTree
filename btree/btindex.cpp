#include <string.h>
#include "btindex.h"


//-------------------------------------------------------------------
// BTIndexPage::InsertKey
//
// Input   : key  - pointer to the key value to be inserted.
//           pid - page id associated to that key.
// Output  : rid - record id of the (key, pid) record inserted.
// Purpose : Insert the pair (key, pid) into this index node.
//-------------------------------------------------------------------

Status BTIndexPage::Insert (const char *key,
							PageID pid, RecordID& rid)
{
	KeyDataEntry entry;
	DataType dataType;
	Status s;
	int len;
	
	dataType.pid = pid;
	MakeEntry(&entry, key, INDEX_NODE, dataType, &len);

	s = SortedPage::InsertRecord((char *)&entry, len, rid);
	if (s != OK)
	{
		cerr << "Fail to insert record into SortedPage\n";
		return FAIL;
	}
	
	return OK;
}


//-------------------------------------------------------------------
// BTIndexPage::DeleteKey
//
// Input   : key  - pointer to the key value to be inserted.
// Output  : rid - record id of the (key, pid) record deleted.
// Purpose : Delete the entry associated with key from this index node.
//-------------------------------------------------------------------

Status BTIndexPage::Delete (const char *key, RecordID& rid)
{
	PageID pageNo;
	KeyType currKey;
	Status s;
	
	s = GetFirst (rid, currKey, pageNo);
	assert(s == OK);
	
	while (KeyCmp(key, currKey) > 0)
	{
		s = GetNext (rid, currKey, pageNo);
		if (s != OK)
			break;
	}
	
	if (KeyCmp(key, currKey) != 0)
		rid.slotNo --;
	if (rid.slotNo < 0)
		cout << "Error slotNo!"<< endl;
	else 
		s = SortedPage::DeleteRecord(rid);
	assert(s == OK);
	
	return OK;
}

Status BTIndexPage::DeletePage (PageID pid, bool rightSibling)
{
	PageID pageNo;
	char *currKey = new char[MAX_KEY_SIZE];
	RecordID rid;
	Status s;

	if (GetPrevPage() == pid) {
		s = GetFirst (rid, currKey, pageNo);
		Delete(currKey, rid);
		SetLeftLink(pageNo);
		delete currKey;
		return OK;
	} else {
		s = GetFirst (rid, currKey, pageNo);
		if (pageNo == pid && rightSibling) {
			Delete(currKey, rid);
			delete currKey;
			return OK;
		}

		while (pageNo != pid)
		{
			s = GetNext (rid, currKey, pageNo);
			if (s != OK) {
				pid = INVALID_PAGE;
				break;
			}
		}

		if (pid == INVALID_PAGE) {
			delete currKey;
			return FAIL;
		} else {
			PageID targetPid = pageNo;
			char * targetKey = new char[MAX_KEY_SIZE];
			strcpy(targetKey, currKey);

			s = GetNext(rid, currKey, pageNo);
			char * oldKey = new char[MAX_KEY_SIZE];
			strcpy(oldKey, currKey);

			s = Delete(targetKey, rid);

			AdjustKey(targetKey, oldKey);
			delete currKey;
			return OK;
		}

	}
}


//-------------------------------------------------------------------
// BTIndexPage::GetPageID
//
// Input   : key  - pointer to the key value to be inserted.
// Output  : pid - page id associated with the key.
// Purpose : Search the index page, look for the pid which points to
//           the appropiate child page to search.  This can be used
//           in a B+tree search.
// Return  : Always OK.
//-------------------------------------------------------------------

Status BTIndexPage::GetPageID (const char *key, PageID& pid)
{
	KeyDataEntry *currKey;
	int len;
	
	// A sequential search is implemented here.  You can modify it
	// to a binary search if you like.
	
	for (int i = numOfSlots - 1; i >= 0; i--)
	{
		currKey = (KeyDataEntry *)(data + slots[i].offset);
		
		if (KeyCmp(key, currKey->key) >= 0)
		{
			len = slots[i].length;
			GetKeyData(NULL, (DataType *)&pid, currKey, len, INDEX_NODE);
			return OK;
		}
	}
	
	// If we reach this point, then the page we should follow in our 
	// B+tree search must be the leftmost child of this page.
	
	pid = GetLeftLink();
	return OK;
}


//-------------------------------------------------------------------
// BTIndexPage::GetSibling
//
// Input   : key  - pointer to the key value to be inserted.
// Output  : pid - page id associated with a sibling of that key.
//           left - set to 1 if the sibling is not the leftmost 
//                  entry.
// Purpose : Search the index page, looking for consecutive entries 
//           (k1, pid1), (k2, pid2),  such that k2 >= key >= k1, 
//           and output pid of the _left sibling_ of (k1, pid)
//           Two special cases :
//           -  Output leftmost entry if no such entry is found (left 
//              is set to 0.)
//           -  If (k1, pid1) is the leftmost entry, output the pid
//              of the page on the left of this page.
// Return  : Always OK.
//-------------------------------------------------------------------

Status BTIndexPage::GetSibling (const char *key,
								PageID &pageNo, int &left)
{
	int i;
	
	for (i = numOfSlots - 1; i >= 0; i--)
	{
		GetKeyData(
			NULL, 
			(DataType *)&pageNo,
			(KeyDataEntry *)(data + slots[i].offset),
			slots[i].length,
			(NodeType)type);
		
		if (KeyCmp(key, (char *)(data+slots[i].offset)) >= 0)
		{
			left = 1;
			if (i != 0)
			{
				GetKeyData(
					NULL, 
					(DataType *)&pageNo,
					(KeyDataEntry *)(data + slots[i-1].offset),
					slots[i-1].length,
					(NodeType)type);
				return OK;
			}
			else
			{
				pageNo = GetLeftLink();
				return OK;
			}
		}
	}
	
	left = 0;
	GetKeyData(
		NULL, 
		(DataType *)&pageNo,
		(KeyDataEntry *)(data + slots[0].offset),
		slots[0].length,
		(NodeType)type);
	return OK;
}


//-------------------------------------------------------------------
// BTIndexPage::GetFirst
//
// Input   : None
// Output  : rid - record id of the first entry
//           key - pointer to the key value
//           pid - the page id
// Purpose : get the first pair (key, pid) in the index page and it's
//           rid.
// Return  : OK always.
//-------------------------------------------------------------------

Status BTIndexPage::GetFirst (RecordID& rid, char *key, PageID& pageNo)
{
	if (numOfSlots == 0) 
	{
		pageNo = INVALID_PAGE;
		return DONE;
	}
	
	rid.pageNo = pid;
	rid.slotNo = 0;
	
	GetKeyData(key, 
		(DataType *)&pageNo, 
		(KeyDataEntry *)(data+slots[0].offset),
		slots[0].length, (NodeType)type);
	
	return OK;
}

Status BTIndexPage::GetLast (RecordID& rid, char* key, PageID & pageNo)
{
	if (numOfSlots == 0) 
	{
		pageNo = INVALID_PAGE;
		return DONE;
	}

	rid.pageNo = pid;
	rid.slotNo = numOfSlots - 1;

	GetKeyData(key, 
		(DataType *)&pageNo,
		(KeyDataEntry *)(data+slots[numOfSlots - 1].offset),
		slots[numOfSlots - 1].length, (NodeType)type);
	
	return OK;
}

//-------------------------------------------------------------------
// BTIndexPage::GetNext
//
// Input   : rid - record id of the current entry
// Output  : rid - record id of the next entry
//           key - pointer to the key value
//           pid - the page id
// Purpose : get the next pair (key, pid) in the index page and it's
//           rid.
// Return  : OK if there is a next record, DONE if no more.
//-------------------------------------------------------------------

Status BTIndexPage::GetNext (RecordID& rid, char *key, PageID & pageNo)
{
	rid.slotNo++;
	
	if (rid.slotNo >= numOfSlots)
	{
		pageNo = INVALID_PAGE;
		return DONE;
	}
	
	GetKeyData(key,
		(DataType *)&pageNo,
		(KeyDataEntry *)(data+slots[rid.slotNo].offset),
		slots[rid.slotNo].length,
		(NodeType)type);
	
	return OK;
}



//-------------------------------------------------------------------
// BTIndexPage::FindKey
//
// Input   : key - pointer to the key to find
//           entry - pointer to mem location to copy result.
// Output  : None
// Purpose : Look for (k1, p1) (k2, p2) such that k1 <= key < k2, 
//           and copy k1 into entry.
// Precond : entry has enough space to hold k1.
// Return  : OK if successful, FAIL if cannot find such k1.
//-------------------------------------------------------------------

Status BTIndexPage::FindKey(char *key, char *entry)
{
	for (int i = numOfSlots - 1; i >= 0; i--)
	{
		if (KeyCmp(key, (char *)(data+slots[i].offset)) >= 0)
		{
			memcpy(entry, data+slots[i].offset, GetKeyLength(key));
			return OK;
		}
	}
	return FAIL;
}

Status BTIndexPage::FindPage(const char *key, PageID& pageNo, bool& leftMost)
{
	for (int i = numOfSlots - 1; i >= 0; i--)
	{
		if (KeyCmp(key, (char *)(data+slots[i].offset)) >= 0)
		{
			GetKeyData(NULL,
				(DataType *)&pageNo,
				(KeyDataEntry *)(data+slots[i].offset),
				slots[i].length,
				(NodeType)type);
			leftMost = false;
			return OK;
		}
	}

	leftMost = true;
	pageNo = GetLeftLink();
	return OK;
}

Status BTIndexPage::FindKeyWithPage(PageID targetPid, char *key, bool& leftMost) {
	if (GetLeftLink() == targetPid) {
		leftMost = true;
		return OK;
	}

	for (int i = numOfSlots - 1; i >= 0; i--)
	{
		PageID pageNo;
		GetKeyData(key,
			(DataType *)&pageNo,
			(KeyDataEntry *)(data+slots[i].offset),
			slots[i].length,
			(NodeType)type);
		if (targetPid == pageNo) {
			leftMost = false;
			return OK;
		}
	}

	return FAIL;
}

Status BTIndexPage::FindSiblingForChild(PageID targetPid, PageID& siblingPid, bool& rightSibling)
{

	if (GetLeftLink() == targetPid) {
		rightSibling = true;
		GetKeyData(NULL,
			(DataType *)&siblingPid,
			(KeyDataEntry *)(data+slots[0].offset),
			slots[0].length,
			(NodeType)type);
	}

	for (int i = numOfSlots - 1; i >= 0; i--)
	{
		PageID pageNo;
		GetKeyData(NULL,
			(DataType *)&pageNo,
			(KeyDataEntry *)(data+slots[i].offset),
			slots[i].length,
			(NodeType)type);
		if (targetPid == pageNo) {
			rightSibling = false;
			if (i == 0) {
				siblingPid = GetLeftLink();
			} else {
				GetKeyData(NULL,
					(DataType *)&siblingPid,
					(KeyDataEntry *)(data+slots[i-1].offset),
					slots[i-1].length,
					(NodeType)type);
			}
			return OK;
		}
	}

	return FAIL;
}

//-------------------------------------------------------------------
// BTIndexPage::GetLeftLink
//
// Input   : None
// Output  : None
// Purpose : Return the page id of the page at the left of this page.
// Return  : The page id of the page at the left of this page.
//-------------------------------------------------------------------

PageID BTIndexPage::GetLeftLink ()
{
	return GetPrevPage();
}


//-------------------------------------------------------------------
// BTIndexPage::SetLeftLink
//
// Input   : pid - new left link
// Output  : None
// Purpose : Set the page id of the page at the left of this page.
// Return  : None
//-------------------------------------------------------------------

void BTIndexPage::SetLeftLink (PageID pid)
{
	SetPrevPage(pid);
}


Status BTIndexPage::AdjustKey (const char *newKey, const char *oldKey)
{
    for (int i = numOfSlots -1; i >= 0; i--) {
        if (KeyCmp(oldKey, (char*)(data+slots[i].offset)) >= 0) {
			memcpy(data+slots[i].offset, newKey, GetKeyLength(newKey)); 
			return OK;
        }
    }
    return FAIL;
}
