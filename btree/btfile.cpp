#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btfilescan.h"

//-------------------------------------------------------------------
// BTreeFile::BTreeFile
//
// Input   : filename - filename of an index.
// Output  : returnStatus - status of execution of constructor.
//           OK if successful, FAIL otherwise.
// Purpose : Open the index file, if it exists.
//			 Otherwise, create a new index, with the specified
//           filename. You can use
//                MINIBASE_DB->GetFileEntry(filename, headerID);
//           to retrieve an existing index file, and
//                MINIBASE_DB->AddFileEntry(filename, headerID);
//           to create a new one. You should pin the header page
//           once you have read or created it. You will use the header
//           page to find the root node.
//-------------------------------------------------------------------
BTreeFile::BTreeFile (Status& returnStatus, const char *filename) {
	// Save the name of the file so we delete appropriately
	// when DestroyFile is called.
	dbname = strcpy(new char[strlen(filename) + 1], filename);

	Status stat = MINIBASE_DB->GetFileEntry(filename, headerID);
	Page *_headerPage;
	returnStatus = OK;

	// File does not exist, so we should create a new index file.
	if (stat == FAIL) {
		//Allocate a new header page.
		stat = MINIBASE_BM->NewPage(headerID, _headerPage, 1);

		if (stat != OK) {
			std::cerr << "Error allocating header page." << std::endl;
			headerID = INVALID_PAGE;
			header = NULL;
			returnStatus = FAIL;
			return;
		}

		header = (BTreeHeaderPage *)(_headerPage);
		header->Init(headerID);
		stat = MINIBASE_DB->AddFileEntry(filename, headerID);

		if (stat != OK) {
			std::cerr << "Error creating file" << std::endl;
			headerID = INVALID_PAGE;
			header = NULL;
			returnStatus = FAIL;
			return;
		}
	} else {
		stat = MINIBASE_BM->PinPage(headerID, _headerPage);

		if (stat != OK) {
			std::cerr << "Error pinning existing header page" << std::endl;
			headerID = INVALID_PAGE;
			header = NULL;
			returnStatus = FAIL;
		}

		header = (BTreeHeaderPage *) _headerPage;
	}
}


//-------------------------------------------------------------------
// BTreeFile::~BTreeFile
//
// Input   : None
// Return  : None
// Output  : None
// Purpose : Free memory and clean Up. You should be sure to
//           unpin the header page if it has not been unpinned
//           in DestroyFile.
//-------------------------------------------------------------------
BTreeFile::~BTreeFile ()
{
    delete [] dbname;
	
    if (headerID != INVALID_PAGE) 
	{
		Status st = MINIBASE_BM->UnpinPage (headerID, CLEAN);
		if (st != OK)
		{
		cerr << "ERROR : Cannot unpin page " << headerID << " in BTreeFile::~BTreeFile" << endl;
		}
    }
}


//-------------------------------------------------------------------
// BTreeFile::DestroyFile
//
// Input   : None
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Free all pages and delete the entire index file. Once you have
//           freed all the pages, you can use MINIBASE_DB->DeleteFileEntry (dbname)
//           to delete the database file.
//-------------------------------------------------------------------
Status BTreeFile::DestroyFile ()
{
	//TODO: add your code here
	//header page is a heap page, not a sorted page
	//parent-> child: heapPage->SortedPage->(BTIndexPage and BTLeafPage)
	//typedef enum {	INDEX_NODE,	LEAF_NODE	} NodeType;
	Status s= OK; PageID rootPageID; SortedPage* rootPage; NodeType nType;

//	DumpStatistics();
//	_PrintTree(header->GetRootPageID());
/*	//for: test 5: Insertion
	_PrintTree(5);
	_PrintTree(4);
	_PrintTree(78);
	_PrintTree(77);
	
	_PrintTree(152);
	_PrintTree(189);
	_PrintTree(115);
	//_PrintTree(112);
	//cin.get();
*/
	if (header == NULL || headerID == INVALID_PAGE) {
		headerID = INVALID_PAGE;
		header = NULL;
		s = MINIBASE_DB->DeleteFileEntry(dbname);
		return s;
	}

	if ( header->GetRootPageID() != INVALID_PAGE){
		//Get the root page (MINIBASE_BM->PinPage)
		rootPageID= header->GetRootPageID();
		PIN(rootPageID,(Page *&)rootPage);
		//if root page is leaf page {
		if( rootPage->GetType()==LEAF_NODE) {
			//Free root page.
			FREEPAGE(rootPageID); //also unpins page. if FAILED, will return FAIL for Status BTreeFile::DestroyFile ()
		}
		else {//Recursively free all other pages. //root page is an index page
			s=DestroyNode(rootPageID);
			MINIBASE_BM->FreePage(rootPageID); // not return any value for Status BTreeFile::DestroyFile ()
		}
		//UNPIN(rootPageID,CLEAN); //freePage unpins page in the fn
	}

	FREEPAGE(headerID);
	headerID = INVALID_PAGE;
	header = NULL;
	//Remove the file entry in the database
	s = MINIBASE_DB->DeleteFileEntry(dbname);
	return s;
}

Status BTreeFile::DestroyNode(PageID pageID) {
	SortedPage *page; NodeType type; 
	RecordID rid; char* key; PageID keyPid; PageID keyPid2; PageID keyPid3; 
	Status s= OK; Status stat= OK;
	//Status PinPage( PageID pid, Page*& page, bool emptyPage=false );
	PIN (pageID, page); //returns SortedPage *page
	type = page->GetType();

	//base case: node is a leaf node
	if (type==LEAF_NODE) {
		FREEPAGE(pageID); //freePage unpins page in the fn
		return OK;
	}

	//recursive case: //node is an index node
	if (type==INDEX_NODE){
	//	Status GetFirst (RecordID& rid, char *key, PageID & pageNo);
	//Status GetNext (RecordID& rid, char *key, PageID & pageNo);
			//Return  : OK if there is a next record, DONE if no more.
		keyPid=((BTIndexPage*&)page)->GetLeftLink(); 
		s=DestroyNode(keyPid);
		FREEPAGE(keyPid); //freePage unpins page in the fn
		key=new char[MAX_KEY_SIZE];
		stat=((BTIndexPage*&)page)->GetFirst (rid, key, keyPid2); //returns rid, key, keypPid
		//<pid1, key1, pid2, key2, ..., keyk, pidk+1>
		//leftlink or leftmost child page stores pid1 while (keyi, pidi+1) are stored as records of type IndexEntry in BTIndexPage
		if (stat==OK) {
			s=DestroyNode(keyPid2); //destroys GetFirst keyPid
			FREEPAGE(keyPid2);
			stat= ((BTIndexPage*&)page)->GetNext(rid, key, keyPid3); //input: rid; returns rid, key, keypPid
			//OK if there's next record, DONE if no more. DONE returns last valid key value
			while(stat!=DONE) {
				s=DestroyNode(keyPid3); //destroys GetNext keyPid
				FREEPAGE(keyPid3);
				stat= ((BTIndexPage*&)page)->GetNext(rid, key, keyPid3);
			}
		}
		UNPIN(pageID, CLEAN);
		delete key;
		return s;
	}
	return s;
}

//-------------------------------------------------------------------
// BTreeFile::Insert
//
// Input   : key - pointer to the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.  
// Note    : If the root didn't exist, create it.
//-------------------------------------------------------------------
Status BTreeFile::Insert (const char *key, const RecordID rid)
{
	PageID rootPageID;
	SortedPage* rootPage; 
	BTLeafPage *leafPage; PageID leafPageID; RecordID leafRid; NodeType type;

	Status res;
	Status s;

	if (header == NULL || headerID == INVALID_PAGE) return FAIL;
	rootPageID = header->GetRootPageID();

	if (rootPageID==INVALID_PAGE) { //If the root didn't exist, create it.
		//BTreeFile with only one BTLeafPage: the single leaf page is also the root.
		//The leaf and index nodes are implemented by the classes BTLeafPage and BTIndexPage, respectively; both subclasses of SortedPage
		//Status NewPage( PageID& pid, Page*& firstpage,int howmany=1 ); 
		// NewPage(): Allocate howMany number of pages & pin firstpage into buffer. Returns firstpage, pointer to page pinned in buffer           
		s = MINIBASE_BM->NewPage(rootPageID, (Page *&)rootPage, 1);
		if (s != OK) {
			std::cerr << "Error allocating root page." << std::endl;
			return FAIL;
		}
		leafPage = (BTLeafPage* &)rootPage;
		leafPageID = rootPageID;
		leafPage->Init(leafPageID);
		leafPage->SetType(LEAF_NODE);
		header->SetRootPageID(rootPageID);

		leafPage->Insert(key, rid, leafRid); //return leafRid: record id of inserted pair (key, dataRid)
		UNPIN(leafPageID, DIRTY);
		return OK;
	}

	if (rootPageID!=INVALID_PAGE) {
		PIN(rootPageID, (Page *&)rootPage); //returns page - a pointer to a page pinned in buffer pool.

		type= rootPage->GetType();

		if(type==LEAF_NODE){
			leafPage=(BTLeafPage* &)rootPage;
			leafPageID= rootPageID;
			if (leafPage->AvailableSpace() >= GetKeyDataLength(key, type)) {
				res = leafPage->Insert(key, rid, leafRid);
			} else {
				PageID newRootPageID;
				res = Split1LeafNode(leafPageID,newRootPageID, key, rid);
				if(res == OK) {
					header->SetRootPageID(newRootPageID);
				}
			}
		} else {
			char * newKey=new char[MAX_KEY_SIZE];
			PageID newPid;
			Status res = _Insert(rootPageID, key, rid, newPid, newKey);
			if (newPid != INVALID_PAGE) {
				PageID newRootPageID;
				BTIndexPage* newRootPage;
				RecordID newRid;
				NEWPAGE(newRootPageID, (Page *&)newRootPage, 1);
				newRootPage->SetType(INDEX_NODE);
				newRootPage->Init(newRootPageID);

				newRootPage->SetLeftLink(rootPageID);
				newRootPage->Insert(newKey, newPid, newRid); 
				header->SetRootPageID(newRootPageID);

				//PrintTree(newRootPageID, SINGLE);

				UNPIN(newRootPageID, DIRTY);	
			}
		}
		UNPIN(rootPageID, DIRTY);
	}
}

//splits leafPageID into 1 root page, 2 leaf pages; returns newRootPageID
Status BTreeFile::Split1LeafNode(PageID leafPageID, PageID& newRootPageID, const char *newKey, const RecordID newRid) {
	BTIndexPage* newRootPage; BTLeafPage *leafPage; BTLeafPage* newLeafPage;
	PageID newLeafPageID; //PageID newRootPageID;
	 
	int originalLeafAvailableSpace=0; int originalLeafUsedSpace=0;
	int newLeafAvailableSpace=0;
	RecordID rid; char* key; RecordID keyRecordID; Status s=OK;
	RecordID validrid; char* validkey; RecordID validkeyRecordID;

	//Status NewPage( PageID& pid, Page*& firstpage,int howmany=1); //pin firstpage.Returns firstpage, pointer to page pinned in buffer
	NEWPAGE(newRootPageID, (Page *&)newRootPage, 1);
		newRootPage->SetType(INDEX_NODE);
		newRootPage->Init(newRootPageID);
	NEWPAGE(newLeafPageID, (Page *&)newLeafPage, 1);
		newLeafPage->SetType(LEAF_NODE);
		newLeafPage->Init(newLeafPageID);
	PIN(leafPageID, (Page *&)leafPage);

	newLeafPage->SetType(LEAF_NODE);	newLeafPage->Init(newLeafPageID);
	newRootPage->SetType(INDEX_NODE);	newRootPage->Init(newRootPageID);
	originalLeafAvailableSpace= leafPage->AvailableSpace();
	originalLeafUsedSpace= HEAPPAGE_DATA_SIZE-originalLeafAvailableSpace;
	newLeafAvailableSpace=newLeafPage->AvailableSpace();

	//Status BTLeafPage::GetFirst (RecordID& rid, char* key, RecordID & dataRid)
	//get the first pair (key, dataRid) in the leaf page and its rid.
	key=new char[MAX_KEY_SIZE];

	leafPage->GetFirst(rid,key,keyRecordID); //return rid,key,keyRecordID
	s= newLeafPage->Insert(key,keyRecordID,rid);//Output: rid - record id of the inserted pair (key, dataRid)
	if(s!=OK) newLeafPage->Delete(key,keyRecordID);
	if(s==OK) leafPage->Delete(key,keyRecordID);

	s=leafPage->GetFirst(rid,key,keyRecordID); //OK if there's next record, DONE if no more. DONE returns last valid key value
	while((newLeafAvailableSpace=newLeafPage->AvailableSpace())>(originalLeafAvailableSpace=leafPage->AvailableSpace())
		&& s!=DONE){
			s= newLeafPage->Insert(key,keyRecordID,rid);//Output: rid - record id of the inserted pair (key, dataRid)
			validrid=rid; validkeyRecordID= keyRecordID; //validkey=key;
			if(s!=OK) newLeafPage->Delete(key,keyRecordID);
			if(s==OK) leafPage->Delete(key,keyRecordID);
			s= leafPage->GetFirst(rid,key,keyRecordID); //OK if there's next record, DONE if no more. DONE returns last valid key value
	} //last key inserted into new leaf is the key in middle of original leaf node

	s = leafPage->GetFirst(rid, key, keyRecordID);
	if (KeyCmp(newKey, key) >= 0) {
		leafPage->Insert(newKey, newRid, rid);
	} else {
		newLeafPage->Insert(newKey, newRid, rid);
	}
	s = leafPage->GetFirst(rid, key, keyRecordID);

	//pointer to left of first index in root node
	newRootPage->SetLeftLink(newLeafPageID);
	//pointer to right of first index in root node

	newRootPage->Insert(key, leafPageID, rid); //INDEX node insert: return rid of the (key, pid) record inserted
	newLeafPage->SetNextPage(leafPageID);
	leafPage->SetPrevPage(newLeafPageID);

	UNPIN(leafPageID, DIRTY);
	UNPIN(newRootPageID, DIRTY);
	UNPIN(newLeafPageID, DIRTY);


	return OK;
}

Status BTreeFile::_Insert(PageID nodePid, const char *targetKey, 
const RecordID targetId, PageID& newPid, char *newKey)
{
	RecordID tempRid;
	Status res;
	Status s;

	SortedPage *nodePage;
	PIN(nodePid, (Page *&)nodePage);
	NodeType nodeType = nodePage->GetType();

	if (nodeType == LEAF_NODE) {
		BTLeafPage *leafPage;
		RecordID leafRid;
		leafPage=(BTLeafPage* &)nodePage;

		if (leafPage->AvailableSpace() >= GetKeyDataLength(targetKey, nodeType)) {
			res = leafPage->Insert(targetKey, targetId, leafRid);
			newPid = INVALID_PAGE;
			UNPIN(nodePid, DIRTY);
			return res;
		} else {
			BTLeafPage* newLeafPage;
			PageID newLeafPid;
			NEWPAGE(newLeafPid, (Page *&)newLeafPage, 1);
			newLeafPage->SetType(LEAF_NODE);
			newLeafPage->Init(newLeafPid);

			RecordID rid;
			RecordID keyRecordID;
			char *key=new char[MAX_KEY_SIZE];

			s = leafPage->GetLast(rid,key,keyRecordID);
			while(newLeafPage->AvailableSpace() > leafPage->AvailableSpace() && s != DONE){
				s= newLeafPage->Insert(key, keyRecordID, rid);
				leafPage->Delete(key,keyRecordID);
				s = leafPage->GetLast(rid,key,keyRecordID);
			}

			s = newLeafPage->GetFirst(rid, key, keyRecordID);

			if (KeyCmp(targetKey, key) >= 0) {
				res = newLeafPage->Insert(targetKey, targetId, leafRid);
			} else {
				res = leafPage->Insert(targetKey, targetId, leafRid);
			}
			s = newLeafPage->GetFirst(rid, key, keyRecordID);
			strcpy(newKey, key);
			newPid = newLeafPid;

			PageID nnPid = leafPage->GetNextPage();
			if (nnPid != INVALID_PAGE) {
				BTLeafPage *nnPage;
				PIN(nnPid, (Page *&)nnPage);
				nnPage->SetPrevPage(newLeafPid);
				UNPIN(nnPid, DIRTY);
			}
			newLeafPage->SetNextPage(nnPid);
			newLeafPage->SetPrevPage(nodePid);
			leafPage->SetNextPage(newLeafPid);

			UNPIN(nodePid, DIRTY);
			UNPIN(newLeafPid, DIRTY);
			return res;
		}

	} else {
		BTIndexPage * indexPage = (BTIndexPage *) nodePage;

		PageID targetPid;
		bool leftMost;
		s = indexPage->FindPage(targetKey, targetPid, leftMost);

		PageID tempNewPid;
		char *tempNewKey = new char[MAX_KEY_SIZE];
		res = _Insert(targetPid, targetKey, targetId, tempNewPid, tempNewKey);
		
		if (tempNewPid == INVALID_PAGE) {
			newPid = INVALID_PAGE;
			UNPIN(nodePid, DIRTY);
			return res;
		}

		if (indexPage->AvailableSpace() >= GetKeyDataLength(tempNewKey, nodeType)) {
			RecordID rid;
			res = indexPage->Insert(tempNewKey, tempNewPid, rid);
			newPid = INVALID_PAGE;
			UNPIN(nodePid, DIRTY);
			return res;
		} else {
			PageID newIndexPid;
			BTIndexPage* newIndexPage;
			RecordID rid;
			NEWPAGE(newIndexPid, (Page *&)newIndexPage, 1);
			newIndexPage->SetType(INDEX_NODE);
			newIndexPage->Init(newIndexPid);

			RecordID keyRecordID;
			PageID cPid;
			char * cKey=new char[MAX_KEY_SIZE];

			s = indexPage->GetLast(rid, cKey, cPid);
			while(newIndexPage->AvailableSpace() > indexPage->AvailableSpace() && s != DONE){
				s= newIndexPage->Insert(cKey, cPid, rid);
				indexPage->Delete(cKey, rid);
				s = indexPage->GetLast(rid, cKey, cPid);
			}

			s = newIndexPage->GetFirst(rid, cKey, cPid);
			newIndexPage->Delete(cKey, rid);
			newIndexPage->SetLeftLink(cPid);

			strcpy(newKey, cKey);

			if (KeyCmp(tempNewKey, newKey) >= 0) {
				res = newIndexPage->Insert(tempNewKey, tempNewPid, rid);
			} else {
				res = indexPage->Insert(tempNewKey, tempNewPid, rid);
			}

			newPid = newIndexPid;
			UNPIN(nodePid, DIRTY);
			UNPIN(newIndexPid, DIRTY);
			return res;
		}
	}
}


//-------------------------------------------------------------------
// BTreeFile::Delete
//
// Input   : key - pointer to the value of the key to be deleted.
//           rid - RecordID of the record to be deleted.
// Output  : None
// Return  : OK if successful, FAIL otherwise. 
// Purpose : Delete an entry with this rid and key.  
// Note    : If the root becomes empty, delete it.
//-------------------------------------------------------------------

Status BTreeFile::Delete (const char *key, const RecordID rid)
{
	SortedPage* rootPage; 
	PageID rootPid;
	Status s = OK;
	Status res;
	NodeType type;
	char *key2; RecordID rid2; RecordID keyRecordID;

	rootPid = header->GetRootPageID();
	PIN(rootPid, (Page *&)rootPage);

 	type = rootPage->GetType();
	if (type == LEAF_NODE) {
		BTLeafPage *leafPage = (BTLeafPage *)rootPage;

		res = leafPage->Delete(key, rid);

		if (leafPage->GetNumOfRecords() == 0) {
			header->SetRootPageID(INVALID_PAGE);
		}

		UNPIN(rootPid,DIRTY);
		return res;
	} else {
		PageID childPid;
		BTIndexPage *indexPage =(BTIndexPage *)rootPage;
		
		PageID oldPid;
		bool rightSibling;
		s = indexPage->GetPageID(key, childPid);
		res = _Delete(rootPid, childPid, key, rid, oldPid, rightSibling);

		if (res == FAIL) {
			UNPIN(rootPid,DIRTY);
			return FAIL;
		}

		if (oldPid != INVALID_PAGE) {
			indexPage->DeletePage(oldPid, rightSibling);
			PageID firstPid;
			key2 = new char[MAX_KEY_SIZE];
			s = indexPage->GetFirst(rid2, key2, firstPid);
			if (s == DONE) {
				firstPid = indexPage->GetLeftLink();
				header->SetRootPageID(firstPid);
			}
			delete key2;
		}
		UNPIN(rootPid,DIRTY);
		return res;
	}
}

Status BTreeFile::_Delete(PageID parentPid, PageID nodePid, const char *key, const RecordID rid, PageID& oldPid, bool& rightSibling) {
	RecordID tempRid;
	char* tempKey = new char[MAX_KEY_SIZE];
	Status res;
	Status s;

	BTIndexPage *parentPage;
	PIN(parentPid, (Page *&)parentPage);
	
	SortedPage *nodePage;
	PIN(nodePid, (Page *&)nodePage);
	NodeType nodeType = nodePage->GetType();

	if (nodeType == LEAF_NODE) {
		BTLeafPage * nodePageL = (BTLeafPage *) nodePage;
		res = nodePageL->Delete(key, rid);

		if (res == FAIL || nodePageL->AvailableSpace() <= HEAPPAGE_DATA_SIZE/2) {
			oldPid = INVALID_PAGE;
			UNPIN(parentPid, DIRTY);
			UNPIN(nodePid, DIRTY);
			return res;
		}

		PageID siblingPid;
		parentPage->FindSiblingForChild(nodePid, siblingPid, rightSibling);

		BTLeafPage *siblingPage;
		PIN(siblingPid, (Page *&)siblingPage);

		char* oldParentKey = new char[MAX_KEY_SIZE];
		RecordID tempDrid;
		if (rightSibling) {
			s = siblingPage->GetFirst(tempRid, tempKey, tempDrid);
			s = parentPage->FindKey(tempKey, oldParentKey);
		} else {
			s = nodePageL->GetFirst(tempRid, tempKey, tempDrid);
			parentPage->FindKey(tempKey, oldParentKey);
		}

		// redistribute
		while(nodePageL->AvailableSpace() > HEAPPAGE_DATA_SIZE/2) {
			if (rightSibling) {
				s = siblingPage->GetFirst(tempRid, tempKey, tempDrid);
				s = nodePageL->Insert(tempKey, tempDrid, tempRid);
				if(s == OK) siblingPage->Delete(tempKey, tempDrid);
			} else {
				s = siblingPage->GetLast(tempRid,tempKey,tempDrid);
				s= nodePageL->Insert(tempKey, tempDrid, tempRid);
				if(s == OK) {
					siblingPage->Delete(tempKey, tempDrid);
				}
			}
		}

		// redistribution successful
		if (siblingPage->AvailableSpace() <= HEAPPAGE_DATA_SIZE/2) {
			if (rightSibling) {
				s = siblingPage->GetFirst(tempRid, tempKey, tempDrid);
			} else {
				s = nodePageL->GetFirst(tempRid, tempKey, tempDrid);
			}
			parentPage->AdjustKey(tempKey, oldParentKey);
			oldPid = INVALID_PAGE;
			UNPIN(parentPid, DIRTY);
			UNPIN(nodePid, DIRTY);
			UNPIN(siblingPid, DIRTY);
			return res;
		} else {
			if (siblingPage->AvailableSpace() + nodePageL->AvailableSpace() >= HEAPPAGE_DATA_SIZE) {
				// merge
				while (true) {
					s = siblingPage->GetFirst(tempRid, tempKey, tempDrid);
					if(s == OK) {
						s = nodePageL->Insert(tempKey, tempDrid, tempRid);
						siblingPage->Delete(tempKey, tempDrid);
					} else if (s == DONE) {
						break;
					};
				}
				if (rightSibling) {
					PageID nnPid = siblingPage->GetNextPage();
					if (nnPid != INVALID_PAGE) {
						BTLeafPage *nnPage;
						PIN(nnPid, (Page *&)nnPage);
						nnPage->SetPrevPage(nodePid);
						UNPIN(nnPid, DIRTY);
					}
					nodePageL->SetNextPage(nnPid);
				} else {
					PageID ppPid = siblingPage->GetPrevPage();
					if (ppPid != INVALID_PAGE) {
						BTLeafPage *ppPage;
						PIN(ppPid, (Page *&)ppPage);
						ppPage->SetNextPage(nodePid);
						UNPIN(ppPid, DIRTY);
					}
					nodePageL->SetPrevPage(ppPid);
				}
				oldPid = siblingPid;
				UNPIN(parentPid, DIRTY);
				UNPIN(nodePid, DIRTY);
				UNPIN(siblingPid, DIRTY);

				return res;
			} else {
				oldPid = INVALID_PAGE;
				UNPIN(parentPid, DIRTY);
				UNPIN(nodePid, DIRTY);
				UNPIN(siblingPid, DIRTY);
				return res;
			}
		}
	} else {
		BTIndexPage * nodePageI = (BTIndexPage *) nodePage;

		PageID targetPid;
		bool leftMost;
		s = nodePageI->FindPage(key, targetPid, leftMost);

		PageID tempOldPid;
		bool tempRightSibling;

		res = _Delete(nodePid, targetPid, key, rid, tempOldPid, tempRightSibling);

		if (res == FAIL || tempOldPid == INVALID_PAGE) {
			oldPid = INVALID_PAGE;
			UNPIN(parentPid, CLEAN);
			UNPIN(nodePid, CLEAN);
			return res;
		}

		nodePageI->DeletePage(tempOldPid, tempRightSibling);

		if (nodePageI->AvailableSpace() <= HEAPPAGE_DATA_SIZE/2) {
			oldPid = INVALID_PAGE;
			UNPIN(parentPid, CLEAN);
			UNPIN(nodePid, DIRTY);
			return res;
		}

		PageID siblingPid;
		bool rightSibling;
		parentPage->FindSiblingForChild(nodePid, siblingPid, rightSibling);

		BTIndexPage *siblingPage;
		PIN(siblingPid, (Page *&)siblingPage);

		char *keyToAdjust = new char[MAX_KEY_SIZE];
		if (rightSibling) {
			parentPage->FindKeyWithPage(siblingPid, keyToAdjust, leftMost);
		} else {
			parentPage->FindKeyWithPage(nodePid, keyToAdjust, leftMost);
		}

		// redistribute
		PageID tempPid;
		while(nodePageI->AvailableSpace() > HEAPPAGE_DATA_SIZE/2) {
			if (rightSibling) {
				siblingPage->GetFirst(tempRid, tempKey, tempPid);
				nodePageI->Insert(keyToAdjust, siblingPage->GetLeftLink(), tempRid);
				parentPage->AdjustKey(tempKey, keyToAdjust);
				keyToAdjust = tempKey;
				siblingPage->SetLeftLink(tempPid);
				siblingPage->Delete(tempKey, tempRid);
			} else {
				s = siblingPage->GetLast(tempRid, tempKey, tempPid);
				nodePageI->Insert(keyToAdjust, nodePageI->GetLeftLink(), tempRid);
				parentPage->AdjustKey(tempKey, keyToAdjust);
				keyToAdjust = tempKey;
				nodePageI->SetLeftLink(tempPid);
				siblingPage->Delete(tempKey, tempRid);
			}
		}

		// redistribution successful
		if (siblingPage->AvailableSpace() <= HEAPPAGE_DATA_SIZE/2) {
			oldPid = INVALID_PAGE;
			UNPIN(parentPid, DIRTY);
			UNPIN(nodePid, DIRTY);
			UNPIN(siblingPid, DIRTY);
			return res;
		} else {
			if (siblingPage->AvailableSpace() + nodePageI->AvailableSpace() >= HEAPPAGE_DATA_SIZE) {
				// merge
				while (true) {
					if (rightSibling) {
						s = siblingPage->GetFirst(tempRid, tempKey, tempPid);
						nodePageI->Insert(keyToAdjust, siblingPage->GetLeftLink(), tempRid);
						if (s == DONE) {
							break;
						}
						parentPage->AdjustKey(tempKey, keyToAdjust);
						keyToAdjust = tempKey;
						siblingPage->SetLeftLink(tempPid);
						siblingPage->Delete(tempKey, tempRid);
					} else {
						
						nodePageI->Insert(keyToAdjust, nodePageI->GetLeftLink(), tempRid);
						s = siblingPage->GetLast(tempRid, tempKey, tempPid);
						if (s == DONE) {
							nodePageI->SetLeftLink(siblingPage->GetLeftLink());
							break;
						} else {
							parentPage->AdjustKey(tempKey, keyToAdjust);
							keyToAdjust = tempKey;
							nodePageI->SetLeftLink(tempPid);
							siblingPage->Delete(tempKey, tempRid);
						}
					}
				}

				s = siblingPage->GetFirst(tempRid, tempKey, tempPid);
				if (s == DONE) {
					oldPid = siblingPid;
					UNPIN(parentPid, DIRTY);
					UNPIN(nodePid, DIRTY);
					UNPIN(siblingPid, DIRTY);
					return res;
				} else {
					oldPid = INVALID_PAGE;
					UNPIN(parentPid, DIRTY);
					UNPIN(nodePid, DIRTY);
					UNPIN(siblingPid, DIRTY);
					return res;
				}
			} else {
				oldPid = INVALID_PAGE;
				UNPIN(parentPid, DIRTY);
				UNPIN(nodePid, DIRTY);
				UNPIN(siblingPid, DIRTY);
				return res;
			}
		}
	}
}

//-------------------------------------------------------------------
// BTreeFile::OpenScan
//
// Input   : lowKey, highKey - pointer to keys, indicate the range
//                             to scan.
// Output  : None
// Return  : A pointer to IndexFileScan class.
// Purpose : Initialize a scan.  
// Note    : Usage of lowKey and highKey :
//
//           lowKey   highKey   range
//			 value	  value	
//           --------------------------------------------------
//           NULL     NULL      whole index
//           NULL     !NULL     minimum to highKey
//           !NULL    NULL      lowKey to maximum
//           !NULL    =lowKey   exact match (may not be unique)
//           !NULL    >lowKey   lowKey to highKey
//-------------------------------------------------------------------

IndexFileScan *BTreeFile::OpenScan (const char *lowKey, const char *highKey)
{
	RecordID rid; char* key; RecordID keyRecordID;
	RecordID validrid; char* validkey; RecordID validkeyRecordID;
	Status s= OK;
	PageID rootPageID;
	PageID startPageID; BTLeafPage *startPage;
	BTreeFileScan* scan=new BTreeFileScan(); 

	scan->setScanFirstTime(true);

	if (highKey != NULL){
		key = new char[MAX_KEY_SIZE];
		key = strcpy(key, highKey);
		scan->setScanHighKey(key);
	} else {
		scan->setScanHighKey(NULL);
	}

	if(lowKey!=NULL) {
		key = new char[MAX_KEY_SIZE];
		key = strcpy(key, lowKey);
		scan->setScanLowKey(key);
	} else {
		scan->setScanLowKey(NULL);
	}

	if (header == NULL || headerID == INVALID_PAGE) {
		headerID = INVALID_PAGE;
		header = NULL;
	}

	rootPageID= header->GetRootPageID();
	if (rootPageID == INVALID_PAGE){
		scan->setScanPid(INVALID_PAGE);
		return scan;
	}

	if (rootPageID!=INVALID_PAGE) {
		PageID startPageID;
		if (lowKey == NULL) {
			startPageID = GetLeftmostLeaf();
		} else {
			if (_Search(lowKey, header->GetRootPageID(), startPageID) != OK) return scan;
		}

		scan->setScanPid(startPageID);

		s = MINIBASE_BM->PinPage(startPageID, (Page *&)startPage);
		if (startPage->GetNumOfRecords() <= 0) {
			s=MINIBASE_BM->UnpinPage(rootPageID);
			return scan;
		}

		key=new char[MAX_KEY_SIZE];
		s = startPage->GetFirst(rid, key, keyRecordID);
		if (lowKey != NULL) {
			while(strcmp(lowKey, key) != 0){ // TODO: probably > 0
				s = startPage->GetNext(rid, key, keyRecordID);
			}
			scan->setScanLowKey(key);
		}
		scan->setScanCrid(rid);

		s=MINIBASE_BM->UnpinPage(startPageID, CLEAN);
	}
	return scan;
}

// Dump Following Statistics:
// 1. Total # of leafnodes, and Indexnodes.
// 2. Total # of dataEntries.
// 3. Total # of index Entries.
// 4. Fill factor of leaf nodes. avg. min. max.
Status BTreeFile::DumpStatistics() {	
	ostream& os = std::cout;
	float avgDataFillFactor, avgIndexFillFactor;

// initialization 
	hight = totalDataPages = totalIndexPages = totalNumIndex = totalNumData = 0;
	maxDataFillFactor = maxIndexFillFactor = 0; minDataFillFactor = minIndexFillFactor =1;
	totalFillData = totalFillIndex = 0;

	if(_DumpStatistics(header->GetRootPageID())== OK)
	{		// output result
		if (totalNumData == 0)
			maxDataFillFactor = minDataFillFactor = avgDataFillFactor = 0;
		else
			avgDataFillFactor = totalFillData/totalDataPages;
		if ( totalNumIndex == 0)
			maxIndexFillFactor = minIndexFillFactor = avgIndexFillFactor = 0;
		else 
			avgIndexFillFactor = totalFillIndex/totalIndexPages;
		os << "\n------------ Now dumping statistics of current B+ Tree!---------------" << endl;
		os << "  Total nodes are        : " << totalDataPages + totalIndexPages << " ( " << totalDataPages << " Data";
		os << "  , " << totalIndexPages <<" indexpages )" << endl;
		os << "  Total data entries are : " << totalNumData << endl;
		os << "  Total index entries are: " << totalNumIndex << endl;
		os << "  Hight of the tree is   : " << hight << endl;
		os << "  Average fill factors for leaf is : " << avgDataFillFactor<< endl;
		os << "  Maximum fill factors for leaf is : " << maxDataFillFactor;
		os << "	  Minumum fill factors for leaf is : " << minDataFillFactor << endl;
		os << "  Average fill factors for index is : " << 	avgIndexFillFactor << endl;
		os << "  Maximum fill factors for index is : " << maxIndexFillFactor;
		os << "	  Minumum fill factors for index is : " << minIndexFillFactor << endl;
		os << "  That's the end of dumping statistics." << endl;

		return OK;
	}
	return FAIL;
}

Status BTreeFile::_DumpStatistics(PageID pageID) { 
	__DumpStatistics(pageID);

	SortedPage *page;
	BTIndexPage *index;

	Status s;
	PageID curPageID;

	RecordID curRid;
	KeyType key;

	PIN (pageID, page);
	NodeType type = page->GetType ();

	switch (type) {
	case INDEX_NODE:
		index = (BTIndexPage *)page;
		curPageID = index->GetLeftLink();
		_DumpStatistics(curPageID);
		s=index->GetFirst(curRid, key, curPageID);
		if ( s == OK) {	
			_DumpStatistics(curPageID);
			s = index->GetNext(curRid, key, curPageID);
			while ( s != DONE) {	
				_DumpStatistics(curPageID);
				s = index->GetNext(curRid, key, curPageID);
			}
		}
		UNPIN(pageID, CLEAN);
		break;

	case LEAF_NODE:
		UNPIN(pageID, CLEAN);
		break;
	default:		
		assert (0);
	}

	return OK;
}

Status BTreeFile::__DumpStatistics (PageID pageID) {
	SortedPage *page;
	BTIndexPage *index;
	BTLeafPage *leaf;
	int i;
	Status s;
	PageID curPageID;
	float	curFillFactor;
	RecordID curRid;

	KeyType  key;
	RecordID dataRid;

	PIN (pageID, page);
	NodeType type = page->GetType ();
	i = 0;
	switch (type) {
	case INDEX_NODE:
		// add totalIndexPages
		totalIndexPages++;
		if ( hight <= 0) // still not reach the bottom
			hight--;
		index = (BTIndexPage *)page;
		curPageID = index->GetLeftLink();
		s=index->GetFirst (curRid , key, curPageID); 
		if ( s == OK) {	
			i++;
			s = index->GetNext(curRid, key, curPageID);
			while (s != DONE) {	
				i++;
				s = index->GetNext(curRid, key, curPageID);
			}
		}
		totalNumIndex  += i;
		curFillFactor = (float)(1.0 - 1.0*(index->AvailableSpace())/MAX_SPACE);
		if ( maxIndexFillFactor < curFillFactor)
			maxIndexFillFactor = curFillFactor;
		if ( minIndexFillFactor > curFillFactor)
			minIndexFillFactor = curFillFactor;
		totalFillIndex += curFillFactor;
		UNPIN(pageID, CLEAN);
		break;

	case LEAF_NODE:
		// when the first time reach a leaf, set hight
		if ( hight < 0)
			hight = -hight;
		totalDataPages++;

		leaf = (BTLeafPage *)page;
		s = leaf->GetFirst (curRid, key, dataRid);
		if (s == OK) {	
			s = leaf->GetNext(curRid, key, dataRid);
			i++;
			while ( s != DONE) {	
				i++;	
				s = leaf->GetNext(curRid, key, dataRid);
			}
		}
		totalNumData += i;
		curFillFactor = (float)(1.0 - 1.0*leaf->AvailableSpace()/MAX_SPACE);
		if ( maxDataFillFactor < curFillFactor)
			maxDataFillFactor = curFillFactor;
		if ( minDataFillFactor > curFillFactor)
			minDataFillFactor = curFillFactor;
		totalFillData += curFillFactor;
		UNPIN(pageID, CLEAN);
		break;
	default:		
		assert (0);
	}

	return OK;	
}

// function  BTreeFile::_SearchIndex
// PURPOSE	: given a IndexNode and key, find the PageID with the key in it
// INPUT	: key, a pointer to key;
//			: currIndexID, 
//			: curIndex, pointer to current BTIndexPage
// OUTPUT	: found PageID
Status BTreeFile::_SearchIndex (const char *key,  PageID currIndexID, BTIndexPage *currIndex, PageID& foundID)
{
	PageID nextPageID;
	
	Status s = currIndex->GetPageID (key, nextPageID);
	if (s != OK)
		return FAIL;
	
	// Now unpin the page, recurse and then pin it again
	
	UNPIN (currIndexID, CLEAN);
	s = _Search (key, nextPageID, foundID);
	if (s != OK)
		return FAIL;
	return OK;
}

// Function: 
// Input	: key, rid
// Output	: found Pid, where Key >= key
// Purpose	: find the leftmost leaf page contain the key, or bigger than the key
Status BTreeFile::_Search( const char *key,  PageID currID, PageID& foundID)
{
	
    SortedPage *page;
	Status s;
	
    PIN (currID, page);
    NodeType type = page->GetType ();
	
    // TWO CASES:
    // - pageType == INDEX:
    //   search the index
    // - pageType == LEAF_NODE:
    //   set the output page ID
	
    switch (type) 
	{
	case INDEX_NODE:
		s =	_SearchIndex(key,  currID, (BTIndexPage*)page, foundID);
		break;
		
	case LEAF_NODE:
		foundID =  page->PageNo();
		UNPIN(currID,CLEAN);
		break;
	default:		
		assert (0);
	}


	return OK;
}

// BTreeeFile:: Search
// PURPOSE	: find the PageNo of a give key
// INPUT	: key, pointer to a key
// OUTPUT	: foundPid

Status BTreeFile::Search(const char *key,  PageID& foundPid)
{
	if (header->GetRootPageID() == INVALID_PAGE)
	{
		foundPid = INVALID_PAGE;
		return DONE;
		
	}

	Status s;
	
	s = _Search(key,  header->GetRootPageID(), foundPid);
	if (s != OK)
	{
		cerr << "Search FAIL in BTreeFile::Search\n";
		return FAIL;
	}

	return OK;
}

Status BTreeFile::_PrintTree ( PageID pageID)
{
	SortedPage *page;
	BTIndexPage *index;
	BTLeafPage *leaf;
	int i;
	Status s;
	PageID curPageID;

	RecordID curRid;

	KeyType  key;
	RecordID dataRid;

	ostream& os = cout;

    PIN (pageID, page);
    NodeType type = page->GetType ();
	i = 0;
      switch (type) 
	{
	case INDEX_NODE:
			index = (BTIndexPage *)page;
			curPageID = index->GetLeftLink();
			os << "\n---------------- Content of Index_Node-----   " << pageID <<endl;
			os << "\n Left most PageID:  "  << curPageID << endl;
			s=index->GetFirst (curRid , key, curPageID); 
			if ( s == OK)
			{	i++;
				os <<"Key: "<< key<< "	PageID: " 
					<< curPageID  << endl;
				s = index->GetNext(curRid, key, curPageID);
				while ( s != DONE)
				{	
					os <<"Key: "<< key<< "	PageID: " 
						<< curPageID  << endl;
					i++;
					s = index->GetNext(curRid, key, curPageID);

				}
			}
			os << "\n This page contains  " << i <<"  Entries!" << endl;
			UNPIN(pageID, CLEAN);
			break;
		
	case LEAF_NODE:
		leaf = (BTLeafPage *)page;
		s = leaf->GetFirst (curRid, key, dataRid);
			if ( s == OK)
			{	os << "\n Content of Leaf_Node"  << pageID << endl;
				os <<   "Key: "<< key<< "	DataRecordID: " 
					<< dataRid  << endl;
				s = leaf->GetNext(curRid, key, dataRid);
				i++;
				while ( s != DONE)
				{	
					os <<   "Key: "<< key<< "	DataRecordID: " 
						<< dataRid  << endl;
					i++;	
					s = leaf->GetNext(curRid, key, dataRid);
				}
			}
			os << "\n This page contains  " << i <<"  entries!" << endl;
			UNPIN(pageID, CLEAN);
			break;
	default:		
		assert (0);
	}
	//os.close();

	return OK;	
}

Status BTreeFile::PrintTree ( PageID pageID, PrintOption option)
{ 
	_PrintTree(pageID);
	if (option == SINGLE) return OK;

	SortedPage *page;
	BTIndexPage *index;

	Status s;
	PageID curPageID;

	RecordID curRid;
	KeyType  key;

    PIN (pageID, page);
    NodeType type = page->GetType ();
	
	switch (type) {
	case INDEX_NODE:
		index = (BTIndexPage *)page;
		curPageID = index->GetLeftLink();
		PrintTree(curPageID, RECURSIVE);
		s=index->GetFirst (curRid , key, curPageID);
		if ( s == OK)
		{	
			PrintTree(curPageID, RECURSIVE);
			s = index->GetNext(curRid, key, curPageID);
			while ( s != DONE)
			{	
				PrintTree(curPageID, RECURSIVE);
				s = index->GetNext(curRid, key, curPageID);
			}
		}
		UNPIN(pageID, CLEAN);
		break;

	case LEAF_NODE:
		UNPIN(pageID, CLEAN);
		break;
	default:		
		assert (0);
	}

	return OK;
}

Status BTreeFile::PrintWhole() {	
	ostream& os = cout;

	os << "\n\n------------------ Now Begin Printing a new whole B+ Tree -----------"<< endl;

	if(PrintTree(header->GetRootPageID(), RECURSIVE)== OK)
		return OK;
	return FAIL;
}


//	Get the leftmost leaf page in this index.
PageID BTreeFile::GetLeftmostLeaf() {
	PageID curPid = header->GetRootPageID();

	while (curPid != INVALID_PAGE) {
		SortedPage *curPage;

		if (MINIBASE_BM->PinPage(curPid, (Page *&)curPage) == FAIL) {
			std::cerr << "Unable to pin page" << std::endl;
			return INVALID_PAGE;
		}
	
		NodeType nodeType = curPage->GetType();

		if (nodeType == LEAF_NODE) {
			//	If we have reached a leaf page, then we are done.
			break;
		} else {
			//	Traverse down to the leftmost branch
			PageID tempPid = curPage->GetPrevPage();

			if (MINIBASE_BM->UnpinPage(curPid, CLEAN) == FAIL) {
				std::cerr << "Unable to unpin page" << std::endl;
				return INVALID_PAGE;
			}

			curPid = tempPid;
		}
	}

	if (curPid != INVALID_PAGE) {
		if (MINIBASE_BM->UnpinPage(curPid, CLEAN) == FAIL) {
			std::cerr << "Unable to unpin page" << std::endl;
			return INVALID_PAGE;
		}
	}
	return curPid;
}