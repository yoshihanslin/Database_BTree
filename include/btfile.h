#ifndef _BTFILE_H
#define _BTFILE_H

#include "btindex.h"
#include "btleaf.h"
#include "index.h"
#include "btfilescan.h"
#include "bt.h"

enum PrintOption
{ SINGLE,
  RECURSIVE
};

class BTreeFile: public IndexFile {
	
public:
	
	friend class BTreeDriver;
	friend class BTreeFileScan;

    BTreeFile(Status& status, const char *filename);

	~BTreeFile();
	
    Status DestroyFile();
	
    Status Insert(const char *key, const RecordID rid); 
    Status Delete(const char *key, const RecordID rid);
    
	IndexFileScan *OpenScan(const char *lowKey = NULL, 
		const char *highKey = NULL);

	Status Search(const char *key,  PageID& foundPid);

	Status PrintTree (PageID pageID, PrintOption option);
	Status PrintWhole ();
	Status DumpStatistics();

private:

    struct BTreeHeaderPage : HeapPage {
	public:
		// Initializes the header page and sets the root to be invalid.
		void Init(PageID hpid) {
			HeapPage::Init(hpid);
			SetRootPageID(INVALID_PAGE);
		}

		PageID GetRootPageID() {
			return *((PageID *) HeapPage::data);
		}

		// Sets the page id of the root.
		void SetRootPageID(PageID pid) {
			PageID *ptr = (PageID *)(HeapPage::data);
			*ptr = pid;
		}
    };

	BTreeHeaderPage *header;   // header page
    PageID           headerID; // page number of header page
    char            *dbname;       // copied from arg of the constructor.	
    
	int				totalDataPages;
	int				totalIndexPages;
	float				maxDataFillFactor;
	float				minDataFillFactor;
	float				maxIndexFillFactor;
	float				minIndexFillFactor;
	float				totalFillData; // sum of each data nodes' usedspace/fullpagespace
	float				totalFillIndex;
	int				totalNumIndex; // total num of Index Entries
	int				totalNumData;
	int				hight; // hight of Tree

	Status _Search( const char *key,  PageID, PageID&);
	Status _SearchIndex (const char *key,  PageID currIndexID, BTIndexPage *currIndex, PageID& foundID);
	Status _PrintTree ( PageID pageID);
	Status _Insert(PageID nodePid, const char *key, const RecordID rid, PageID& newPid, char *newKey);
	Status _Delete(PageID parentPid, PageID nodePid, const char *key, const RecordID rid, PageID& oldPid, bool& rightSibling);

	Status BTreeFile::_DumpStatistics(PageID);
	Status BTreeFile::__DumpStatistics(PageID);

	// You may add members and methods here.
	//BTreeFileScan* scan; 
	Status DestroyNode(PageID pageID);
	Status Split1LeafNode(PageID leafPageID, PageID& newRootPageID, const char *key, const RecordID rid); //splits leafPageID, returns newRootPageID
	PageID BTreeFile::GetLeftmostLeaf();
};


#endif // _BTFILE_H