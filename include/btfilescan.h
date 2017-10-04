#ifndef _BTREE_FILESCAN_H
#define _BTREE_FILESCAN_H

#include "btfile.h"

class BTreeFile;

class BTreeFileScan : public IndexFileScan {
	
public:
	
	friend class BTreeFile;

    Status GetNext (RecordID & rid, char* keyptr);

	~BTreeFileScan();	

private:
	// You may add members and methods here.
	bool firstTime;
	const char *lowKey;
	const char *highKey;
	RecordID crid;
	PageID pid;

	void setScanFirstTime(bool ft) {firstTime = ft;}
	void setScanLowKey(const char *nlowKey) {lowKey=nlowKey;}
	void setScanHighKey(const char *nhighKey) {highKey=nhighKey;}
	void setScanPid(PageID npid) {pid=npid;}
	void setScanCrid(RecordID rid) {crid=rid;}
};

#endif