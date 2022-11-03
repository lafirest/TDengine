/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _TD_VNODE_TSDB_H_
#define _TD_VNODE_TSDB_H_

#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// tsdbDebug ================
// clang-format off
#define tsdbFatal(...) do { if (tsdbDebugFlag & DEBUG_FATAL) { taosPrintLog("TSD FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define tsdbError(...) do { if (tsdbDebugFlag & DEBUG_ERROR) { taosPrintLog("TSD ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define tsdbWarn(...)  do { if (tsdbDebugFlag & DEBUG_WARN)  { taosPrintLog("TSD WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define tsdbInfo(...)  do { if (tsdbDebugFlag & DEBUG_INFO)  { taosPrintLog("TSD ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define tsdbDebug(...) do { if (tsdbDebugFlag & DEBUG_DEBUG) { taosPrintLog("TSD ", DEBUG_DEBUG, tsdbDebugFlag, __VA_ARGS__); }} while(0)
#define tsdbTrace(...) do { if (tsdbDebugFlag & DEBUG_TRACE) { taosPrintLog("TSD ", DEBUG_TRACE, tsdbDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

typedef struct TSDBROW          TSDBROW;
typedef struct TABLEID          TABLEID;
typedef struct TSDBKEY          TSDBKEY;
typedef struct SDelData         SDelData;
typedef struct SDelIdx          SDelIdx;
typedef struct STbData          STbData;
typedef struct SMemTable        SMemTable;
typedef struct STbDataIter      STbDataIter;
typedef struct SMapData         SMapData;
typedef struct SBlockIdx        SBlockIdx;
typedef struct SDataBlk         SDataBlk;
typedef struct SSttBlk          SSttBlk;
typedef struct SDiskDataHdr     SDiskDataHdr;
typedef struct SBlockData       SBlockData;
typedef struct SDelFile         SDelFile;
typedef struct SHeadFile        SHeadFile;
typedef struct SDataFile        SDataFile;
typedef struct SSttFile         SSttFile;
typedef struct SSmaFile         SSmaFile;
typedef struct SDFileSet        SDFileSet;
typedef struct SDataFWriter     SDataFWriter;
typedef struct SDataFReader     SDataFReader;
typedef struct SDelFWriter      SDelFWriter;
typedef struct SDelFReader      SDelFReader;
typedef struct SRowIter         SRowIter;
typedef struct STsdbFS          STsdbFS;
typedef struct SRowMerger       SRowMerger;
typedef struct STsdbReadSnap    STsdbReadSnap;
typedef struct SBlockInfo       SBlockInfo;
typedef struct SSmaInfo         SSmaInfo;
typedef struct SBlockCol        SBlockCol;
typedef struct SVersionRange    SVersionRange;
typedef struct SLDataIter       SLDataIter;
typedef struct SDiskCol         SDiskCol;
typedef struct SDiskData        SDiskData;
typedef struct SDiskDataBuilder SDiskDataBuilder;
typedef struct SBlkInfo         SBlkInfo;

#define TSDB_FILE_DLMT     ((uint32_t)0xF00AFA0F)
#define TSDB_MAX_SUBBLOCKS 8
#define TSDB_FHDR_SIZE     512

#define VERSION_MIN 0
#define VERSION_MAX INT64_MAX

#define TSDBKEY_MIN ((TSDBKEY){.ts = TSKEY_MIN, .version = VERSION_MIN})
#define TSDBKEY_MAX ((TSDBKEY){.ts = TSKEY_MAX, .version = VERSION_MAX})

#define TABLE_SAME_SCHEMA(SUID1, UID1, SUID2, UID2) ((SUID1) ? (SUID1) == (SUID2) : (UID1) == (UID2))

#define PAGE_CONTENT_SIZE(PAGE) ((PAGE) - sizeof(TSCKSUM))
#define LOGIC_TO_FILE_OFFSET(LOFFSET, PAGE) \
  ((LOFFSET) / PAGE_CONTENT_SIZE(PAGE) * (PAGE) + (LOFFSET) % PAGE_CONTENT_SIZE(PAGE))
#define FILE_TO_LOGIC_OFFSET(OFFSET, PAGE) ((OFFSET) / (PAGE)*PAGE_CONTENT_SIZE(PAGE) + (OFFSET) % (PAGE))
#define PAGE_OFFSET(PGNO, PAGE)            (((PGNO)-1) * (PAGE))
#define OFFSET_PGNO(OFFSET, PAGE)          ((OFFSET) / (PAGE) + 1)

static FORCE_INLINE int64_t tsdbLogicToFileSize(int64_t lSize, int32_t szPage) {
  int64_t fOffSet = LOGIC_TO_FILE_OFFSET(lSize, szPage);
  int64_t pgno = OFFSET_PGNO(fOffSet, szPage);

  if (fOffSet % szPage == 0) {
    pgno--;
  }

  return pgno * szPage;
}

#include "tsdb/tsdbUtil.h"

#include "tsdb/tsdbFile.h"

#include "tsdb/tsdbFS.h"

#include "tsdb/tsdbReaderWriter.h"

// tsdbRead.c ==============================================================================================
int32_t tsdbTakeReadSnap(STsdb *pTsdb, STsdbReadSnap **ppSnap, const char *id);
void    tsdbUntakeReadSnap(STsdb *pTsdb, STsdbReadSnap *pSnap, const char *id);

#define TSDB_CACHE_NO(c)       ((c).cacheLast == 0)
#define TSDB_CACHE_LAST_ROW(c) (((c).cacheLast & 1) > 0)
#define TSDB_CACHE_LAST(c)     (((c).cacheLast & 2) > 0)

// tsdbDiskData ==============================================================================================
int32_t tDiskDataBuilderCreate(SDiskDataBuilder **ppBuilder);
void   *tDiskDataBuilderDestroy(SDiskDataBuilder *pBuilder);
int32_t tDiskDataBuilderInit(SDiskDataBuilder *pBuilder, STSchema *pTSchema, TABLEID *pId, uint8_t cmprAlg,
                             uint8_t calcSma);
int32_t tDiskDataBuilderClear(SDiskDataBuilder *pBuilder);
int32_t tDiskDataAddRow(SDiskDataBuilder *pBuilder, TSDBROW *pRow, STSchema *pTSchema, TABLEID *pId);
int32_t tGnrtDiskData(SDiskDataBuilder *pBuilder, const SDiskData **ppDiskData, const SBlkInfo **ppBlkInfo);

#include "tsdb/tsdbFS2.h"

// structs =======================
struct STsdbFS {
  SDelFile *pDelFile;
  SArray   *aDFileSet;  // SArray<SDFileSet>
};

struct STsdb {
  char          *path;
  SVnode        *pVnode;
  STsdbKeepCfg   keepCfg;
  TdThreadRwlock rwLock;

  // memtable
  SMemTable *mem;
  SMemTable *imem;

  // file system
  volatile int64_t id;
  STsdbFileSystem *pFS;
  STsdbFileSystem *pFSN;

  STsdbFS fs;  // todo: delete

  // cache
  SLRUCache    *lruCache;
  TdThreadMutex lruMutex;
};

struct TSDBKEY {
  int64_t version;
  TSKEY   ts;
};

struct SVersionRange {
  uint64_t minVer;
  uint64_t maxVer;
};

typedef struct SMemSkipListNode SMemSkipListNode;
struct SMemSkipListNode {
  int8_t            level;
  SMemSkipListNode *forwards[0];
};
typedef struct SMemSkipList {
  int64_t           size;
  uint32_t          seed;
  int8_t            maxLevel;
  int8_t            level;
  SMemSkipListNode *pHead;
  SMemSkipListNode *pTail;
} SMemSkipList;

struct STbData {
  tb_uid_t     suid;
  tb_uid_t     uid;
  TSKEY        minKey;
  TSKEY        maxKey;
  SDelData    *pHead;
  SDelData    *pTail;
  SMemSkipList sl;
  STbData     *next;
};

struct SMemTable {
  SRWLatch         latch;
  STsdb           *pTsdb;
  SVBufPool       *pPool;
  volatile int32_t nRef;
  TSKEY            minKey;
  TSKEY            maxKey;
  int64_t          nRow;
  int64_t          nDel;
  struct {
    int32_t   nTbData;
    int32_t   nBucket;
    STbData **aBucket;
  };
};

struct TSDBROW {
  int8_t type;  // 0 for row from tsRow, 1 for row from block data
  union {
    struct {
      int64_t version;
      STSRow *pTSRow;
    };
    struct {
      SBlockData *pBlockData;
      int32_t     iRow;
    };
  };
};

struct SBlockIdx {
  int64_t suid;
  int64_t uid;
  int64_t offset;
  int64_t size;
};

struct SMapData {
  int32_t  nItem;
  int32_t  nData;
  int32_t *aOffset;
  uint8_t *pData;
};

struct SBlockCol {
  int16_t cid;
  int8_t  type;
  int8_t  smaOn;
  int8_t  flag;      // HAS_NONE|HAS_NULL|HAS_VALUE
  int32_t szOrigin;  // original column value size (only save for variant data type)
  int32_t szBitmap;  // bitmap size, 0 only for flag == HAS_VAL
  int32_t szOffset;  // offset size, 0 only for non-variant-length type
  int32_t szValue;   // value size, 0 when flag == (HAS_NULL | HAS_NONE)
  int32_t offset;
};

struct SBlockInfo {
  int64_t offset;  // block data offset
  int32_t szBlock;
  int32_t szKey;
};

struct SSmaInfo {
  int64_t offset;
  int32_t size;
};

struct SBlkInfo {
  int64_t minUid;
  int64_t maxUid;
  TSKEY   minKey;
  TSKEY   maxKey;
  int64_t minVer;
  int64_t maxVer;
  TSDBKEY minTKey;
  TSDBKEY maxTKey;
};

struct SDataBlk {
  TSDBKEY    minKey;
  TSDBKEY    maxKey;
  int64_t    minVer;
  int64_t    maxVer;
  int32_t    nRow;
  int8_t     hasDup;
  int8_t     nSubBlock;
  SBlockInfo aSubBlock[TSDB_MAX_SUBBLOCKS];
  SSmaInfo   smaInfo;
};

struct SSttBlk {
  int64_t    suid;
  int64_t    minUid;
  int64_t    maxUid;
  TSKEY      minKey;
  TSKEY      maxKey;
  int64_t    minVer;
  int64_t    maxVer;
  int32_t    nRow;
  SBlockInfo bInfo;
};

// (SBlockData){.suid = 0, .uid = 0}: block data not initialized
// (SBlockData){.suid = suid, .uid = uid}: block data for ONE child table int .data file
// (SBlockData){.suid = suid, .uid = 0}: block data for N child tables int .last file
// (SBlockData){.suid = 0, .uid = uid}: block data for 1 normal table int .last/.data file
struct SBlockData {
  int64_t  suid;      // 0 means normal table block data, otherwise child table block data
  int64_t  uid;       // 0 means block data in .last file, otherwise in .data file
  int32_t  nRow;      // number of rows
  int64_t *aUid;      // uids of each row, only exist in block data in .last file (uid == 0)
  int64_t *aVersion;  // versions of each row
  TSKEY   *aTSKEY;    // timestamp of each row
  SArray  *aIdx;      // SArray<int32_t>
  SArray  *aColData;  // SArray<SColData>
};

struct TABLEID {
  tb_uid_t suid;
  tb_uid_t uid;
};

struct STbDataIter {
  STbData          *pTbData;
  int8_t            backward;
  SMemSkipListNode *pNode;
  TSDBROW          *pRow;
  TSDBROW           row;
};

struct SDelData {
  int64_t   version;
  TSKEY     sKey;
  TSKEY     eKey;
  SDelData *pNext;
};

struct SDelIdx {
  tb_uid_t suid;
  tb_uid_t uid;
  int64_t  offset;
  int64_t  size;
};

struct SDiskDataHdr {
  uint32_t delimiter;
  uint32_t fmtVer;
  int64_t  suid;
  int64_t  uid;
  int32_t  szUid;
  int32_t  szVer;
  int32_t  szKey;
  int32_t  szBlkCol;
  int32_t  nRow;
  int8_t   cmprAlg;
};

struct SDelFile {
  volatile int32_t nRef;

  int64_t commitID;
  int64_t size;
  int64_t offset;
};

struct SHeadFile {
  volatile int32_t nRef;

  int64_t commitID;
  int64_t size;
  int64_t offset;
};

struct SDataFile {
  volatile int32_t nRef;

  int64_t commitID;
  int64_t size;
};

struct SSttFile {
  volatile int32_t nRef;

  int64_t commitID;
  int64_t size;
  int64_t offset;
};

struct SSmaFile {
  volatile int32_t nRef;

  int64_t commitID;
  int64_t size;
};

struct SDFileSet {
  SDiskID    diskId;
  int32_t    fid;
  SHeadFile *pHeadF;
  SDataFile *pDataF;
  SSmaFile  *pSmaF;
  uint8_t    nSttF;
  SSttFile  *aSttF[TSDB_MAX_STT_TRIGGER];
};

struct SRowIter {
  TSDBROW  *pRow;
  STSchema *pTSchema;
  SColVal   colVal;
  int32_t   i;
};
struct SRowMerger {
  STSchema *pTSchema;
  int64_t   version;
  SArray   *pArray;  // SArray<SColVal>
};

typedef struct {
  char     *path;
  int32_t   szPage;
  int32_t   flag;
  TdFilePtr pFD;
  int64_t   pgno;
  uint8_t  *pBuf;
  int64_t   szFile;
} STsdbFD;

struct SDelFWriter {
  STsdb   *pTsdb;
  SDelFile fDel;
  STsdbFD *pWriteH;
  uint8_t *aBuf[1];
};

struct STsdbReadSnap {
  SMemTable *pMem;
  SMemTable *pIMem;
  STsdbFS    fs;
};

struct SDataFWriter {
  STsdb    *pTsdb;
  SDFileSet wSet;

  STsdbFD *pHeadFD;
  STsdbFD *pDataFD;
  STsdbFD *pSmaFD;
  STsdbFD *pSttFD;

  SHeadFile fHead;
  SDataFile fData;
  SSmaFile  fSma;
  SSttFile  fStt[TSDB_MAX_STT_TRIGGER];

  uint8_t *aBuf[4];
};

struct SDataFReader {
  STsdb     *pTsdb;
  SDFileSet *pSet;
  STsdbFD   *pHeadFD;
  STsdbFD   *pDataFD;
  STsdbFD   *pSmaFD;
  STsdbFD   *aSttFD[TSDB_MAX_STT_TRIGGER];
  uint8_t   *aBuf[3];
};

typedef struct {
  int64_t suid;
  int64_t uid;
  TSDBROW row;
} SRowInfo;

typedef struct SSttBlockLoadInfo {
  SBlockData blockData[2];
  SArray    *aSttBlk;
  int32_t    blockIndex[2];  // to denote the loaded block in the corresponding position.
  int32_t    currentLoadBlockIndex;
  int32_t    loadBlocks;
  double     elapsedTime;
  STSchema  *pSchema;
  int16_t   *colIds;
  int32_t    numOfCols;
  bool       sttBlockLoaded;
} SSttBlockLoadInfo;

typedef struct SMergeTree {
  int8_t             backward;
  SRBTree            rbt;
  SArray            *pIterList;
  SLDataIter        *pIter;
  bool               destroyLoadInfo;
  SSttBlockLoadInfo *pLoadInfo;
  const char        *idStr;
} SMergeTree;

typedef struct {
  int64_t   suid;
  int64_t   uid;
  STSchema *pTSchema;
} SSkmInfo;

struct SDiskCol {
  SBlockCol      bCol;
  const uint8_t *pBit;
  const uint8_t *pOff;
  const uint8_t *pVal;
  SColumnDataAgg agg;
};

struct SDiskData {
  SDiskDataHdr   hdr;
  const uint8_t *pUid;
  const uint8_t *pVer;
  const uint8_t *pKey;
  SArray        *aDiskCol;  // SArray<SDiskCol>
};

struct SDiskDataBuilder {
  int64_t      suid;
  int64_t      uid;
  int32_t      nRow;
  uint8_t      cmprAlg;
  uint8_t      calcSma;
  SCompressor *pUidC;
  SCompressor *pVerC;
  SCompressor *pKeyC;
  int32_t      nBuilder;
  SArray      *aBuilder;  // SArray<SDiskColBuilder>
  uint8_t     *aBuf[2];
  SDiskData    dd;
  SBlkInfo     bi;
};

int32_t tMergeTreeOpen(SMergeTree *pMTree, int8_t backward, SDataFReader *pFReader, uint64_t suid, uint64_t uid,
                       STimeWindow *pTimeWindow, SVersionRange *pVerRange, SSttBlockLoadInfo *pBlockLoadInfo,
                       bool destroyLoadInfo, const char *idStr);
void    tMergeTreeAddIter(SMergeTree *pMTree, SLDataIter *pIter);
bool    tMergeTreeNext(SMergeTree *pMTree);
TSDBROW tMergeTreeGetRow(SMergeTree *pMTree);
void    tMergeTreeClose(SMergeTree *pMTree);

SSttBlockLoadInfo *tCreateLastBlockLoadInfo(STSchema *pSchema, int16_t *colList, int32_t numOfCols);
void               resetLastBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo);
void               getLastBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo, int64_t *blocks, double *el);
void              *destroyLastBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo);

// tsdbCache ==============================================================================================
typedef struct SCacheRowsReader {
  SVnode   *pVnode;
  STSchema *pSchema;
  uint64_t  uid;
  uint64_t  suid;
  char    **transferBuf;  // todo remove it soon
  int32_t   numOfCols;
  int32_t   type;
  int32_t   tableIndex;  // currently returned result tables

  STableKeyInfo     *pTableList;  // table id list
  int32_t            numOfTables;
  SSttBlockLoadInfo *pLoadInfo;
  STsdbReadSnap     *pReadSnap;
  SDataFReader      *pDataFReader;
  SDataFReader      *pDataFReaderLast;
} SCacheRowsReader;

typedef struct {
  TSKEY   ts;
  SColVal colVal;
} SLastCol;

int32_t tsdbOpenCache(STsdb *pTsdb);
void    tsdbCloseCache(STsdb *pTsdb);
int32_t tsdbCacheInsertLast(SLRUCache *pCache, tb_uid_t uid, STSRow *row, STsdb *pTsdb);
int32_t tsdbCacheInsertLastrow(SLRUCache *pCache, STsdb *pTsdb, tb_uid_t uid, STSRow *row, bool dup);
int32_t tsdbCacheGetLastH(SLRUCache *pCache, tb_uid_t uid, SCacheRowsReader *pr, LRUHandle **h);
int32_t tsdbCacheGetLastrowH(SLRUCache *pCache, tb_uid_t uid, SCacheRowsReader *pr, LRUHandle **h);
int32_t tsdbCacheRelease(SLRUCache *pCache, LRUHandle *h);

int32_t tsdbCacheDeleteLastrow(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey);
int32_t tsdbCacheDeleteLast(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey);
int32_t tsdbCacheDelete(SLRUCache *pCache, tb_uid_t uid, TSKEY eKey);

void   tsdbCacheSetCapacity(SVnode *pVnode, size_t capacity);
size_t tsdbCacheGetCapacity(SVnode *pVnode);

int32_t tsdbCacheLastArray2Row(SArray *pLastArray, STSRow **ppRow, STSchema *pSchema);

// ========== inline functions ==========
static FORCE_INLINE int32_t tsdbKeyCmprFn(const void *p1, const void *p2) {
  TSDBKEY *pKey1 = (TSDBKEY *)p1;
  TSDBKEY *pKey2 = (TSDBKEY *)p2;

  if (pKey1->ts < pKey2->ts) {
    return -1;
  } else if (pKey1->ts > pKey2->ts) {
    return 1;
  }

  if (pKey1->version < pKey2->version) {
    return -1;
  } else if (pKey1->version > pKey2->version) {
    return 1;
  }

  return 0;
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_TSDB_H_*/
