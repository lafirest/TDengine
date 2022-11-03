/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define TSDB_FTYPE_HEAD 0  // .head
#define TSDB_FTYPE_DATA 1  // .data
#define TSDB_FTYPE_SMA  2  // .sma
#define TSDB_FTYPE_STT  3  // .stt
#define TSDB_FTYPE_DEL  4  // .del
#define TSDB_FTYPE_MAX  5

typedef struct TSDBFILE        TSDBFILE;
typedef struct STsdbFile       STsdbFile;
typedef struct STsdbFileOp     STsdbFileOp;
typedef struct STsdbFileWriter STsdbFileWriter;
typedef struct STsdbFileReader STsdbFileReader;
typedef struct STsdbFileObj    STsdbFileObj;
typedef struct STsdbFileArray  STsdbFileArray;
typedef struct STsdbFileGroup  STsdbFileGroup;
typedef struct STsdbFileSystem STsdbFileSystem;

// TSDBFILE ======================================================
struct TSDBFILE {
  char     *name;
  int32_t   szPage;
  int32_t   flags;
  int64_t   pgno;
  uint8_t  *pBuf;
  TdFilePtr pFD;
  int64_t   szFile;
};

int32_t tsdbFOpen(const char *fName, int32_t szPage, int32_t flags, TSDBFILE **ppFILE);
int32_t tsdbFClose(TSDBFILE **ppFILE, int8_t flush);
int32_t tsdbFWrite(TSDBFILE *pFILE, int64_t loffset, const uint8_t *pBuf, int64_t size);
int32_t tsdbFRead(TSDBFILE *pFILE, int64_t loffset, uint8_t *pBuf, int64_t size);

// STsdbFile ======================================================
struct STsdbFile {
  int32_t ftype;
  SDiskID did;
  int32_t fid;
  int64_t id;
  int64_t size;
  int64_t offset;
};

bool tsdbIsSameFile(const STsdbFile *pFile1, const STsdbFile *pFile2);

// STsdbFileOp ======================================================
typedef enum {
  TSDB_FOP_ADD = 0, /* ADD FILE */
  TSDB_FOP_REMOVE,  /* REMOVE FILE */
  TSDB_FOP_MOD      /* MODIFY FILE */
} ETsdbFileOpT;

struct STsdbFileOp {
  ETsdbFileOpT op;
  STsdbFile    file;
};

int32_t tsdbFileOpCreate(ETsdbFileOpT op, const STsdbFile *pFile, STsdbFileOp **ppFileOp);
void    tsdbFileOpDestroy(STsdbFileOp **ppFileOp);

// STsdbFileWriter ======================================================
struct STsdbFileWriter {
  STsdb     *pTsdb;
  STsdbFile *pf;
  TSDBFILE  *pFILE;
};

int32_t tsdbFileWriterOpen(STsdb *pTsdb, STsdbFile *pFile, STsdbFileWriter **ppWriter);
int32_t tsdbFileWriterClose(STsdbFileWriter **ppWriter, int8_t flush);
int32_t tsdbFileAppend(STsdbFileWriter *pWriter, const uint8_t *pBuf, int64_t size);

// STsdbFileObj ======================================================
struct STsdbFileObj {
  volatile int32_t nRef;
  STsdbFile        file;
};

// STsdbFileReader ======================================================
struct STsdbFileReader {
  STsdbFile file;
  TSDBFILE *pFILE;
};

// STsdbFileArray ======================================================
struct STsdbFileArray {
  int32_t level;
  SArray *aFileObj;
};

// STsdbFileGroup ======================================================
struct STsdbFileGroup {
  int32_t       fid;
  STsdbFileObj *fHead;
  STsdbFileObj *fData;
  STsdbFileObj *fSma;
  SArray       *aFStt;  // SArray<STsdbFileObj *>
};

// STsdbFileSystem ======================================================
struct STsdbFileSystem {
  STsdbFileObj *fDel;
  SArray       *aFileGroup;  // SArray<SFileGroup>
};

int32_t tsdbOpenFS(STsdb *pTsdb, int8_t rollback);
int32_t tsdbCloseFS(STsdb *pTsdb);
int32_t tsdbPrepareFS(STsdb *pTsdb, SArray *aFileOpP);
int32_t tsdbCommitFS(STsdb *pTsdb);
int32_t tsdbRollbackFS(STsdb *pTsdb);
int64_t tsdbNextFileID(STsdb *pTsdb);
bool    tsdbShouldMerge(STsdb *pTsdb);