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

#include "tsdb.h"

// TSDBFILE ==========================================
#define TSDB_PAGE_CONTENT_SIZE(PAGE) ((PAGE) - sizeof(TSCKSUM))
#define TSDB_LOFFSET_TO_OFFSET(LOFFSET, PAGE) \
  ((LOFFSET) / TSDB_PAGE_CONTENT_SIZE(PAGE) * (PAGE) + (LOFFSET) % TSDB_PAGE_CONTENT_SIZE(PAGE))
#define TSDB_OFFSET_TO_LOFFSET(OFFSET, PAGE) ((OFFSET) / (PAGE)*TSDB_PAGE_CONTENT_SIZE(PAGE) + (OFFSET) % (PAGE))
#define TSDB_PAGE_OFFSET(PGNO, PAGE)         (((PGNO)-1) * (PAGE))
#define TSDB_OFFSET_PGNO(OFFSET, PAGE)       ((OFFSET) / (PAGE) + 1)

static int32_t tsdbFWritePage(TSDBFILE *pFILE) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pFILE->pgno <= 0) return code;

  int64_t n = taosLSeekFile(pFILE->pFD, TSDB_PAGE_OFFSET(pFILE->pgno, pFILE->szPage), SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosCalcChecksumAppend(0, pFILE->pBuf, pFILE->szPage);

  n = taosWriteFile(pFILE->pFD, pFILE->pBuf, pFILE->szPage);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pFILE->szFile < pFILE->pgno) {
    pFILE->szFile = pFILE->pgno;
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s, fName:%s pgno:%" PRId64 " szPage:%d", __func__, lino, tstrerror(code),
              pFILE->name, pFILE->pgno, pFILE->szPage);
  } else {
    tsdbTrace("%s done, fName:%s pgno:%" PRId64 " szPage:%d", __func__, pFILE->name, pFILE->pgno, pFILE->szPage);
    pFILE->pgno = 0;
  }
  return code;
}

static int32_t tsdbFReadPage(TSDBFILE *pFILE, int64_t pgno) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(pgno > 0 && pgno <= pFILE->szFile);

  // seek
  int64_t n = taosLSeekFile(pFILE->pFD, TSDB_PAGE_OFFSET(pgno, pFILE->szPage), SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // read
  n = taosReadFile(pFILE->pFD, pFILE->pBuf, pFILE->szPage);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (n < pFILE->szPage) {
    code = TSDB_CODE_FILE_CORRUPTED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // check
  if (pgno > 1 && !taosCheckChecksumWhole(pFILE->pBuf, pFILE->szPage)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pFILE->pgno = pgno;

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s, fName:%s pgno:%" PRId64 " szPage:%d", __func__, lino, tstrerror(code),
              pFILE->name, pgno, pFILE->szPage);
  } else {
    tsdbTrace("%s done, fName:%s pgno:%" PRId64 " szPage:%d", __func__, pFILE->name, pgno, pFILE->szPage);
  }
  return code;
}

static int32_t tsdbFFlush(TSDBFILE *pFILE) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFWritePage(pFILE);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (taosFsyncFile(pFILE->pFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s, fName:%s szPage:%d", __func__, lino, tstrerror(code), pFILE->name,
              pFILE->szPage);
  } else {
    tsdbTrace("%s done", __func__);
  }
  return code;
}

int32_t tsdbFOpen(const char *fName, int32_t szPage, int32_t flags, TSDBFILE **ppFILE) {
  int32_t code = 0;
  int32_t lino = 0;

  TSDBFILE *pFILE = (TSDBFILE *)taosMemoryCalloc(1, sizeof(*pFILE) + strlen(fName) + 1);
  if (NULL == pFILE) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pFILE->name = (char *)&pFILE[1];
  strcpy(pFILE->name, fName);
  pFILE->szPage = szPage;
  pFILE->flags = flags;
  pFILE->pgno = 0;

  pFILE->pBuf = taosMemoryMalloc(szPage);
  if (NULL == pFILE->pBuf) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pFILE->pFD = taosOpenFile(fName, flags);
  if (NULL == pFILE->pFD) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (taosStatFile(fName, &pFILE->szFile, NULL) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  ASSERT(pFILE->szFile % szPage == 0);
  pFILE->szFile = pFILE->szFile / szPage;

_exit:
  if (code) {
    if (pFILE) {
      if (pFILE->pFD) {
        taosCloseFile(&pFILE->pFD);
      }
      if (pFILE->pBuf) {
        taosMemoryFree(pFILE->pBuf);
      }
      taosMemoryFree(pFILE);
    }

    *ppFILE = NULL;
    tsdbError("%s failed at line %d since %s, fName:%s flags:%d", __func__, lino, tstrerror(code), fName, flags);
  } else {
    *ppFILE = pFILE;
    tsdbTrace("%s done, fName:%s flags:%d", __func__, fName, flags);
  }
  return code;
}

int32_t tsdbFClose(TSDBFILE **ppFILE, int8_t flush) {
  int32_t code = 0;
  int32_t lino = 0;

  TSDBFILE *pFILE = *ppFILE;

  if (NULL == pFILE) {
    return code;
  }

  if (flush) {
    code = tsdbFFlush(pFILE);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosCloseFile(&pFILE->pFD);

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s, fName:%s szPage:%d", __func__, lino, tstrerror(code), pFILE->name,
              pFILE->szPage);
  } else {
    tsdbTrace("%s done, fName:%s", __func__, pFILE->name);
  }
  taosMemoryFree(pFILE->pBuf);
  taosMemoryFree(pFILE);
  *ppFILE = NULL;
  return code;
}

int32_t tsdbFWrite(TSDBFILE *pFILE, int64_t loffset, const uint8_t *pBuf, int64_t size) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t offset = TSDB_LOFFSET_TO_OFFSET(loffset, pFILE->szPage);
  int64_t pgno = TSDB_OFFSET_PGNO(offset, pFILE->szPage);
  int64_t bOffset = offset % pFILE->szPage;
  int64_t n = 0;
  do {
    if (pFILE->pgno != pgno) {
      code = tsdbFWritePage(pFILE);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pgno <= pFILE->szFile) {
        code = tsdbFReadPage(pFILE, pgno);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        pFILE->pgno = pgno;
      }
    }

    int64_t nWrite = TMIN(TSDB_PAGE_CONTENT_SIZE(pFILE->szPage) - bOffset, size - n);
    memcpy(pFILE->pBuf + bOffset, pBuf + n, nWrite);

    pgno++;
    bOffset = 0;
    n += nWrite;
  } while (n < size);

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s, fName:%s szPage:%d loffset:%" PRId64 " size:%" PRId64, __func__, lino,
              tstrerror(code), pFILE->name, pFILE->szPage, loffset, size);
  } else {
    tsdbTrace("%s done, fName:%s szPage:%d loffset:%" PRId64 " size:%" PRId64, __func__, pFILE->name, pFILE->szPage,
              loffset, size);
  }
  return code;
}

int32_t tsdbFRead(TSDBFILE *pFILE, int64_t loffset, uint8_t *pBuf, int64_t size) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t n = 0;
  int64_t fOffset = TSDB_LOFFSET_TO_OFFSET(loffset, pFILE->szPage);
  int64_t pgno = TSDB_OFFSET_PGNO(fOffset, pFILE->szPage);
  int32_t szPgCont = TSDB_PAGE_CONTENT_SIZE(pFILE->szPage);
  int64_t bOffset = fOffset % pFILE->szPage;

  ASSERT(pgno && pgno <= pFILE->szFile);
  ASSERT(bOffset < szPgCont);

  while (n < size) {
    if (pFILE->pgno != pgno) {
      code = tsdbFReadPage(pFILE, pgno);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    int64_t nRead = TMIN(szPgCont - bOffset, size - n);
    memcpy(pBuf + n, pFILE->pBuf + bOffset, nRead);

    n += nRead;
    pgno++;
    bOffset = 0;
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s, fName:%s szPage:%d loffset%" PRId64 " size:%" PRId64, __func__, lino,
              tstrerror(code), pFILE->name, pFILE->szPage, loffset, size);
  } else {
    tsdbTrace("%s done, fName:%s szPage:%d loffset%" PRId64 " size:%" PRId64, __func__, pFILE->name, pFILE->szPage,
              loffset, size);
  }
  return code;
}

// STsdbFile ==========================================
static void tsdbFileName(STsdb *pTsdb, const STsdbFile *pFile, char fName[]) {
  const char *fSuffix = NULL;
  SVnode     *pVnode = pTsdb->pVnode;

  switch (pFile->ftype) {
    case TSDB_FTYPE_HEAD:
      fSuffix = ".head";
      break;
    case TSDB_FTYPE_DATA:
      fSuffix = ".data";
      break;
    case TSDB_FTYPE_SMA:
      fSuffix = ".sma";
      break;
    case TSDB_FTYPE_STT:
      fSuffix = ".stt";
      break;
    case TSDB_FTYPE_DEL:
      fSuffix = ".del";
      break;
    default:
      assert(0);
  }

  if (pVnode->pTfs) {
    snprintf(fName, TSDB_FILENAME_LEN, "%s%s%s%sv%df%dver%" PRId64 "%s", tfsGetDiskPath(pVnode->pTfs, pFile->did),
             TD_DIRSEP, pTsdb->path, TD_DIRSEP, TD_VID(pTsdb->pVnode), pFile->fid, pFile->id, fSuffix);
  } else {
    snprintf(fName, TSDB_FILENAME_LEN, "%s%sv%df%dver%" PRId64 "%s", pTsdb->path, TD_DIRSEP, TD_VID(pTsdb->pVnode),
             pFile->fid, pFile->id, fSuffix);
  }
}

static int32_t tsdbFileToBinary(uint8_t *p, STsdbFile *pFile) {
  int32_t n = 0;

  n += tPutI32(p ? p + n : p, pFile->ftype);
  n += tPutI32(p ? p + n : p, pFile->did.level);
  n += tPutI32(p ? p + n : p, pFile->did.id);
  n += tPutI32(p ? p + n : p, pFile->fid);
  n += tPutI64(p ? p + n : p, pFile->id);
  n += tPutI64(p ? p + n : p, pFile->size);
  n += tPutI64(p ? p + n : p, pFile->offset);

  return n;
}

static int32_t tsdbBinaryToFile(uint8_t *p, STsdbFile *pFile) {
  int32_t n = 0;

  n += tGetI32(p + n, &pFile->ftype);
  n += tGetI32(p + n, &pFile->did.level);
  n += tGetI32(p + n, &pFile->did.id);
  n += tGetI32(p + n, &pFile->fid);
  n += tGetI64(p + n, &pFile->id);
  n += tGetI64(p + n, &pFile->size);
  n += tGetI64(p + n, &pFile->offset);

  return n;
}

static int32_t tsdbFileToJson(const STsdbFile *pFile, SJson *pJson) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tjsonAddIntegerToObject(pJson, "type", pFile->ftype)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (tjsonAddIntegerToObject(pJson, "disk level", pFile->did.level)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (tjsonAddIntegerToObject(pJson, "disk id", pFile->did.id)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (tjsonAddIntegerToObject(pJson, "fid", pFile->fid)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (tjsonAddIntegerToObject(pJson, "id", pFile->id)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (tjsonAddIntegerToObject(pJson, "size", pFile->size)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (tjsonAddIntegerToObject(pJson, "offset", pFile->offset)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbJsonToFile(const SJson *pJson, STsdbFile *pFile) {
  int32_t code = 0;
  int32_t lino = 0;

  tjsonGetNumberValue(pJson, "type", pFile->ftype, code);
  TSDB_CHECK_CODE(code, lino, _exit);

  tjsonGetNumberValue(pJson, "disk level", pFile->did.level, code);
  TSDB_CHECK_CODE(code, lino, _exit);

  tjsonGetNumberValue(pJson, "disk id", pFile->did.id, code);
  TSDB_CHECK_CODE(code, lino, _exit);

  tjsonGetNumberValue(pJson, "fid", pFile->fid, code);
  TSDB_CHECK_CODE(code, lino, _exit);

  tjsonGetNumberValue(pJson, "id", pFile->id, code);
  TSDB_CHECK_CODE(code, lino, _exit);

  tjsonGetNumberValue(pJson, "size", pFile->size, code);
  TSDB_CHECK_CODE(code, lino, _exit);

  tjsonGetNumberValue(pJson, "offset", pFile->offset, code);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

bool tsdbIsSameFile(const STsdbFile *pFile1, const STsdbFile *pFile2) {
  if (pFile1->ftype != pFile2->ftype) {
    return false;
  }

  if ((pFile1->did.level != pFile2->did.level) || (pFile1->did.id != pFile2->did.id)) {
    return false;
  }

  if (pFile1->fid != pFile2->fid) {
    return false;
  }

  if (pFile1->id != pFile2->id) {
    return false;
  }

  return true;
}

// STsdbFileOp ==========================================
int32_t tsdbFileOpCreate(ETsdbFileOpT op, const STsdbFile *pFile, STsdbFileOp **ppFileOp) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFileOp *pFileOp = (STsdbFileOp *)taosMemoryCalloc(1, sizeof(*pFileOp));
  if (NULL == pFileOp) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pFileOp->op = op;
  pFileOp->file = *pFile;

_exit:
  if (code) {
    *ppFileOp = NULL;
    tsdbFileOpDestroy(&pFileOp);
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  } else {
    *ppFileOp = pFileOp;
  }
  return code;
}

void tsdbFileOpDestroy(STsdbFileOp **ppFileOp) {
  STsdbFileOp *pFileOp = *ppFileOp;
  if (pFileOp) {
    taosMemoryFree(pFileOp);
    *ppFileOp = NULL;
  }
}

// STsdbFileWriter ==========================================
static int32_t tsdbFileUpdateHdr(STsdbFileWriter *pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  uint8_t hdr[TSDB_FHDR_SIZE] = {0};

  // TODO

  code = tsdbFileAppend(pWriter, hdr, TSDB_FHDR_SIZE);  // todo not correct
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  } else {
    tsdbTrace("%s done", __func__);
  }
  return code;
}

int32_t tsdbFileWriterOpen(STsdb *pTsdb, STsdbFile *pFile, STsdbFileWriter **ppWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFileWriter *pWriter = (STsdbFileWriter *)taosMemoryCalloc(1, sizeof(*pWriter));
  if (NULL == pWriter) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  char fName[TSDB_FILENAME_LEN] = {0};
  tsdbFileName(pTsdb, pFile, fName);

  int32_t flags = TD_FILE_READ | TD_FILE_WRITE;
  if (0 == pFile->size) {  // need create
    flags |= (TD_FILE_CREATE | TD_FILE_TRUNC);
  }

  code = tsdbFOpen(fName, pTsdb->pVnode->config.tsdbPageSize, flags, &pWriter->pFILE);
  TSDB_CHECK_CODE(code, lino, _exit);

  pWriter->pTsdb = pTsdb;
  pWriter->pf = pFile;

  if (0 == pFile->size) {
    code = tsdbFileUpdateHdr(pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    if (pWriter) {
      // TODO
      tsdbFClose(&pWriter->pFILE, 0);
      taosMemoryFree(pWriter);
    }

    *ppWriter = NULL;
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    *ppWriter = pWriter;
  }
  return code;
}

int32_t tsdbFileWriterClose(STsdbFileWriter **ppWriter, int8_t flush) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFileWriter *pWriter = *ppWriter;
  if (pWriter) {
    if (pWriter->pFILE) {
      tsdbFClose(&pWriter->pFILE, flush);
    }
    taosMemoryFree(pWriter);
    *ppWriter = NULL;
  }

_exit:
  return code;
}

int32_t tsdbFileAppend(STsdbFileWriter *pWriter, const uint8_t *pBuf, int64_t size) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(size > 0);

  code = tsdbFWrite(pWriter->pFILE, pWriter->pf->size, pBuf, size);
  TSDB_CHECK_CODE(code, lino, _exit);

  pWriter->pf->size += size;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
  }
  return code;
}

// STsdbFileReader ======================================================
int32_t tsdbFileReaderOpen(STsdb *pTsdb, const STsdbFile *pFile, STsdbFileReader **ppReader) {
  int32_t code = 0;
  int32_t lino = 0;

  char fName[TSDB_FILENAME_LEN] = {0};
  tsdbFileName(pTsdb, pFile, fName);

  STsdbFileReader *pReader = (STsdbFileReader *)taosMemoryCalloc(1, sizeof(*pReader));
  if (NULL == pReader) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pReader->pTsdb = pTsdb;
  code = tsdbFOpen(fName, pTsdb->pVnode->config.tsdbPageSize, TD_FILE_READ, &pReader->pFILE);
  TSDB_CHECK_CODE(code, lino, _exit);
  pReader->file = *pFile;

// TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fName:%s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code),
              fName);

    *ppReader = NULL;
    if (pReader) {
      if (pReader->pFILE) {
        tsdbFClose(&pReader->pFILE, 0);
      }
      taosMemoryFree(pReader);
    }
  } else {
    *ppReader = pReader;
  }
  return code;
}

void tsdbFileReaderClose(STsdbFileReader *pReader) {
  if (pReader) {
    if (pReader->pFILE) {
      tsdbFClose(&pReader->pFILE, 0);
    }
    taosMemoryFree(pReader);
  }
}

int32_t tsdbFileRead(STsdbFileReader *pReader, int64_t loffset, uint8_t *pBuf, int64_t size) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(loffset >= 0 && size > 0);
  ASSERT(loffset + size <= pReader->file.size);

  int64_t n = tsdbFRead(pReader->pFILE, loffset, pBuf, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pReader->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

// STsdbFileObj ==========================================
static int32_t tsdbNewFileObj(const STsdbFile *pFile, STsdbFileObj **ppFileObj) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFileObj *pFileObj = (STsdbFileObj *)taosMemoryCalloc(1, sizeof(*pFileObj));
  if (NULL == pFileObj) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pFileObj->nRef = 1;
  pFileObj->file = *pFile;

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    *ppFileObj = NULL;
  } else {
    *ppFileObj = pFileObj;
  }
  return code;
}

static void tsdbFreeFileObj(STsdb *pTsdb, STsdbFileObj *pFileObj, int8_t remove) {
  if (pFileObj) {
    if (remove) {
      char fName[TSDB_FILENAME_LEN] = {0};
      tsdbFileName(pTsdb, &pFileObj->file, fName);
      (void)taosRemoveFile(fName);
      tsdbDebug("vgId:%d %s, remove file:%s", TD_VID(pTsdb->pVnode), __func__, fName);
    }
    taosMemoryFree(pFileObj);
  }
}

static int32_t tsdbRefFileObj(STsdb *pTsdb, STsdbFileObj *pFileObj) {
  int32_t nRef = atomic_fetch_add_32(&pFileObj->nRef, 1);
  ASSERT(nRef > 0);
  return nRef;
}

static int32_t tsdbUnrefFileObj(STsdb *pTsdb, STsdbFileObj *pFileObj, int8_t remove) {
  int32_t nRef = atomic_sub_fetch_32(&pFileObj->nRef, 1);
  if (0 == nRef) {
    tsdbFreeFileObj(pTsdb, pFileObj, remove);
  }
  return nRef;
}

static int32_t tsdbFileObjPToJson(const STsdbFileObj **ppFileObj, SJson *pJson) {
  return tsdbFileToJson(&(*ppFileObj)->file, pJson);
}

static int32_t tsdbJsonToFileObjP(const SJson *pJson, STsdbFileObj **ppFileObj) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFile file = {0};
  tsdbJsonToFile(pJson, &file);

  code = tsdbNewFileObj(&file, ppFileObj);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// STsdbFileGroup ==========================================
static int32_t tsdbFileGroupCmprFn(const void *p1, const void *p2) {
  STsdbFileGroup *pFg1 = (STsdbFileGroup *)p1;
  STsdbFileGroup *pFg2 = (STsdbFileGroup *)p2;

  if (pFg1->fid < pFg2->fid) {
    return -1;
  } else if (pFg1->fid > pFg2->fid) {
    return 1;
  }

  return 0;
}

static int32_t tsdbFileGroupToJson(const STsdbFileGroup *pFg, SJson *pJson) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tjsonAddIntegerToObject(pJson, "fid", pFg->fid)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pFg->fHead) {
    if (tjsonAddObject(pJson, "head", (FToJson)tsdbFileObjPToJson, &pFg->fHead)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (pFg->fData) {
    if (tjsonAddObject(pJson, "data", (FToJson)tsdbFileObjPToJson, &pFg->fData)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (pFg->fSma) {
    if (tjsonAddObject(pJson, "sma", (FToJson)tsdbFileObjPToJson, &pFg->fData)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (tjsonAddTArray(pJson, "stt", (FToJson)tsdbFileObjPToJson, pFg->aFStt)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbJsonToFileGroup(const SJson *pJson, STsdbFileGroup *pFg) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tjsonGetIntValue(pJson, "fid", &pFg->fid)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  STsdbFile file = {0};
  if (0 == tjsonToObject(pJson, "head", (FToObject)tsdbJsonToFile, &file)) {
    code = tsdbNewFileObj(&file, &pFg->fHead);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (0 == tjsonToObject(pJson, "data", (FToObject)tsdbJsonToFile, &file)) {
    code = tsdbNewFileObj(&file, &pFg->fData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (0 == tjsonToObject(pJson, "sma", (FToObject)tsdbJsonToFile, &file)) {
    code = tsdbNewFileObj(&file, &pFg->fSma);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tjsonToTArray(pJson, "stt", (FToObject)tsdbJsonToFileObjP, &pFg->aFStt, sizeof(STsdbFileObj *));
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// STsdbFileSystem ==========================================
static int32_t tsdbNewFileSystem(STsdbFileSystem **ppFS) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFileSystem *pFS = (STsdbFileSystem *)taosMemoryCalloc(1, sizeof(*pFS));
  if (NULL == pFS) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pFS->aFileGroup = taosArrayInit(0, sizeof(STsdbFileGroup));
  if (NULL == pFS->aFileGroup) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    *ppFS = NULL;
    if (pFS) {
      if (pFS->aFileGroup) {
        taosArrayDestroy(pFS->aFileGroup);
      }
      taosMemoryFree(pFS);
    }
  } else {
    *ppFS = pFS;
  }
  return code;
}

static void tsdbFreeFileSystem(STsdbFileSystem *pFS) {
  if (pFS) {
    ASSERT(pFS->fDel == NULL);
    if (pFS->aFileGroup) {
      ASSERT(taosArrayGetSize(pFS->aFileGroup) == 0);
      taosArrayDestroy(pFS->aFileGroup);
      pFS->aFileGroup = NULL;
    }
    taosMemoryFree(pFS);
  }
}

static void tsdbEmptyFS(STsdbFileSystem *pFS) {
  if (pFS->fDel) {
    tsdbUnrefFileObj(NULL, pFS->fDel, 0);
    pFS->fDel = NULL;
  }

  for (int32_t iFg = 0; iFg < taosArrayGetSize(pFS->aFileGroup); iFg++) {
    // todo
  }
  taosArrayClear(pFS->aFileGroup);
}

static int32_t tsdbFileSystemToJson(const STsdbFileSystem *pFS, SJson *pJson) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pFS->fDel) {
    if (tjsonAddObject(pJson, "tombstone", (FToJson)tsdbFileToJson, &pFS->fDel->file) < 0) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }
  tjsonAddTArray(pJson, "time-series", (FToJson)tsdbFileGroupToJson, pFS->aFileGroup);

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbJsonToFileSystem(const SJson *pJson, STsdbFileSystem *pFS) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFile file;
  if (0 == tjsonToObject(pJson, "tombstone", (FToObject)tsdbJsonToFile, &file)) {
    code = tsdbNewFileObj(&file, &pFS->fDel);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  SArray *aFileGroup = NULL;
  tjsonToTArray(pJson, "time-series", (FToObject)tsdbJsonToFileGroup, &aFileGroup, sizeof(STsdbFileGroup));

  if (aFileGroup) {
    for (int32_t iFg = 0; iFg < taosArrayGetSize(aFileGroup); iFg++) {
      STsdbFileGroup *pFg = (STsdbFileGroup *)taosArrayGet(aFileGroup, iFg);
      if (NULL == taosArrayPush(pFS->aFileGroup, pFg)) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (aFileGroup) {
    taosArrayDestroy(aFileGroup);
  }
  return code;
}

static void tsdbCurrentFileName(STsdb *pTsdb, char current[], char current_t[]) {
  SVnode *pVnode = pTsdb->pVnode;
  if (pVnode->pTfs) {
    if (current) {
      snprintf(current, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT", tfsGetPrimaryPath(pTsdb->pVnode->pTfs), TD_DIRSEP,
               pTsdb->path, TD_DIRSEP);
    }
    if (current_t) {
      snprintf(current_t, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT.t", tfsGetPrimaryPath(pTsdb->pVnode->pTfs), TD_DIRSEP,
               pTsdb->path, TD_DIRSEP);
    }
  } else {
    if (current) {
      snprintf(current, TSDB_FILENAME_LEN - 1, "%s%sCURRENT", pTsdb->path, TD_DIRSEP);
    }
    if (current_t) {
      snprintf(current_t, TSDB_FILENAME_LEN - 1, "%s%sCURRENT.t", pTsdb->path, TD_DIRSEP);
    }
  }
}

static int32_t tsdbSaveFileSystemToFile(STsdb *pTsdb, const STsdbFileSystem *pFS, const char *fName) {
  int32_t code = 0;
  int32_t lino = 0;

  SJson    *pJson = NULL;
  char     *jsonStr = NULL;
  TdFilePtr pFD = NULL;

  // json encode
  pJson = tjsonCreateObject();
  if (NULL == pJson) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFileSystemToJson(pFS, pJson);
  TSDB_CHECK_CODE(code, lino, _exit);

  jsonStr = tjsonToString(pJson);
  if (NULL == jsonStr) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // write
  pFD = taosOpenFile(fName, TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (NULL == pFD) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (taosWriteFile(pFD, jsonStr, strlen(jsonStr)) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (taosFsyncFile(pFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }

  if (NULL != pFD) {
    taosCloseFile(&pFD);
  }
  if (pJson) {
    tjsonDelete(pJson);
  }
  if (jsonStr) {
    taosMemoryFree(jsonStr);
  }

  return code;
}

static int32_t tsdbLoadFileSystemFromFile(STsdb *pTsdb, const char *fName, STsdbFileSystem *pFS) {
  int32_t code = 0;
  int32_t lino = 0;

  TdFilePtr pFD = NULL;
  char     *jsonStr = NULL;
  SJson    *pJson = NULL;
  int64_t   size = 0;

  // read file and parse json
  pFD = taosOpenFile(fName, TD_FILE_READ);
  if (NULL == pFD) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (taosFStatFile(pFD, &size, NULL) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  jsonStr = taosMemoryMalloc(size + 1);
  if (NULL == jsonStr) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (taosReadFile(pFD, jsonStr, size) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  jsonStr[size] = '\0';

  pJson = tjsonParse(jsonStr);
  if (NULL == pJson) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // json to file system
  code = tsdbJsonToFileSystem(pJson, pFS);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }

  if (pFD) {
    taosCloseFile(&pFD);
  }
  if (jsonStr) {
    taosMemoryFree(jsonStr);
  }
  if (pJson) {
    tjsonDelete(pJson);
  }

  return code;
}

static int32_t tsdbScanAndTryFixFileSystem(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static STsdbFileGroup *tsdbFileSystemGetFileGroup(STsdbFileSystem *pFS, int32_t fid, int32_t flags) {
  if (NULL == pFS->aFileGroup) {
    return NULL;
  }

  STsdbFileGroup fg = {.fid = fid};
  return (STsdbFileGroup *)taosArraySearch(pFS->aFileGroup, &fg, tsdbFileGroupCmprFn, flags);
}

static int32_t tsdbFileSystemApplyOp(STsdb *pTsdb, STsdbFileSystem *pFS, const STsdbFileOp *pOp) {
  int32_t code = 0;
  int32_t lino = 0;

  if (TSDB_FTYPE_DEL == pOp->file.ftype) {
    // TODO
    ASSERT(0);
  } else {
    STsdbFileGroup *pFg = tsdbFileSystemGetFileGroup(pFS, pOp->file.fid, TD_EQ);

    if (TSDB_FOP_ADD == pOp->op) {
      // TSDB_FOP_ADD

      STsdbFileObj *pFileObj = NULL;
      code = tsdbNewFileObj(&pOp->file, &pFileObj);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (NULL == pFg) {
        STsdbFileGroup fg = {.fid = pOp->file.fid};

        pFg = taosArrayPush(pFS->aFileGroup, &fg);
        if (NULL == pFg) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }

      switch (pOp->file.ftype) {
        case TSDB_FTYPE_HEAD:
          pFg->fHead = pFileObj;
          break;
        case TSDB_FTYPE_DATA:
          pFg->fData = pFileObj;
          break;
        case TSDB_FTYPE_SMA:
          pFg->fSma = pFileObj;
          break;
        case TSDB_FTYPE_STT:
          if (NULL == pFg->aFStt) {
            pFg->aFStt = taosArrayInit(0, sizeof(STsdbFileObj *));
            if (NULL == pFg->aFStt) {
              code = TSDB_CODE_OUT_OF_MEMORY;
              TSDB_CHECK_CODE(code, lino, _exit);
            }
          }
          if (NULL == taosArrayPush(pFg->aFStt, &pFileObj)) {
            code = TSDB_CODE_OUT_OF_MEMORY;
            TSDB_CHECK_CODE(code, lino, _exit);
          }
          break;
        default:
          ASSERT(0);
          break;
      }
    } else {
      ASSERT(pFg);

      if (TSDB_FOP_REMOVE == pOp->op) {
        switch (pOp->file.ftype) {
          case TSDB_FTYPE_HEAD:
            ASSERT(pFg->fHead);
            tsdbUnrefFileObj(pTsdb, pFg->fHead, 0);
            pFg->fHead = NULL;
            break;
          case TSDB_FTYPE_DATA:
            ASSERT(pFg->fData);
            tsdbUnrefFileObj(pTsdb, pFg->fData, 0);
            pFg->fData = NULL;
            break;
          case TSDB_FTYPE_SMA:
            ASSERT(pFg->fSma);
            tsdbUnrefFileObj(pTsdb, pFg->fSma, 0);
            pFg->fSma = NULL;
            break;
          case TSDB_FTYPE_STT: {
            int32_t       iStt = 0;
            STsdbFileObj *pSttFileObj = NULL;
            for (; iStt < taosArrayGetSize(pFg->aFStt); iStt++) {
              pSttFileObj = (STsdbFileObj *)taosArrayGetP(pFg->aFStt, iStt);
              if (tsdbIsSameFile(&pSttFileObj->file, &pOp->file)) {
                break;
              }
            }

            ASSERT(pSttFileObj != NULL);

            taosArrayRemove(pFg->aFStt, iStt);
            tsdbUnrefFileObj(pTsdb, pSttFileObj, 0);
          } break;
          default:
            ASSERT(0);
            break;
        }
      } else if (TSDB_FOP_MOD == pOp->op) {
        /* TODO */
        ASSERT(0);
      } else {
        ASSERT(0);
      }
    }
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbApplyNewFS(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFileSystem *pFS_Old = pTsdb->pFS;
  STsdbFileSystem *pFS_New = pTsdb->pFSN;

  // tombstone
  if (pFS_Old->fDel) {
    if (pFS_New->fDel) {
      // may change
    } else {
      ASSERT(0);
    }
  } else {
    if (pFS_New->fDel) {
    } else {
    }
  }

  // time-series
  int32_t iOld = 0;
  int32_t iNew = 0;

  while (true) {
    STsdbFileGroup *pFg_Old = NULL;
    STsdbFileGroup *pFg_New = NULL;

    if (iOld < taosArrayGetSize(pFS_Old->aFileGroup)) {
      pFg_Old = (STsdbFileGroup *)taosArrayGet(pFS_Old->aFileGroup, iOld);
    }
    if (iNew < taosArrayGetSize(pFS_New->aFileGroup)) {
      pFg_New = (STsdbFileGroup *)taosArrayGet(pFS_New->aFileGroup, iNew);
    }

    if (NULL == pFg_Old && NULL == pFg_New) break;

    if (pFg_Old && pFg_New) {
      if (pFg_Old->fid == pFg_New->fid) {
        // merge change
      } else if (pFg_Old->fid < pFg_New->fid) {
        // delete old
        iOld++;
      } else {
        // add new
        iOld++;
        iNew++;
      }
    } else if (pFg_Old) {
      // delete old
      iOld++;
    } else {
      // add new
      iOld++;
      iNew++;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbOpenFS(STsdb *pTsdb, int8_t rollback) {
  int32_t code = 0;
  int32_t lino = 0;

  // create in memory
  code = tsdbNewFileSystem(&pTsdb->pFS);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbNewFileSystem(&pTsdb->pFSN);
  TSDB_CHECK_CODE(code, lino, _exit);

  // recover from disk
  char current[TSDB_FILENAME_LEN] = {0};
  char current_t[TSDB_FILENAME_LEN] = {0};
  tsdbCurrentFileName(pTsdb, current, current_t);

  if (taosCheckExistFile(current)) {
    code = tsdbLoadFileSystemFromFile(pTsdb, current, pTsdb->pFS);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (taosCheckExistFile(current_t)) {
      if (rollback) {
        (void)taosRemoveFile(current_t);
      } else {
        code = tsdbLoadFileSystemFromFile(pTsdb, current_t, pTsdb->pFSN);
        TSDB_CHECK_CODE(code, lino, _exit);

        if (taosRenameFile(current_t, current) < 0) {
          code = TAOS_SYSTEM_ERROR(errno);
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        code = tsdbApplyNewFS(pTsdb);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    // copy the file system (todo)
  } else {
    ASSERT(!rollback);

    code = tsdbSaveFileSystemToFile(pTsdb, pTsdb->pFS, current);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // scan and fix file system
  code = tsdbScanAndTryFixFileSystem(pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbCloseFS(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;
#if 0

  if (NULL == pTsdb->pFS) return code;

  STsdbFileSystem *pFS = pTsdb->pFS;
  int32_t          nRef = 0;
  pTsdb->pFS = NULL;

  SRBTreeIter  fgIter = tRBTreeIterCreate(&pFS->fGroup, 1);
  SRBTreeNode *fgNode = tRBTreeIterNext(&fgIter);
  while (fgNode) {
    tRBTreeDrop(&pFS->fGroup, fgNode);

    STsdbFileGroup *pFg = RBTN_TO_FILE_GROUP(fgNode);

    // TSDB_FTYPE_HEAD
    nRef = tsdbUnrefFileObj(pTsdb, pFg->fHead, 0);
    ASSERT(0 == nRef);

    // TSDB_FTYPE_DATA
    nRef = tsdbUnrefFileObj(pTsdb, pFg->fData, 0);
    ASSERT(0 == nRef);

    // TSDB_FTYPE_SMA
    nRef = tsdbUnrefFileObj(pTsdb, pFg->fSma, 0);
    ASSERT(0 == nRef);

    // TSDB_FTYPE_STT
    SRBTreeIter sttIter = tRBTreeIterCreate(&pFg->fStt, 1);
    while (true) {
      SRBTreeNode *sttNode = tRBTreeIterNext(&sttIter);
      if (NULL == sttNode) break;

      tRBTreeDrop(&pFg->fStt, sttNode);
      STsdbFileObj *fStt = RBTN_TO_FILE_OBJ(sttNode);

      nRef = tsdbUnrefFileObj(pTsdb, fStt, 0);
      ASSERT(0 == nRef);
    }

    taosMemoryFree(pFg);

    fgNode = tRBTreeIterNext(&fgIter);
  }

  // TSDB_FTYPE_DEL
  if (pFS->fDel) {
    nRef = tsdbUnrefFileObj(pTsdb, pFS->fDel, 0);
    ASSERT(0 == nRef);
  }

  tsdbFreeFileSystem(pFS);
#endif

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbPrepareFS(STsdb *pTsdb, SArray *aFileOpP) {
  int32_t code = 0;
  int32_t lino = 0;

  // apply change to pTsdb->pFSN
  for (int32_t iFileOp = 0; iFileOp < taosArrayGetSize(aFileOpP); iFileOp++) {
    STsdbFileOp *pOp = (STsdbFileOp *)taosArrayGetP(aFileOpP, iFileOp);

    code = tsdbFileSystemApplyOp(pTsdb, pTsdb->pFSN, pOp);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // save to current_t
  char current_t[TSDB_FILENAME_LEN] = {0};
  tsdbCurrentFileName(pTsdb, NULL, current_t);

  code = tsdbSaveFileSystemToFile(pTsdb, pTsdb->pFSN, current_t);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbCommitFS(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  char current[TSDB_FILENAME_LEN] = {0};
  char current_t[TSDB_FILENAME_LEN] = {0};
  tsdbCurrentFileName(pTsdb, current, current_t);

  if (taosRenameFile(current_t, current) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbApplyNewFS(pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbRollbackFS(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  // remove current_t
  char current_t[TSDB_FILENAME_LEN] = {0};
  tsdbCurrentFileName(pTsdb, NULL, current_t);
  (void)taosRemoveFile(current_t);

  // rollback the change (todo)

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

int64_t tsdbNextFileID(STsdb *pTsdb) { return atomic_add_fetch_64(&pTsdb->id, 1); }

bool tsdbShouldMerge(STsdb *pTsdb) {
  // todo: make it thread safe
  int32_t          sttTrigger = pTsdb->pVnode->config.sttTrigger;
  STsdbFileSystem *pFS = pTsdb->pFSN;  // (todo)

  for (int32_t iFg = 0; iFg < taosArrayGetSize(pFS->aFileGroup); iFg++) {
    STsdbFileGroup *pFg = (STsdbFileGroup *)taosArrayGet(pFS->aFileGroup, iFg);

    if (taosArrayGetSize(pFg->aFStt) >= sttTrigger) {
      return true;
    }
  }

  return false;
}