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

typedef struct STsdbFlusher STsdbFlusher;

static int32_t tsdbWriteBlockDataImpl(STsdbFileWriter *pWriter, SBlockData *pBlockData, SBlockInfo *pBlockInfo,
                                      int8_t cmprAlg) {
  int32_t code = 0;
  int32_t lino = 0;

  uint8_t *aBuf[4] = {NULL};
  int32_t  aNBuf[4] = {0};
  code = tCmprBlockData(pBlockData, cmprAlg, NULL, NULL, aBuf, aNBuf);
  TSDB_CHECK_CODE(code, lino, _exit);

  pBlockInfo->offset = pWriter->pf->size;
  pBlockInfo->szKey = aNBuf[2] + aNBuf[3];
  pBlockInfo->szBlock = aNBuf[0] + aNBuf[1] + aNBuf[2] + aNBuf[3];

  for (int32_t iBuf = 3; iBuf >= 0; iBuf--) {
    if (aNBuf[iBuf]) {
      code = tsdbFileAppend(pWriter, aBuf[iBuf], aNBuf[iBuf]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
  }
  for (int32_t iBuf = 0; iBuf < sizeof(aBuf) / sizeof(aBuf[0]); iBuf++) {
    tFree(aBuf[iBuf]);
  }
  return code;
}

static int32_t tsdbWriteBlockDataEx(STsdbFileWriter *pWriter, SBlockData *pBData, SArray *aSttBlk, int8_t cmprAlg) {
  int32_t code = 0;
  int32_t lino = 0;

  if (0 == pBData->nRow) return code;

  SSttBlk *pSttBlk = taosArrayPush(aSttBlk, &(SSttBlk){0});
  if (NULL == pSttBlk) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pSttBlk->suid = pBData->suid;
  pSttBlk->nRow = pBData->nRow;
  pSttBlk->minKey = TSKEY_MAX;
  pSttBlk->maxKey = TSKEY_MIN;
  pSttBlk->minVer = VERSION_MAX;
  pSttBlk->maxVer = VERSION_MIN;
  for (int32_t iRow = 0; iRow < pBData->nRow; iRow++) {
    pSttBlk->minKey = TMIN(pSttBlk->minKey, pBData->aTSKEY[iRow]);
    pSttBlk->maxKey = TMAX(pSttBlk->maxKey, pBData->aTSKEY[iRow]);
    pSttBlk->minVer = TMIN(pSttBlk->minVer, pBData->aVersion[iRow]);
    pSttBlk->maxVer = TMAX(pSttBlk->maxVer, pBData->aVersion[iRow]);
  }
  pSttBlk->minUid = pBData->uid ? pBData->uid : pBData->aUid[0];
  pSttBlk->maxUid = pBData->uid ? pBData->uid : pBData->aUid[pBData->nRow - 1];

  // write
  code = tsdbWriteBlockDataImpl(pWriter, pBData, &pSttBlk->bInfo, cmprAlg);
  TSDB_CHECK_CODE(code, lino, _exit);

  tBlockDataClear(pBData);

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  } else {
    tsdbTrace("%s done", __func__);
  }
  return code;
}

static int32_t tsdbReadBlockDataEx(STsdbFileReader *pReader, SBlockInfo *pBlockInfo, SBlockData *pBlockData) {
  int32_t code = 0;
  int32_t lino = 0;

  uint8_t *aBuf[2] = {NULL};
  code = tRealloc(&aBuf[0], pBlockInfo->szBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFileRead(pReader, pBlockInfo->offset, aBuf[0], pBlockInfo->szBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tDecmprBlockData(aBuf[0], pBlockInfo->szBlock, pBlockData, &aBuf[1]);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pReader->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  for (int32_t iBuf = 0; iBuf < sizeof(aBuf) / sizeof(aBuf[0]); iBuf++) {
    tFree(aBuf[iBuf]);
  }
  return code;
}

static int32_t tsdbWriteSttBlkEx(STsdbFileWriter *pWriter, SArray *aSttBlk) {
  int32_t  code = 0;
  int32_t  lino = 0;
  uint8_t *pBuf = NULL;

  pWriter->pf->offset = pWriter->pf->size;

  if (0 == taosArrayGetSize(aSttBlk)) {
    goto _exit;
  }

  int32_t size = 0;
  for (int32_t iSttBlk = 0; iSttBlk < taosArrayGetSize(aSttBlk); iSttBlk++) {
    size += tPutSttBlk(NULL, taosArrayGet(aSttBlk, iSttBlk));
  }

  code = tRealloc(&pBuf, size);
  TSDB_CHECK_CODE(code, lino, _exit);

  int32_t n = 0;
  for (int32_t iSttBlk = 0; iSttBlk < taosArrayGetSize(aSttBlk); iSttBlk++) {
    n += tPutSttBlk(pBuf + n, taosArrayGet(aSttBlk, iSttBlk));
  }

  code = tsdbFileAppend(pWriter, pBuf, size);
  TSDB_CHECK_CODE(code, lino, _exit);

// TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, number of stt blk:%" PRId64, TD_VID(pWriter->pTsdb->pVnode),
              __func__, lino, tstrerror(code), taosArrayGetSize(aSttBlk));
  } else {
    tsdbTrace("vgId:%d %s done, number of stt blk:%" PRId64, TD_VID(pWriter->pTsdb->pVnode), __func__,
              taosArrayGetSize(aSttBlk));
  }
  tFree(pBuf);
  return code;
}

static int32_t tsdbReadSttBlkEx(STsdbFileReader *pReader, SArray *aSttBlk) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(TSDB_FTYPE_STT == pReader->file.ftype);
  taosArrayClear(aSttBlk);

  int64_t size = pReader->file.size - pReader->file.offset;

  if (0 == size) return code;

  uint8_t *pBuf = NULL;

  code = tRealloc(&pBuf, size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFileRead(pReader, pReader->file.offset, pBuf, size);
  TSDB_CHECK_CODE(code, lino, _exit);

  int64_t n = 0;
  while (n < size) {
    SSttBlk *pSttBlk = taosArrayPush(aSttBlk, &(SSttBlk){0});
    if (NULL == pSttBlk) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    n += tGetSttBlk(pBuf + n, pSttBlk);
  }

  ASSERT(n == size);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pReader->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  tFree(pBuf);
  return code;
}
// FLUSH MEMTABLE TO FILE SYSTEM ===================================
struct STsdbFlusher {
  // configs
  STsdb  *pTsdb;
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int8_t  sttTrigger;

  SArray *aTbDataP;
  SArray *aFileOpP;  // SArray<STsdbFileOp *>

  // time-series data
  int32_t          fid;
  TSKEY            minKey;
  TSKEY            maxKey;
  STsdbFileWriter *pWriter;
  SArray          *aSttBlk;
  SBlockData       bData;
  int32_t          iTbData;
  SSkmInfo         skmTable;
  SSkmInfo         skmRow;
  // tomestone data
};

static int32_t tsdbFlusherInit(STsdb *pTsdb, STsdbFlusher *pFlusher) {
  int32_t code = 0;
  int32_t lino = 0;
  SVnode *pVnode = pTsdb->pVnode;

  pFlusher->pTsdb = pTsdb;
  pFlusher->minutes = pTsdb->keepCfg.days;
  pFlusher->precision = pTsdb->keepCfg.precision;
  pFlusher->minRow = pVnode->config.tsdbCfg.minRows;
  pFlusher->maxRow = pVnode->config.tsdbCfg.maxRows;
  pFlusher->cmprAlg = pVnode->config.tsdbCfg.compression;
  pFlusher->sttTrigger = pVnode->config.sttTrigger;

  pFlusher->aTbDataP = tsdbMemTableGetTbDataArray(pTsdb->imem);
  if (NULL == pFlusher->aTbDataP) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pFlusher->aFileOpP = taosArrayInit(0, sizeof(STsdbFileOp *));
  if (NULL == pFlusher->aFileOpP) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pFlusher->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static void tsdbFlusherClear(STsdbFlusher *pFlusher) {
  tBlockDataDestroy(&pFlusher->bData, 1);

  if (pFlusher->aSttBlk) {
    taosArrayDestroy(pFlusher->aSttBlk);
    pFlusher->aSttBlk = NULL;
  }
  if (pFlusher->aFileOpP) {
    taosArrayDestroyEx(pFlusher->aFileOpP, (FDelete)tsdbFileOpDestroy);
    pFlusher->aFileOpP = NULL;
  }

  if (pFlusher->aTbDataP) {
    taosArrayDestroy(pFlusher->aTbDataP);
    pFlusher->aTbDataP = NULL;
  }
}

static int32_t tsdbUpdateSkmInfo(SMeta *pMeta, int64_t suid, int64_t uid, int32_t sver, SSkmInfo *pSkmInfo) {
  int32_t code = 0;

  if (!TABLE_SAME_SCHEMA(suid, uid, pSkmInfo->suid, pSkmInfo->uid)   // not same schema
      || (pSkmInfo->pTSchema == NULL)                                // schema not created
      || (sver > 0 && pSkmInfo->pTSchema->version != sver /*todo*/)  // not same version
  ) {
    pSkmInfo->suid = suid;
    pSkmInfo->uid = uid;
    if (pSkmInfo->pTSchema) {
      tTSchemaDestroy(pSkmInfo->pTSchema);
    }
    code = metaGetTbTSchemaEx(pMeta, suid, uid, sver, &pSkmInfo->pTSchema);
  }

_exit:
  return code;
}

static int32_t tsdbFlushTableTimeSeriesData(STsdbFlusher *pFlusher, TSKEY *nextKey) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pFlusher->pTsdb;

  STbData    *pTbData = (STbData *)taosArrayGetP(pFlusher->aTbDataP, pFlusher->iTbData);
  STbDataIter iter = {0};
  int64_t     nRows = 0;
  TABLEID     id = {.suid = pTbData->suid, .uid = pTbData->uid};
  TSDBKEY     fromKey = {.version = VERSION_MIN, .ts = pFlusher->minKey};
  SMeta      *pMeta = pTsdb->pVnode->pMeta;

  code = tsdbUpdateSkmInfo(pMeta, pTbData->suid, pTbData->uid, -1, &pFlusher->skmTable);
  TSDB_CHECK_CODE(code, lino, _exit);

  // check if need to flush the data
  if (!TABLE_SAME_SCHEMA(pFlusher->bData.suid, pFlusher->bData.uid, pTbData->suid, pTbData->uid)) {
    code = tsdbWriteBlockDataEx(pFlusher->pWriter, &pFlusher->bData, pFlusher->aSttBlk, pFlusher->cmprAlg);
    TSDB_CHECK_CODE(code, lino, _exit);

    tBlockDataReset(&pFlusher->bData);
  }

  // init the block data
  if (!TABLE_SAME_SCHEMA(pTbData->suid, pTbData->uid, pFlusher->bData.suid, pFlusher->bData.uid)) {
    code = tBlockDataInit(&pFlusher->bData, &id, pFlusher->skmTable.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tsdbTbDataIterOpen(pTbData, &fromKey, 0, &iter);
  for (;;) {
    TSDBROW *pRow = tsdbTbDataIterGet(&iter);
    if (NULL == pRow) {
      *nextKey = TSKEY_MAX;
      break;
    } else if (TSDBROW_TS(pRow) > pFlusher->maxKey) {
      *nextKey = TSDBROW_TS(pRow);
      break;
    }

    nRows++;

    code = tsdbUpdateSkmInfo(pMeta, pTbData->suid, pTbData->uid, TSDBROW_SVERSION(pRow), &pFlusher->skmRow);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tBlockDataAppendRow(&pFlusher->bData, pRow, pFlusher->skmRow.pTSchema, pTbData->uid);
    TSDB_CHECK_CODE(code, lino, _exit);

    tsdbTbDataIterNext(&iter);

    if (pFlusher->bData.nRow >= pFlusher->maxRow) {
      code = tsdbWriteBlockDataEx(pFlusher->pWriter, &pFlusher->bData, pFlusher->aSttBlk, pFlusher->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d suid:%" PRId64 " uid:%" PRId64, TD_VID(pTsdb->pVnode),
              __func__, lino, tstrerror(code), pFlusher->fid, pTbData->suid, pTbData->uid);
  } else {
    tsdbTrace("vgId:%d %s done, fid:%d suid:%" PRId64 " uid:%" PRId64 " nRows:%" PRId64, TD_VID(pTsdb->pVnode),
              __func__, pFlusher->fid, pTbData->suid, pTbData->uid, nRows);
  }
  return code;
}

static int32_t tsdbFlushFileTimeSeriesData(STsdbFlusher *pFlusher, TSKEY *nextKey) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pFlusher->pTsdb;

  // prepare and set state
  pFlusher->fid = tsdbKeyFid(*nextKey, pFlusher->minutes, pFlusher->precision);
  tsdbFidKeyRange(pFlusher->fid, pFlusher->minutes, pFlusher->precision, &pFlusher->minKey, &pFlusher->maxKey);
  if ((NULL == pFlusher->aSttBlk) && ((pFlusher->aSttBlk = taosArrayInit(0, sizeof(SSttBlk))) == NULL)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    taosArrayClear(pFlusher->aSttBlk);
  }
  tBlockDataReset(&pFlusher->bData);

  // create/open file to write
  STsdbFileOp *pFileOp = NULL;
  STsdbFile    file = {
         .ftype = TSDB_FTYPE_STT,
         .did = {0},  // todo
         .fid = pFlusher->fid,
         .id = tsdbNextFileID(pTsdb),
  };
  code = tsdbFileOpCreate(TSDB_FOP_ADD, &file, &pFileOp);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (NULL == taosArrayPush(pFlusher->aFileOpP, &pFileOp)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFileWriterOpen(pTsdb, &pFileOp->file, &pFlusher->pWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // loop to flush table time-series data
  *nextKey = TSKEY_MAX;
  for (pFlusher->iTbData = 0; pFlusher->iTbData < taosArrayGetSize(pFlusher->aTbDataP); pFlusher->iTbData++) {
    TSKEY nextTbKey;

    code = tsdbFlushTableTimeSeriesData(pFlusher, &nextTbKey);
    TSDB_CHECK_CODE(code, lino, _exit);

    *nextKey = TMIN(*nextKey, nextTbKey);
  }

  // flush remain data
  if (pFlusher->bData.nRow > 0) {
    code = tsdbWriteBlockDataEx(pFlusher->pWriter, &pFlusher->bData, pFlusher->aSttBlk, pFlusher->cmprAlg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbWriteSttBlkEx(pFlusher->pWriter, pFlusher->aSttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  // close filas
  code = tsdbFileWriterClose(&pFlusher->pWriter, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code),
              pFlusher->fid);
    {
      // TODO: clear/rollback
    }
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(pTsdb->pVnode), __func__, pFlusher->fid);
  }
  return code;
}

static int32_t tsdbFlushTimeSeriesData(STsdbFlusher *pFlusher) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STsdb     *pTsdb = pFlusher->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  TSKEY nextKey = pMemTable->minKey;
  while (nextKey < TSKEY_MAX) {
    code = tsdbFlushFileTimeSeriesData(pFlusher, &nextKey);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, nRows:%" PRId64, TD_VID(pTsdb->pVnode), __func__, pMemTable->nRow);
  }
  return code;
}

static int32_t tsdbFlushDelData(STsdbFlusher *pFlusher) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pFlusher->pTsdb;

  // TODO
  ASSERT(0);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbMerge(STsdb *pTsdb);

int32_t tsdbFlush(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  // check ----
  SMemTable *pMemTable = pTsdb->mem;
  if (0 == pMemTable->nRow && 0 == pMemTable->nDel) {
    taosThreadRwlockWrlock(&pTsdb->rwLock);
    pTsdb->mem = NULL;
    taosThreadRwlockUnlock(&pTsdb->rwLock);

    tsdbUnrefMemTable(pMemTable);
    return code;
  } else {
    taosThreadRwlockWrlock(&pTsdb->rwLock);
    pTsdb->mem = NULL;
    pTsdb->imem = pMemTable;
    taosThreadRwlockUnlock(&pTsdb->rwLock);
  }

  // flush ----
  STsdbFlusher flusher = {0};

  code = tsdbFlusherInit(pTsdb, &flusher);
  TSDB_CHECK_CODE(code, lino, _exit);

  ASSERT(taosArrayGetSize(flusher.aTbDataP) > 0);

  if (pMemTable->nRow > 0) {
    code = tsdbFlushTimeSeriesData(&flusher);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pMemTable->nDel > 0) {
    code = tsdbFlushDelData(&flusher);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // apply change
  code = tsdbPrepareFS(pTsdb, flusher.aFileOpP);
  TSDB_CHECK_CODE(code, lino, _exit);

  // schedule compact if need (todo)

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    // todo (do some rollback)
  }
  tsdbFlusherClear(&flusher);
  return code;
}

int32_t tsdbFlushCommit(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  SMemTable *pMemTable = pTsdb->imem;
  if (NULL == pMemTable) {
    return code;
  }

  // commit
  taosThreadRwlockWrlock(&pTsdb->rwLock);

  code = tsdbCommitFS(pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

  pTsdb->imem = NULL;

  taosThreadRwlockUnlock(&pTsdb->rwLock);

  tsdbUnrefMemTable(pMemTable);

  // schedule merge (todo)
  if (tsdbShouldMerge(pTsdb)) {
    code = tsdbMerge(pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbFlushRollback(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(0);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

// MERGE MULTIPLE STT ===================================
typedef struct {
  SRowInfo        *pRInfo;
  SRowInfo         rInfo;
  SArray          *aSttBlk;
  int32_t          iSttBlk;
  int32_t          iRow;
  SBlockData       bData;
  STsdbFileReader *pReader;
  SRBTreeNode      rbtn;
} SSttDataIter;

#define RBTN_TO_STT_DATA_ITER(PNODE) ((SSttDataIter *)(((uint8_t *)(PNODE)) - offsetof(SSttDataIter, rbtn)))

typedef struct {
  STsdb        *pTsdb;
  int8_t        cmprAlg;
  SArray       *aFileOpP;
  SArray       *aSttIterP;  // SArray<SSttDataIter *>
  SRBTree       mTree;
  SSttDataIter *pIter;

  SSkmInfo         skmTable;
  STsdbFileWriter *pWriter;
  SBlockData       bData;
  SArray          *aSttBlk;
} STsdbMerger;

// SSttDataIter
static int32_t tsdbSttDataIterNext(SSttDataIter *pIter) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pIter->iRow >= pIter->bData.nRow) {
    pIter->iSttBlk++;

    if (pIter->iSttBlk < taosArrayGetSize(pIter->aSttBlk)) {
      SSttBlk *pSttBlk = (SSttBlk *)taosArrayGet(pIter->aSttBlk, pIter->iSttBlk);

      code = tsdbReadBlockDataEx(pIter->pReader, &pSttBlk->bInfo, &pIter->bData);
      TSDB_CHECK_CODE(code, lino, _exit);

      pIter->iRow = 0;
    } else {
      pIter->pRInfo = NULL;
      goto _exit;
    }
  }

  ASSERT(pIter->iRow < pIter->bData.nRow);

  pIter->rInfo.suid = pIter->bData.suid;
  pIter->rInfo.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[pIter->iRow];
  pIter->rInfo.row = tsdbRowFromBlockData(&pIter->bData, pIter->iRow);
  pIter->iRow++;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pIter->pReader->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttDataIterCreate(STsdb *pTsdb, const STsdbFile *pFile, SSttDataIter **ppIter) {
  int32_t code = 0;
  int32_t lino = 0;

  SSttDataIter *pIter = (SSttDataIter *)taosMemoryCalloc(1, sizeof(*pIter));
  if (NULL == pIter) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pIter->aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
  if (NULL == pIter->aSttBlk) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pIter->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFileReaderOpen(pTsdb, pFile, &pIter->pReader);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadSttBlkEx(pIter->pReader, pIter->aSttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  pIter->pRInfo = &pIter->rInfo;
  pIter->iSttBlk = -1;
  pIter->iRow = 0;
  code = tsdbSttDataIterNext(pIter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    *ppIter = NULL;

    if (pIter) {
      if (pIter->aSttBlk) {
        taosArrayDestroy(pIter->aSttBlk);
      }
      tBlockDataDestroy(&pIter->bData, 1);
      if (pIter->pReader) {
        tsdbFileReaderClose(pIter->pReader);
      }
      taosMemoryFree(pIter);
    }
  } else {
    *ppIter = pIter;
  }
  return code;
}

static void tsdbSttDataIterDestroy(SSttDataIter **ppIter) {
  SSttDataIter *pIter = *ppIter;
  if (pIter) {
    tBlockDataDestroy(&pIter->bData, 1);
    if (pIter->aSttBlk) {
      taosArrayDestroy(pIter->aSttBlk);
    }
    taosMemoryFree(pIter);
  }
}

static SRowInfo *tsdbSttDataIterGet(SSttDataIter *pIter) { return pIter->pRInfo; }

static int32_t tsdbSttDataIterCmprFn(const SRBTreeNode *p1, const SRBTreeNode *p2) {
  SSttDataIter *pIter1 = RBTN_TO_STT_DATA_ITER(p1);
  SSttDataIter *pIter2 = RBTN_TO_STT_DATA_ITER(p2);

  ASSERT(pIter1->pRInfo && pIter2->pRInfo);

  if (pIter1->pRInfo->suid < pIter2->pRInfo->suid) {
    return -1;
  } else if (pIter1->pRInfo->suid > pIter2->pRInfo->suid) {
    return 1;
  }

  if (pIter1->pRInfo->uid < pIter2->pRInfo->uid) {
    return -1;
  } else if (pIter1->pRInfo->uid > pIter2->pRInfo->uid) {
    return 1;
  }

  TSDBKEY key1 = TSDBROW_KEY(&pIter1->pRInfo->row);
  TSDBKEY key2 = TSDBROW_KEY(&pIter2->pRInfo->row);

  return tsdbKeyCmprFn(&key1, &key2);
}

// STsdbMerger
static int32_t tsdbMergerOpen(STsdb *pTsdb, STsdbMerger **ppMerger) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbMerger *pMerger = (STsdbMerger *)taosMemoryCalloc(1, sizeof(*pMerger));
  if (NULL == pMerger) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pMerger->pTsdb = pTsdb;
  pMerger->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  pMerger->aFileOpP = taosArrayInit(0, sizeof(STsdbFileOp *));
  if (NULL == pMerger->aFileOpP) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pMerger->aSttIterP = taosArrayInit(0, sizeof(SSttDataIter *));
  if (NULL == pMerger->aSttIterP) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pMerger->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  pMerger->aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
  if (NULL == pMerger->aSttBlk) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    *ppMerger = NULL;

    if (pMerger) {
      if (pMerger->aSttBlk) {
        pMerger->aSttBlk = taosArrayDestroy(pMerger->aSttBlk);
      }
      tBlockDataDestroy(&pMerger->bData, 1);
      if (pMerger->aSttIterP) {
        pMerger->aSttIterP = taosArrayDestroy(pMerger->aSttIterP);
      }
      if (pMerger->aFileOpP) {
        pMerger->aFileOpP = taosArrayDestroy(pMerger->aFileOpP);
      }
      taosMemoryFree(pMerger);
    }
  } else {
    *ppMerger = pMerger;
  }
  return code;
}

static void tsdbMergerClose(STsdbMerger *pMerger) {
  if (pMerger) {
    if (pMerger->aSttBlk) {
      pMerger->aSttBlk = taosArrayDestroy(pMerger->aSttBlk);
    }
    tBlockDataDestroy(&pMerger->bData, 1);
    if (pMerger->aSttIterP) {
      taosArrayDestroy(pMerger->aSttIterP);
    }
    if (pMerger->aFileOpP) {
      taosArrayDestroyEx(pMerger->aFileOpP, (FDelete)tsdbFileOpDestroy);
    }
    taosMemoryFree(pMerger);
  }
}

static SRowInfo *tsdbGetMergeRow(STsdbMerger *pMerger) {
  if (NULL == pMerger->pIter) {
    SRBTreeNode *pNode = tRBTreeMin(&pMerger->mTree);

    if (pNode) {
      tRBTreeDrop(&pMerger->mTree, pNode);
      pMerger->pIter = RBTN_TO_STT_DATA_ITER(pNode);
    }
  }

  return pMerger->pIter ? tsdbSttDataIterGet(pMerger->pIter) : NULL;
}

static int32_t tsdbNextMergeRow(STsdbMerger *pMerger) {
  int32_t code = 0;
  int32_t lino = 0;

  if (NULL == pMerger->pIter) goto _exit;

  code = tsdbSttDataIterNext(pMerger->pIter);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (NULL == pMerger->pIter->pRInfo) {
    pMerger->pIter = NULL;
    goto _exit;
  }

  SRBTreeNode *pNode = tRBTreeMin(&pMerger->mTree);
  if (NULL == pNode) {
    goto _exit;
  }

  SSttDataIter *pIter = RBTN_TO_STT_DATA_ITER(pNode);
  int32_t       c = tsdbSttDataIterCmprFn(&pMerger->pIter->rbtn, &pIter->rbtn);
  ASSERT(c);
  if (c > 0) {
    tRBTreePut(&pMerger->mTree, &pMerger->pIter->rbtn);
    pMerger->pIter = NULL;
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbMergeFileGroup(STsdbMerger *pMerger, STsdbFileGroup *pFg) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pMerger->pTsdb;

  // prepare
  ASSERT(taosArrayGetSize(pMerger->aSttIterP) == 0);
  tRBTreeInit(&pMerger->mTree, tsdbSttDataIterCmprFn);
  for (int32_t iStt = 0; iStt < taosArrayGetSize(pFg->aFStt); iStt++) {
    STsdbFileObj *pSttFileObj = (STsdbFileObj *)taosArrayGetP(pFg->aFStt, iStt);

    // op
    STsdbFileOp  *pOp = NULL;
    STsdbFileOp **ppOp = (STsdbFileOp **)taosArrayPush(pMerger->aFileOpP, &pOp);
    if (NULL == ppOp) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbFileOpCreate(TSDB_FOP_REMOVE, &pSttFileObj->file, ppOp);
    TSDB_CHECK_CODE(code, lino, _exit);

    // iter
    SSttDataIter  *pIter = NULL;
    SSttDataIter **ppIter = (SSttDataIter **)taosArrayPush(pMerger->aSttIterP, &pIter);
    if (NULL == ppIter) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbSttDataIterCreate(pTsdb, &pSttFileObj->file, ppIter);
    TSDB_CHECK_CODE(code, lino, _exit);

    SRBTreeNode *pTNode = tRBTreePut(&pMerger->mTree, &(*ppIter)->rbtn);
    ASSERT(pTNode);
  }

  STsdbFileOp  *pOp = NULL;
  STsdbFileOp **ppOp = (STsdbFileOp **)taosArrayPush(pMerger->aFileOpP, &pOp);
  if (NULL == ppOp) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFileOpCreate(TSDB_FOP_ADD,
                          &(STsdbFile){
                              .ftype = TSDB_FTYPE_STT,
                              .did = {0},  // todo
                              .fid = pFg->fid,
                              .id = tsdbNextFileID(pTsdb),
                          },
                          ppOp);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFileWriterOpen(pTsdb, &(*ppOp)->file, &pMerger->pWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  tBlockDataReset(&pMerger->bData);
  taosArrayClear(pMerger->aSttBlk);
  while (true) {
    SRowInfo *pRowInfo = tsdbGetMergeRow(pMerger);
    if (NULL == (pRowInfo)) {
      break;
    }

    if (!TABLE_SAME_SCHEMA(pMerger->bData.suid, pMerger->bData.uid, pRowInfo->suid, pRowInfo->uid)) {
      code = tsdbWriteBlockDataEx(pMerger->pWriter, &pMerger->bData, pMerger->aSttBlk, pMerger->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);

      tBlockDataReset(&pMerger->bData);
    }

    if (!TABLE_SAME_SCHEMA(pMerger->bData.suid, pMerger->bData.uid, pRowInfo->suid, pRowInfo->uid)) {
      code = tsdbUpdateSkmInfo(pTsdb->pVnode->pMeta, pRowInfo->suid, pRowInfo->uid, -1, &pMerger->skmTable);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tBlockDataInit(&pMerger->bData, &(TABLEID){.suid = pRowInfo->suid, .uid = 0}, pMerger->skmTable.pTSchema,
                            NULL, 0);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataAppendRow(&pMerger->bData, &pRowInfo->row, NULL, pRowInfo->uid);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbNextMergeRow(pMerger);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pMerger->bData.nRow >= pTsdb->pVnode->config.tsdbCfg.maxRows) {
      code = tsdbWriteBlockDataEx(pMerger->pWriter, &pMerger->bData, pMerger->aSttBlk, pMerger->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (pMerger->bData.nRow) {
    code = tsdbWriteBlockDataEx(pMerger->pWriter, &pMerger->bData, pMerger->aSttBlk, pMerger->cmprAlg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbWriteSttBlkEx(pMerger->pWriter, pMerger->aSttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  // clear
  code = tsdbFileWriterClose(&pMerger->pWriter, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

  // taosArrayClearEx(pMerger->aSttIterP, tsdbSttDataIterDestroy); (todo)

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code),
              pFg->fid);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(pTsdb->pVnode), __func__, pFg->fid);
  }
  return code;
}

static int32_t tsdbMerge(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbFileSystem *pFS = pTsdb->pFSN;  // todo
  int32_t          sttTrigger = pTsdb->pVnode->config.sttTrigger;
  STsdbMerger     *pMerger = NULL;

  for (int32_t iFg = 0; iFg < taosArrayGetSize(pFS->aFileGroup); iFg++) {
    STsdbFileGroup *pFg = (STsdbFileGroup *)taosArrayGet(pFS->aFileGroup, iFg);

    if (taosArrayGetSize(pFg->aFStt) >= sttTrigger) {
      if (NULL == pMerger) {
        code = tsdbMergerOpen(pTsdb, &pMerger);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = tsdbMergeFileGroup(pMerger, pFg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (pMerger) {
    code = tsdbPrepareFS(pTsdb, pMerger->aFileOpP);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbCommitFS(pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }

  if (pMerger) {
    tsdbMergerClose(pMerger);
  }
  return code;
}

// RETENTION ===================================

// TRANSACTION CONTROL ===================================