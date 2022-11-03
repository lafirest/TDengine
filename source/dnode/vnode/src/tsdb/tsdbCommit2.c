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

static int32_t tsdbWriteBlockDataEx(STsdbFileWriter *pWriter, SBlockData *pBlockData, SBlockInfo *pBlockInfo,
                                    int8_t cmprAlg) {
  int32_t code = 0;
  int32_t lino = 0;

  uint8_t *aBuf[4] = {NULL};
  int32_t  aNBuf[4] = {0};
  code = tCmprBlockData(pBlockData, cmprAlg, NULL, NULL, aBuf, aNBuf);
  TSDB_CHECK_CODE(code, lino, _exit);

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

// FLUSH MEMTABLE TO FILE SYSTEM ===================================
typedef struct {
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
} STsdbFlusher;

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

static int32_t tsdbFlushBlockData(STsdbFlusher *pFlusher) {
  int32_t code = 0;
  int32_t lino = 0;

  if (0 == pFlusher->bData.nRow) return code;

  SBlockData *pBlockData = &pFlusher->bData;
  SSttBlk     sttBlk = {0};

  sttBlk.suid = pBlockData->suid;
  sttBlk.nRow = pBlockData->nRow;
  sttBlk.minKey = TSKEY_MAX;
  sttBlk.maxKey = TSKEY_MIN;
  sttBlk.minVer = VERSION_MAX;
  sttBlk.maxVer = VERSION_MIN;
  for (int32_t iRow = 0; iRow < pBlockData->nRow; iRow++) {
    sttBlk.minKey = TMIN(sttBlk.minKey, pBlockData->aTSKEY[iRow]);
    sttBlk.maxKey = TMAX(sttBlk.maxKey, pBlockData->aTSKEY[iRow]);
    sttBlk.minVer = TMIN(sttBlk.minVer, pBlockData->aVersion[iRow]);
    sttBlk.maxVer = TMAX(sttBlk.maxVer, pBlockData->aVersion[iRow]);
  }
  sttBlk.minUid = pBlockData->uid ? pBlockData->uid : pBlockData->aUid[0];
  sttBlk.maxUid = pBlockData->uid ? pBlockData->uid : pBlockData->aUid[pBlockData->nRow - 1];

  // write
  code = tsdbWriteBlockDataEx(pFlusher->pWriter, pBlockData, &sttBlk.bInfo, pFlusher->cmprAlg);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (NULL == taosArrayPush(pFlusher->aSttBlk, &sttBlk)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tBlockDataClear(&pFlusher->bData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pFlusher->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done", TD_VID(pFlusher->pTsdb->pVnode), __func__);
  }
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
    code = tsdbFlushBlockData(pFlusher);
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
      code = tsdbFlushBlockData(pFlusher);
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
    code = tsdbFlushBlockData(pFlusher);
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
  SRowInfo         rInfo;
  SArray          *aSttBlk;
  int32_t          iSttBlk;
  SBlockData       bData;
  SRBTreeNode      rbtn;
  STsdbFileReader *pReader;
} SSttDataIter;

typedef struct {
  STsdb     *pTsdb;
  SRBTree    mergeTree;
  SBlockData bData;
  SArray    *aFileOpP;
} STsdbMerger;

// SSttDataIter
static int32_t tsdbSttDataIterNext(SSttDataIter *pIter) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  return code;
}

static int32_t tsdbSttDataIterCreate(STsdbFile *pFile, SSttDataIter **ppIter) {
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
      taosMemoryFree(pIter);
    }
  } else {
    *ppIter = pIter;
  }
  return code;
}

static void tsdbSttDataIterDestroy(SSttDataIter *pIter) {
  if (pIter) {
    tBlockDataDestroy(&pIter->bData, 1);
    if (pIter->aSttBlk) {
      taosArrayDestroy(pIter->aSttBlk);
    }
    taosMemoryFree(pIter);
  }
}

static SRowInfo *tsdbSttDataIterGet(SSttDataIter *pIter) {
  // todo
  return NULL;
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
  // todo
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    *ppMerger = NULL;
  } else {
  }
  return code;
}

static void tsdbMergerClose(STsdbMerger *pMerger) {
  if (pMerger) {
    taosMemoryFree(pMerger);
  }
}

static int32_t tsdbMergeFileGroup(STsdbMerger *pMerger, STsdbFileGroup *pFg) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pMerger->pTsdb;
  // TODO
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
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