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

#ifndef _TD_COMMON_DEF_H_
#define _TD_COMMON_DEF_H_

#include "taosdef.h"
#include "tarray.h"
#include "tmsg.h"
#include "tvariant.h"

#ifdef __cplusplus
extern "C" {
#endif

// TODO remove it
enum {
  TMQ_CONF__RESET_OFFSET__NONE = -3,
  TMQ_CONF__RESET_OFFSET__EARLIEAST = -2,
  TMQ_CONF__RESET_OFFSET__LATEST = -1,
};

// clang-format off
#define IS_META_MSG(x) ( \
     x == TDMT_VND_CREATE_STB     \
  || x == TDMT_VND_ALTER_STB      \
  || x == TDMT_VND_DROP_STB       \
  || x == TDMT_VND_CREATE_TABLE   \
  || x == TDMT_VND_ALTER_TABLE    \
  || x == TDMT_VND_DROP_TABLE     \
  || x == TDMT_VND_DELETE         \
)
// clang-format on

typedef struct SWinKey {
  uint64_t groupId;
  TSKEY    ts;
} SWinKey;

typedef struct SSessionKey {
  STimeWindow win;
  uint64_t    groupId;
} SSessionKey;

static inline int winKeyCmprImpl(const void* pKey1, const void* pKey2) {
  SWinKey* pWin1 = (SWinKey*)pKey1;
  SWinKey* pWin2 = (SWinKey*)pKey2;

  if (pWin1->groupId > pWin2->groupId) {
    return 1;
  } else if (pWin1->groupId < pWin2->groupId) {
    return -1;
  }

  if (pWin1->ts > pWin2->ts) {
    return 1;
  } else if (pWin1->ts < pWin2->ts) {
    return -1;
  }

  return 0;
}

static inline int winKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  return winKeyCmprImpl(pKey1, pKey2);
}

typedef struct {
  uint64_t groupId;
  TSKEY    ts;
  int32_t  exprIdx;
} STupleKey;

typedef struct STuplePos {
  union {
    struct {
      int32_t pageId;
      int32_t offset;
    };
    STupleKey streamTupleKey;
  };
} STuplePos;

typedef struct SFirstLastRes {
  bool hasResult;
  // used for last_row function only, isNullRes in SResultRowEntry can not be passed to downstream.So,
  // this attribute is required
  bool      isNull;
  int32_t   bytes;
  int64_t   ts;
  STuplePos pos;
  char      buf[];
} SFirstLastRes;

static inline int STupleKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  STupleKey* pTuple1 = (STupleKey*)pKey1;
  STupleKey* pTuple2 = (STupleKey*)pKey2;

  if (pTuple1->groupId > pTuple2->groupId) {
    return 1;
  } else if (pTuple1->groupId < pTuple2->groupId) {
    return -1;
  }

  if (pTuple1->ts > pTuple2->ts) {
    return 1;
  } else if (pTuple1->ts < pTuple2->ts) {
    return -1;
  }

  if (pTuple1->exprIdx > pTuple2->exprIdx) {
    return 1;
  } else if (pTuple1->exprIdx < pTuple2->exprIdx) {
    return -1;
  }

  return 0;
}

enum {
  TMQ_MSG_TYPE__DUMMY = 0,
  TMQ_MSG_TYPE__POLL_RSP,
  TMQ_MSG_TYPE__POLL_META_RSP,
  TMQ_MSG_TYPE__EP_RSP,
  TMQ_MSG_TYPE__TAOSX_RSP,
  TMQ_MSG_TYPE__END_RSP,
};

enum {
  STREAM_INPUT__DATA_SUBMIT = 1,
  STREAM_INPUT__DATA_BLOCK,
  STREAM_INPUT__MERGED_SUBMIT,
  STREAM_INPUT__TQ_SCAN,
  STREAM_INPUT__DATA_RETRIEVE,
  STREAM_INPUT__GET_RES,
  STREAM_INPUT__CHECKPOINT,
  STREAM_INPUT__REF_DATA_BLOCK,
  STREAM_INPUT__DESTROY,
};

typedef enum EStreamType {
  STREAM_NORMAL = 1,
  STREAM_INVERT,
  STREAM_CLEAR,
  STREAM_INVALID,
  STREAM_GET_ALL,
  STREAM_DELETE_RESULT,
  STREAM_DELETE_DATA,
  STREAM_RETRIEVE,
  STREAM_PULL_DATA,
  STREAM_PULL_OVER,
  STREAM_FILL_OVER,
} EStreamType;

#pragma pack(push, 1)
typedef struct SColumnDataAgg {
  int16_t colId;
  int16_t numOfNull;
  int64_t sum;
  int64_t max;
  int64_t min;
} SColumnDataAgg;
#pragma pack(pop)

typedef struct SDataBlockInfo {
  STimeWindow window;
  int32_t     rows;  // todo hide this attribute
  int32_t     rowSize;
  uint64_t    uid;      // the uid of table, from which current data block comes
  uint16_t    blockId;  // block id, generated by physical planner
  uint64_t    groupId;
  int16_t     hasVarCol;
  uint32_t    capacity;
  // TODO: optimize and remove following
  int64_t     version;    // used for stream, and need serialization
  int32_t     childId;    // used for stream, do not serialize
  EStreamType type;       // used for stream, do not serialize
  STimeWindow calWin;     // used for stream, do not serialize
  TSKEY       watermark;  // used for stream

  char  parTbName[TSDB_TABLE_NAME_LEN];  // used for stream partition
  STag* pTag;                            // used for stream partition
} SDataBlockInfo;

typedef struct SSDataBlock {
  SColumnDataAgg** pBlockAgg;
  SArray*          pDataBlock;  // SArray<SColumnInfoData>
  SDataBlockInfo   info;
} SSDataBlock;

enum {
  FETCH_TYPE__DATA = 1,
  FETCH_TYPE__META,
  FETCH_TYPE__SEP,
  FETCH_TYPE__NONE,
};

typedef struct {
  int8_t       fetchType;
  STqOffsetVal offset;
  union {
    SSDataBlock data;
    void*       meta;
  };
} SFetchRet;

typedef struct SVarColAttr {
  int32_t* offset;    // start position for each entry in the list
  uint32_t length;    // used buffer size that contain the valid data
  uint32_t allocLen;  // allocated buffer size
} SVarColAttr;

// pBlockAgg->numOfNull == info.rows, all data are null
// pBlockAgg->numOfNull == 0, no data are null.
typedef struct SColumnInfoData {
  SColumnInfo info;     // column info
  bool        hasNull;  // if current column data has null value.
  char*       pData;    // the corresponding block data in memory
  union {
    char*       nullbitmap;  // bitmap, one bit for each item in the list
    SVarColAttr varmeta;
  };
} SColumnInfoData;

typedef struct SQueryTableDataCond {
  uint64_t     suid;
  int32_t      order;  // desc|asc order to iterate the data block
  int32_t      numOfCols;
  SColumnInfo* colList;
  int32_t      type;  // data block load type:
  STimeWindow  twindows;
  int64_t      startVersion;
  int64_t      endVersion;
} SQueryTableDataCond;

int32_t tEncodeDataBlock(void** buf, const SSDataBlock* pBlock);
void*   tDecodeDataBlock(const void* buf, SSDataBlock* pBlock);

int32_t tEncodeDataBlocks(void** buf, const SArray* blocks);
void*   tDecodeDataBlocks(const void* buf, SArray** blocks);
void    colDataDestroy(SColumnInfoData* pColData);

//======================================================================================================================
// the following structure shared by parser and executor
typedef struct SColumn {
  union {
    uint64_t uid;
    int64_t  dataBlockId;
  };

  int16_t colId;
  int16_t slotId;

  char    name[TSDB_COL_NAME_LEN];
  int16_t colType;  // column type: normal column, tag, or window column
  int16_t type;
  int32_t bytes;
  uint8_t precision;
  uint8_t scale;
} SColumn;

typedef struct STableBlockDistInfo {
  uint32_t rowSize;
  uint16_t numOfFiles;
  uint32_t numOfTables;
  uint32_t numOfBlocks;
  uint64_t totalSize;
  uint64_t totalRows;
  int32_t  maxRows;
  int32_t  minRows;
  int32_t  defMinRows;
  int32_t  defMaxRows;
  int32_t  firstSeekTimeUs;
  uint32_t numOfInmemRows;
  uint32_t numOfSmallBlocks;
  int32_t  blockRowsHisto[20];
} STableBlockDistInfo;

int32_t tSerializeBlockDistInfo(void* buf, int32_t bufLen, const STableBlockDistInfo* pInfo);
int32_t tDeserializeBlockDistInfo(void* buf, int32_t bufLen, STableBlockDistInfo* pInfo);

enum {
  FUNC_PARAM_TYPE_VALUE = 0x1,
  FUNC_PARAM_TYPE_COLUMN = 0x2,
};

typedef struct SFunctParam {
  int32_t  type;
  SColumn* pCol;
  SVariant param;
} SFunctParam;

// the structure for sql function in select clause
typedef struct SResSchame {
  int8_t  type;
  int32_t slotId;
  int32_t bytes;
  int32_t precision;
  int32_t scale;
  char    name[TSDB_COL_NAME_LEN];
} SResSchema;

typedef struct SExprBasicInfo {
  SResSchema   resSchema;
  int16_t      numOfParams;  // argument value of each function
  SFunctParam* pParam;
} SExprBasicInfo;

typedef struct SExprInfo {
  struct SExprBasicInfo base;
  struct tExprNode*     pExpr;
} SExprInfo;

typedef struct {
  const char* key;
  int32_t     keyLen;
  uint8_t     type;
  union {
    const char* value;
    int64_t     i;
    uint64_t    u;
    double      d;
    float       f;
  };
  int32_t length;
} SSmlKv;

#define QUERY_ASC_FORWARD_STEP  1
#define QUERY_DESC_FORWARD_STEP -1

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

#define SORT_QSORT_T              0x1
#define SORT_SPILLED_MERGE_SORT_T 0x2
typedef struct SSortExecInfo {
  int32_t sortMethod;
  int32_t sortBuffer;
  int32_t loops;       // loop count
  int32_t writeBytes;  // write io bytes
  int32_t readBytes;   // read io bytes
} SSortExecInfo;

// stream special block column

#define START_TS_COLUMN_INDEX           0
#define END_TS_COLUMN_INDEX             1
#define UID_COLUMN_INDEX                2
#define GROUPID_COLUMN_INDEX            3
#define CALCULATE_START_TS_COLUMN_INDEX 4
#define CALCULATE_END_TS_COLUMN_INDEX   5
#define TABLE_NAME_COLUMN_INDEX         6

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_DEF_H_*/