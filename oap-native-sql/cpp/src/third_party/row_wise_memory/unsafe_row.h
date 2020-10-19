#pragma once

#include <arrow/util/decimal.h>
#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include <string>

#include "third_party/row_wise_memory/native_memory.h"

#define TEMP_UNSAFEROW_BUFFER_SIZE 1024

/* Unsafe Row Layout as below
 *
 * | validity | col 0 | col 1 | col 2 | ... | cursor |
 * explain:
 * validity: 64 * n fields = 8 * n bytes
 * col: each col is 8 bytes, string or decimal col should be offsetAndLength
 * cursor: used by string data and decimal
 *
 */
struct UnsafeRow {
  int numFields;
  int sizeInBytes;
  char* data = nullptr;
  int cursor;
  UnsafeRow() {}
  UnsafeRow(int numFields) : numFields(numFields) {
    auto validity_size = ((numFields + 63) / 64) * 8;
    cursor = validity_size + numFields * 8LL;
    data = (char*)nativeMalloc(TEMP_UNSAFEROW_BUFFER_SIZE, MEMTYPE_ROW);
    memset(data, 0, validity_size);
    sizeInBytes = cursor;
  }
  virtual ~UnsafeRow() {
    if (data) {
      nativeFree(data);
    }
  }
};

struct ReadOnlyUnsafeRow : public UnsafeRow {
  ReadOnlyUnsafeRow(int _numFields) {
    auto validity_size = ((numFields + 63) / 64) * 8;
    cursor = validity_size + numFields * 8LL;
    numFields = _numFields;
  }
  ~ReadOnlyUnsafeRow() override { data = nullptr; }
};

static inline int calculateBitSetWidthInBytes(int numFields) {
  return ((numFields + 63) / 64) * 8;
}

static inline int64_t getFieldOffset(UnsafeRow* row, int ordinal) {
  int bitSetWidthInBytes = calculateBitSetWidthInBytes(row->numFields);
  return bitSetWidthInBytes + ordinal * 8LL;
}

static inline int getSizeInBytes(UnsafeRow* row) { return row->sizeInBytes; }

static inline bool isEqualUnsafeRow(UnsafeRow* row0, UnsafeRow* row1) {
  if (row0->sizeInBytes != row1->sizeInBytes) return false;

  return (memcmp(row0->data, row1->data, row0->sizeInBytes) == 0);
}

static inline int roundNumberOfBytesToNearestWord(int numBytes) {
  int remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
  if (remainder == 0) {
    return numBytes;
  } else {
    return numBytes + (8 - remainder);
  }
}

static inline void zeroOutPaddingBytes(UnsafeRow* row, int numBytes) {
  if ((numBytes & 0x07) > 0) {
    *((int64_t*)(char*)(row->data + row->cursor + ((numBytes >> 3) << 3))) = 0L;
  }
}

static inline void setNullAt(UnsafeRow* row, int index) {
  assert((index >= 0) && (index < row->numFields));
  int64_t mask = 1LL << (index & 0x3f);  // mod 64 and shift
  int64_t wordOffset = (index >> 6) * WORD_SIZE;
  int64_t word = *((int64_t*)(char*)(row->data + wordOffset));
  // set validity
  *((int64_t*)(row->data + wordOffset)) = word | mask;
  // set data
  *((int64_t*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL)) = 0;
}

static inline void setNotNullAt(UnsafeRow* row, int index) {
  assert((index >= 0) && (index < row->numFields));
  int64_t mask = 1LL << (index & 0x3f);  // mod 64 and shift
  int64_t wordOffset = (index >> 6) * WORD_SIZE;
  int64_t word = *((int64_t*)(char*)(row->data + wordOffset));
  // set validity
  *((int64_t*)(row->data + wordOffset)) = word & ~mask;
}

static inline void setOffsetAndSize(UnsafeRow* row, int ordinal, int64_t size) {
  int64_t relativeOffset = row->cursor;
  int64_t fieldOffset = ((row->numFields + 63) / 64) * 8 + ordinal * 8LL;
  int64_t offsetAndSize = (relativeOffset << 32) | size;
  // set data
  *((int64_t*)(char*)(row->data + fieldOffset)) = offsetAndSize;
}

static inline bool isNull(UnsafeRow* row, int index) {
  assert((index >= 0) && (index < row->numFields));
  int64_t mask = 1LL << (index & 0x3f);  // mod 64 and shift
  int64_t wordOffset = (index >> 6) * WORD_SIZE;
  int64_t word = *((int64_t*)(char*)(row->data + wordOffset));
  return (word & mask) != 0;
}

static inline void getValue(UnsafeRow* row, int index, bool* val) {
  assert((index >= 0) && (index < row->numFields));
  *val = *((bool*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, int8_t* val) {
  assert((index >= 0) && (index < row->numFields));
  *val = *((int8_t*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, uint8_t* val) {
  assert((index >= 0) && (index < row->numFields));
  *val = *((uint8_t*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, int16_t* val) {
  assert((index >= 0) && (index < row->numFields));
  *val = *((int16_t*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, uint16_t* val) {
  assert((index >= 0) && (index < row->numFields));
  *val =
      *((uint16_t*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, int32_t* val) {
  assert((index >= 0) && (index < row->numFields));
  *val = *((int32_t*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, uint32_t* val) {
  assert((index >= 0) && (index < row->numFields));
  *val =
      *((uint32_t*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, int64_t* val) {
  assert((index >= 0) && (index < row->numFields));
  *val = *((int64_t*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, uint64_t* val) {
  assert((index >= 0) && (index < row->numFields));
  *val =
      *((uint64_t*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, float* val) {
  assert((index >= 0) && (index < row->numFields));
  *val = *((float*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, double* val) {
  assert((index >= 0) && (index < row->numFields));
  *val = *((double*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
}

static inline void getValue(UnsafeRow* row, int index, std::string* val) {
  assert((index >= 0) && (index < row->numFields));

  int64_t offsetAndSize =
      *((uint64_t*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
  int offset = (int)(offsetAndSize >> 32);
  int size = (int)offsetAndSize;
  *val = std::string((char*)(row->data + offset), size);
}

static inline void getValue(UnsafeRow* row, int index, arrow::Decimal128* val) {
  assert((index >= 0) && (index < row->numFields));
  int64_t offsetAndSize =
      *((uint64_t*)(char*)(row->data + ((row->numFields + 63) / 64) * 8 + index * 8LL));
  int offset = (int)(offsetAndSize >> 32);
  int size = (int)offsetAndSize;
  memcpy(val, row->data + offset, size);
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, bool val) {
  setNotNullAt(row, index);
  int64_t offset = ((row->numFields + 63) / 64) * 8 + index * 8LL;
  *((int64_t*)(row->data + offset)) = val;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, int8_t val) {
  setNotNullAt(row, index);
  int64_t offset = ((row->numFields + 63) / 64) * 8 + index * 8LL;
  *((int64_t*)(row->data + offset)) = val;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, uint8_t val) {
  appendToUnsafeRow(row, index, static_cast<int8_t>(val));
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, int16_t val) {
  setNotNullAt(row, index);
  int64_t offset = ((row->numFields + 63) / 64) * 8 + index * 8LL;
  uint64_t longValue = (uint64_t)(uint16_t)val;
  *((int64_t*)(char*)(row->data + offset)) = longValue;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, uint16_t val) {
  appendToUnsafeRow(row, index, static_cast<int16_t>(val));
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, int32_t val) {
  setNotNullAt(row, index);
  int64_t offset = ((row->numFields + 63) / 64) * 8 + index * 8LL;
  uint64_t longValue = (uint64_t)(unsigned int)val;
  *((int64_t*)(char*)(row->data + offset)) = longValue;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, uint32_t val) {
  appendToUnsafeRow(row, index, static_cast<int32_t>(val));
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, int64_t val) {
  setNotNullAt(row, index);
  int64_t offset = ((row->numFields + 63) / 64) * 8 + index * 8LL;
  *((int64_t*)(char*)(row->data + offset)) = val;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, uint64_t val) {
  appendToUnsafeRow(row, index, static_cast<int64_t>(val));
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, float val) {
  setNotNullAt(row, index);
  int64_t offset = ((row->numFields + 63) / 64) * 8 + index * 8LL;
  double longValue = (double)val;
  *((double*)(char*)(row->data + offset)) = val;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, double val) {
  setNotNullAt(row, index);
  int64_t offset = ((row->numFields + 63) / 64) * 8 + index * 8LL;
  *((double*)(char*)(row->data + offset)) = val;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, std::string str) {
  int numBytes = str.size();
  int roundedSize = roundNumberOfBytesToNearestWord(numBytes);

  setNotNullAt(row, index);

  zeroOutPaddingBytes(row, numBytes);

  memcpy(row->data + row->cursor, str.c_str(), numBytes);

  setOffsetAndSize(row, index, numBytes);

  // move the cursor forward.
  row->cursor += roundedSize;
  row->sizeInBytes = row->cursor;
}

static inline void appendToUnsafeRow(UnsafeRow* row, int index, arrow::Decimal128 dcm) {
  int numBytes = 16;

  setNotNullAt(row, index);

  zeroOutPaddingBytes(row, numBytes);

  memcpy(row->data + row->cursor, dcm.ToBytes().data(), numBytes);

  setOffsetAndSize(row, index, numBytes);

  // move the cursor forward.
  row->cursor += numBytes;
  row->sizeInBytes = row->cursor;
}
