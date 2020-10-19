#pragma once

#include <stdlib.h>
#include <string.h>

#include "third_party/row_wise_memory/unsafe_row.h"

#define MAX_HASH_MAP_CAPACITY (1 << 29)  // must be power of 2

#define HASH_NEW_KEY -1
#define HASH_FOUND_MATCH -2
#define HASH_FULL -3

#define loadFactor 0.5

/** HashMap Layout
 *
 * keyArray: Fixed Size Array to map hash key to payloads
 * each item has 8 bytes, 4 for a hash32 key and 4 for location in bytesMap
 * | key-hash(4 bytes) | bytesMap offset(4 bytes) |
 *
 * BytesMap: map to store key and value data
 * each item has format as below, same key items will be linked
 * | total-length(4 bytes) | key-length(4 bytes) | key data(variable size) | value
 *data(variable size) | next value ptr(4 bytes) |
 *
 **/

typedef struct {
  int arrayCapacity;  // The size of the keyArray
  size_t mapSize;     // The size of the bytesMap
  int cursor;
  int numKeys;
  int numValues;
  bool needSpill;
  int* keyArray;   //<32-bit key hash,32-bit offset>, hash slot itself.
  char* bytesMap;  // use to save the  key-row, and value row.
} unsafeHashMap;   /*general purpose hash structure*/

static inline int getTotalLength(char* base) { return *((int*)base); }

static inline int getKeyLength(char* base) { return *((int*)(base + 4)); }

static inline unsafeHashMap* createUnsafeHashMap(int initArrayCapacity,
                                                 int initialHashCapacity) {
  unsafeHashMap* hashMap =
      (unsafeHashMap*)nativeMalloc(sizeof(unsafeHashMap), MEMTYPE_HASHMAP);

  hashMap->keyArray =
      (int*)nativeMalloc(initArrayCapacity * sizeof(int) * 2, MEMTYPE_HASHMAP);
  hashMap->arrayCapacity = initArrayCapacity;
  memset(hashMap->keyArray, -1, initArrayCapacity * sizeof(int) * 2);

  hashMap->bytesMap = (char*)nativeMalloc(initialHashCapacity, MEMTYPE_HASHMAP);
  hashMap->mapSize = initialHashCapacity;

  hashMap->cursor = 0;
  hashMap->numKeys = 0;
  hashMap->numValues = 0;
  hashMap->needSpill = false;
  return hashMap;
}

static inline void destroyHashMap(unsafeHashMap* hm) {
  if (hm != NULL) {
    if (hm->keyArray != NULL) nativeFree(hm->keyArray);
    if (hm->bytesMap != NULL) nativeFree(hm->bytesMap);

    nativeFree(hm);
  }
}

static inline void clearHashMap(unsafeHashMap* hm) {
  memset(hm->keyArray, -1, hm->arrayCapacity * sizeof(int) * 2);

  hm->cursor = 0;
  hm->numKeys = 0;
  hm->numValues = 0;
  hm->needSpill = false;
}

static inline int getRecordLengthFromBytesMap(char* record) {
  int totalLengh = *((int*)record);
  return (4 + totalLengh + 4);
}

static inline int getkLenFromBytesMap(char* record) {
  int klen = *((int*)(record + 4));
  return klen;
}

static inline int getvLenFromBytesMap(char* record) {
  int totalLengh = *((int*)record);
  int klen = getkLenFromBytesMap(record);
  return (totalLengh - 4 - klen);
}

static inline char* getKeyFromBytesMap(char* record) { return (record + 8); }

static inline char* getValueFromBytesMap(char* record) {
  int klen = getkLenFromBytesMap(record);
  return (record + klen + 8);
}

static inline int getNextOffsetFromBytesMap(char* record) {
  int totalLengh = *((int*)(record));
  return *((int*)(record + 4 + totalLengh));
}

static inline int getvLenFromBytesMap(unsafeHashMap* hashMap, int KeyAddressOffset) {
  char* record = hashMap->bytesMap + KeyAddressOffset;
  int totalLengh = *((int*)record);
  int klen = getkLenFromBytesMap(record);
  return (totalLengh - 4 - klen);
}

static inline int getNextOffsetFromBytesMap(unsafeHashMap* hashMap,
                                            int KeyAddressOffset) {
  char* record = hashMap->bytesMap + KeyAddressOffset;
  int totalLengh = *((int*)(record));
  return *((int*)(record + 4 + totalLengh));
}

/**
 * return:
 *   relative offset of record in hashMap->bytesMap
 **/
static inline int getValueFromBytesMapByOffset(unsafeHashMap* hashMap,
                                               int KeyAddressOffset, char* output) {
  char* base = hashMap->bytesMap;
  char* record = base + KeyAddressOffset;
  memcpy(output, getValueFromBytesMap(record), getvLenFromBytesMap(record));
  return KeyAddressOffset;
}

static inline bool growHashBytesMap(unsafeHashMap* hashMap) {
  int oldSize = hashMap->mapSize;
  int newSize = oldSize << 1;
  char* newBytesMap = (char*)nativeRealloc(hashMap->bytesMap, newSize, MEMTYPE_HASHMAP);
  if (newBytesMap == NULL) return false;

  hashMap->bytesMap = newBytesMap;
  hashMap->mapSize = newSize;
  return true;
}

static inline bool growAndRehashKeyArray(unsafeHashMap* hashMap) {
  assert(hashMap->keyArray != NULL);

  int i;
  int oldCapacity = hashMap->arrayCapacity;
  int newCapacity = (oldCapacity << 1);
  newCapacity =
      (newCapacity >= MAX_HASH_MAP_CAPACITY) ? MAX_HASH_MAP_CAPACITY : newCapacity;
  int* oldKeyArray = hashMap->keyArray;

  // Allocate the new keyArray and zero it
  int* newKeyArray = (int*)nativeMalloc(newCapacity * sizeof(int) * 2, MEMTYPE_HASHMAP);
  if (newKeyArray == NULL) return false;

  memset(newKeyArray, -1, newCapacity * sizeof(int) * 2);
  int mask = newCapacity - 1;

  // Rehash the map
  for (i = 0; i < (oldCapacity << 1); i += 2) {
    int keyOffset = oldKeyArray[i];
    if (keyOffset < 0) continue;

    int hashcode = oldKeyArray[i + 1];
    int newPos = hashcode & mask;
    int step = 1;
    while (newKeyArray[newPos * 2] >= 0) {
      newPos = (newPos + step) & mask;
      step++;
    }
    newKeyArray[newPos * 2] = keyOffset;
    newKeyArray[newPos * 2 + 1] = hashcode;
  }

  hashMap->keyArray = newKeyArray;
  hashMap->arrayCapacity = newCapacity;

  nativeFree(oldKeyArray);
  return true;
}

/*
 * return:
 *   0 if exists
 *   -1 if not exists
 */
static inline int safeLookup(unsafeHashMap* hashMap, std::shared_ptr<UnsafeRow> keyRow,
                             int hashVal, std::vector<char*>* output) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRow->sizeInBytes;
  char* base = hashMap->bytesMap;

  while (true) {
    int KeyAddressOffset = hashMap->keyArray[pos * 2];
    int keyHashCode = hashMap->keyArray[pos * 2 + 1];

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.

        char* record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(record), keyLength) == 0)) {
          // there may be more than one record
          while (record != nullptr) {
            (*output).push_back(getValueFromBytesMap(record));
            KeyAddressOffset = getNextOffsetFromBytesMap(record);
            record = KeyAddressOffset == 0 ? nullptr : (base + KeyAddressOffset);
          }
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

/*
 * return:
 *   0 if exists
 *   -1 if not exists
 */
static inline int safeLookup(unsafeHashMap* hashMap, std::shared_ptr<UnsafeRow> keyRow,
                             int hashVal, int numField,
                             std::vector<std::shared_ptr<UnsafeRow>>* output) {
  assert(hashMap->keyArray != NULL);
  int mask = hashMap->arrayCapacity - 1;
  int pos = hashVal & mask;
  int step = 1;
  int keyLength = keyRow->sizeInBytes;
  char* base = hashMap->bytesMap;

  while (true) {
    int KeyAddressOffset = hashMap->keyArray[pos * 2];
    int keyHashCode = hashMap->keyArray[pos * 2 + 1];

    if (KeyAddressOffset < 0) {
      // This is a new key.
      return HASH_NEW_KEY;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.

        char* record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(record), keyLength) == 0)) {
          // there may be more than one record
          while (record != nullptr) {
            auto out_unsafe = std::make_shared<ReadOnlyUnsafeRow>(numField);
            auto valueLen = getvLenFromBytesMap(record);
            out_unsafe.get()->sizeInBytes = valueLen;
            out_unsafe.get()->data = getValueFromBytesMap(record);
            (*output).push_back(out_unsafe);
            KeyAddressOffset = getNextOffsetFromBytesMap(record);
            record = KeyAddressOffset == 0 ? nullptr : (base + KeyAddressOffset);
          }
          return 0;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // Cannot reach here
  assert(0);
}

/**
 * append is used for same key may has multiple value scenario
 * if key does not exists, insert key and append a new record for key value
 * if key exists, append a new record and linked by previous same key record
 *
 * return should be a flag of succession of the append.
 **/
static inline bool append(unsafeHashMap* hashMap, UnsafeRow* keyRow, int hashVal,
                          char* value, size_t value_size) {
  assert(hashMap->keyArray != NULL);
  assert((keyRow->sizeInBytes % 8) == 0);

  const int cursor = hashMap->cursor;
  const int mask = hashMap->arrayCapacity - 1;

  int pos = hashVal & mask;
  int step = 1;

  const int keyLength = keyRow->sizeInBytes;
  char* base = hashMap->bytesMap;
  int klen = keyRow->sizeInBytes;
  const int vlen = value_size;
  const int recordLength = 8 + klen + vlen + 4;
  char* record = nullptr;

  while (true) {
    int KeyAddressOffset = hashMap->keyArray[pos * 2];
    int keyHashCode = hashMap->keyArray[pos * 2 + 1];

    if (KeyAddressOffset < 0) {
      // This is a new key.
      int keyArrayPos = pos;
      record = base + cursor;
      // Update keyArray in hashMap
      hashMap->numKeys++;
      hashMap->keyArray[keyArrayPos * 2] = cursor;
      hashMap->keyArray[keyArrayPos * 2 + 1] = hashVal;
      hashMap->cursor += recordLength;
      hashMap->numValues++;
      break;
    } else {
      if ((int)keyHashCode == hashVal) {
        // Full hash code matches.  Let's compare the keys for equality.
        record = base + KeyAddressOffset;
        if ((getKeyLength(record) == keyLength) &&
            (memcmp(keyRow->data, getKeyFromBytesMap(record), keyLength) == 0)) {
          if (cursor + recordLength >= hashMap->mapSize) {
            // Grow the hash table
            assert(growHashBytesMap(hashMap));
            base = hashMap->bytesMap;
            record = base + cursor;
          }

          // link current record next ptr to new record
          auto nextOffset = (int*)(record + getRecordLengthFromBytesMap(record) - 4);
          while (*nextOffset != 0) {
            record = base + *nextOffset;
            nextOffset = (int*)(record + getRecordLengthFromBytesMap(record) - 4);
          }
          *nextOffset = cursor;
          record = base + cursor;
          klen = 0;

          // Update hashMap
          hashMap->cursor += recordLength;
          hashMap->numValues++;
          break;
        }
      }
    }

    pos = (pos + step) & mask;
    step++;
  }

  // copy keyRow and valueRow into hashmap
  *((int*)record) = klen + vlen + 4;
  *((int*)(record + 4)) = klen;
  memcpy(record + 8, keyRow->data, klen);
  memcpy(record + 8 + klen, value, vlen);
  *((int*)(record + 8 + klen + vlen)) = 0;

  // See if we need to grow keyArray
  int growthThreshold = (int)(hashMap->arrayCapacity * loadFactor);
  if ((hashMap->numKeys > growthThreshold) &&
      (hashMap->arrayCapacity < MAX_HASH_MAP_CAPACITY)) {
    if (!growAndRehashKeyArray(hashMap)) hashMap->needSpill = true;
  }

  return true;
}