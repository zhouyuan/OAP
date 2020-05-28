#pragma once

#include <arrow/array.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>

template <typename T>
class ResultIterator {
 public:
  virtual bool HasNext() { return false; }
  virtual arrow::Status Next(std::shared_ptr<T>* out) {
    return arrow::Status::NotImplemented("ResultIterator abstract Next()");
  }
  virtual arrow::Status Process(
      std::vector<std::shared_ptr<arrow::Array>> in, std::shared_ptr<T>* out,
      const std::shared_ptr<arrow::Array>& selection = nullptr) {
    return arrow::Status::NotImplemented("ResultIterator abstract Process()");
  }
  virtual arrow::Status ProcessAndCacheOne(
      std::vector<std::shared_ptr<arrow::Array>> in,
      const std::shared_ptr<arrow::Array>& selection = nullptr) {
    return arrow::Status::NotImplemented("ResultIterator abstract ProcessAndCacheOne()");
  }
  virtual arrow::Status GetResult(std::shared_ptr<arrow::RecordBatch>* out) {
    return arrow::Status::NotImplemented("ResultIterator abstract GetResult()");
  }
};
