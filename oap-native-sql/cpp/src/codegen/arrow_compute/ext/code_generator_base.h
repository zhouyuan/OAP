#pragma once
#include <arrow/array.h>
#include <arrow/type.h>
#include "codegen/common/result_iterator.h"

#include <memory>
#include <vector>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
class CodeGenBase {
 public:
  virtual arrow::Status Evaluate(const ArrayList& in) {
    return arrow::Status::NotImplemented("SortBase Evaluate is an abstract interface.");
  }
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("SortBase Finish is an abstract interface.");
  }
  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "SortBase MakeResultIterator is an abstract interface.");
  }
};
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
