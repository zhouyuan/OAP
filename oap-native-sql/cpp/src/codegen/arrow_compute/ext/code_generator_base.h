#pragma once
#include <arrow/array.h>
#include <arrow/type.h>
#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/common/result_iterator.h"
#include "third_party/arrow/utils/hashing.h"

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
    return arrow::Status::NotImplemented(
        "CodeGenBase Evaluate is an abstract interface.");
  }
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array>* out) {
    return arrow::Status::NotImplemented("CodeGenBase Finish is an abstract interface.");
  }
  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "CodeGenBase MakeResultIterator is an abstract interface.");
  }
  virtual arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema, std::shared_ptr<extra::KernalBase> kernel,
      std::shared_ptr<arrow::internal::ScalarMemoTable<int64_t>> hash_table_,
      std::vector<std::vector<ArrayItemIndex>> memo_index_to_arrayid_,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "codeGenBase MakeResultIterator is an abstract interface.");
  }
};
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
