#pragma once

#include <arrow/array.h>
#include <arrow/type.h>
#include <gandiva/expression.h>
#include <gandiva/node.h>
#include "codegen/common/result_iterator.h"
namespace sparkcolumnarplugin {
namespace codegen {

class CodeGenerator {
 public:
  explicit CodeGenerator() = default;
  virtual arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) = 0;
  virtual arrow::Status getResSchema(std::shared_ptr<arrow::Schema>* out) = 0;
  virtual arrow::Status SetMember(const std::shared_ptr<arrow::RecordBatch>& ms) = 0;
  virtual arrow::Status evaluate(
      const std::shared_ptr<arrow::RecordBatch>& in,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* out) = 0;
  virtual arrow::Status finish(std::vector<std::shared_ptr<arrow::RecordBatch>>* out) = 0;
  virtual arrow::Status finish(std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "Finish return with ResultIterator is not Implemented");
  }
  virtual arrow::Status SetDependency(
      const std::shared_ptr<ResultIterator<arrow::RecordBatch>>& dependency_iter,
      int index = -1) {
    return arrow::Status::NotImplemented("SetDependency is not Implemented");
  }
  virtual arrow::Status SetResSchema(const std::shared_ptr<arrow::Schema>& in) {
    return arrow::Status::NotImplemented("setResSchema is not Implemented.");
  }
  virtual arrow::Status evaluate(const std::shared_ptr<arrow::Array>& selection_in,
                                 const std::shared_ptr<arrow::RecordBatch>& in,
                                 std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    return arrow::Status::NotImplemented(
        "evaluate with selection array is not Implemented.");
  }
};
}  // namespace codegen
}  // namespace sparkcolumnarplugin
