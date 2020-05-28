#pragma once

#include <arrow/type.h>
#include "codegen/code_generator.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace gandiva {

class GandivaCodeGenerator : public CodeGenerator {
 public:
  GandivaCodeGenerator(
      std::shared_ptr<arrow::Schema> schema_ptr,
      std::vector<std::shared_ptr<::gandiva::Expression>> exprs_vector,
      std::vector<std::shared_ptr<arrow::Field>> ret_types, bool return_when_finish,
      std::vector<std::shared_ptr<::gandiva::Expression>> finish_exprs_vector) {}
  ~GandivaCodeGenerator() {}
  arrow::Status getSchema(std::shared_ptr<arrow::Schema>* out) {
    return arrow::Status::OK();
  }
  arrow::Status getResSchema(std::shared_ptr<arrow::Schema>* out) {
    return arrow::Status::OK();
  }
  arrow::Status SetMember(const std::shared_ptr<arrow::RecordBatch>& in) {
    return arrow::Status::OK();
  }
  arrow::Status evaluate(const std::shared_ptr<arrow::RecordBatch>& in,
                         std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    return arrow::Status::OK();
  }
  arrow::Status finish(std::vector<std::shared_ptr<arrow::RecordBatch>>* out) {
    return arrow::Status::OK();
  }
};

}  // namespace gandiva
}  // namespace codegen
}  // namespace sparkcolumnarplugin
