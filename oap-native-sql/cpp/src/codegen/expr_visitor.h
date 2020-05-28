#pragma once

#include <iostream>
#include "codegen/code_generator.h"
#include "codegen/common/visitor_base.h"

#define ARROW_COMPUTE 0x0001
#define GANDIVA 0x0002
#define COMPUTE_EXT 0x0003

namespace sparkcolumnarplugin {
namespace codegen {
class ExprVisitor : public VisitorBase {
 public:
  ExprVisitor() {}

  arrow::Status create(std::vector<std::shared_ptr<gandiva::Expression>> exprs_vector,
                       int* out) {
    arrow::Status status = arrow::Status::OK();
    for (auto expr : exprs_vector) {
      status = expr->root()->Accept(*this);
      if (!status.ok()) {
        return status;
      }
    }
    *out = codegen_type;
    return status;
  }

 private:
  // std::vector<std::string> ac{
  //    "sum", "max", "min", "count", "getPrepareFunc", "splitArrayList", "encodeArray"};
  std::vector<std::string> gdv{"add", "substract", "multiply", "divide"};
  std::vector<std::string> ce{};
  int codegen_type;
  arrow::Status Visit(const gandiva::FunctionNode& node);
};
}  // namespace codegen
}  // namespace sparkcolumnarplugin
