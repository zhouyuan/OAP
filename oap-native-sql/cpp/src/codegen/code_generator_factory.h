#include <arrow/type.h>
#include <gandiva/expression.h>

#include "codegen/arrow_compute/code_generator.h"
#include "codegen/code_generator.h"
#include "codegen/compute_ext/code_generator.h"
#include "codegen/expr_visitor.h"
#include "codegen/gandiva/code_generator.h"

namespace sparkcolumnarplugin {
namespace codegen {
arrow::Status CreateCodeGenerator(
    std::shared_ptr<arrow::Schema> schema_ptr,
    std::vector<std::shared_ptr<::gandiva::Expression>> exprs_vector,
    std::vector<std::shared_ptr<arrow::Field>> ret_types,
    std::shared_ptr<CodeGenerator>* out, bool return_when_finish = false,
    std::vector<std::shared_ptr<::gandiva::Expression>> finish_exprs_vector =
        std::vector<std::shared_ptr<::gandiva::Expression>>()) {
  ExprVisitor nodeVisitor;
  int codegen_type;
  auto status = nodeVisitor.create(exprs_vector, &codegen_type);
  switch (codegen_type) {
    case ARROW_COMPUTE:
      *out = std::make_shared<arrowcompute::ArrowComputeCodeGenerator>(
          schema_ptr, exprs_vector, ret_types, return_when_finish, finish_exprs_vector);
      break;
    case GANDIVA:
      *out = std::make_shared<gandiva::GandivaCodeGenerator>(
          schema_ptr, exprs_vector, ret_types, return_when_finish, finish_exprs_vector);
      break;
    case COMPUTE_EXT:
      *out = std::make_shared<computeext::ComputeExtCodeGenerator>(
          schema_ptr, exprs_vector, ret_types, return_when_finish, finish_exprs_vector);
      break;
    default:
      *out = nullptr;
      status = arrow::Status::TypeError("Unrecognized expression type.");
      break;
  }
  return status;
}
}  // namespace codegen
}  // namespace sparkcolumnarplugin
