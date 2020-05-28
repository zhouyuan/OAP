#include "codegen/expr_visitor.h"

#include <arrow/status.h>
namespace sparkcolumnarplugin {
namespace codegen {
arrow::Status ExprVisitor::Visit(const gandiva::FunctionNode& node) {
  auto desc = node.descriptor();
  if (std::find(gdv.begin(), gdv.end(), desc->name()) != gdv.end()) {
    // gandiva can handle this
    codegen_type = GANDIVA;
  } else if (std::find(ce.begin(), ce.end(), desc->name()) != ce.end()) {
    // we need to implement our own function to handle this
    codegen_type = COMPUTE_EXT;
  } else {
    // arrow compute can handle this
    codegen_type = ARROW_COMPUTE;
  }
  return arrow::Status::OK();
}
}  // namespace codegen
}  // namespace sparkcolumnarplugin
