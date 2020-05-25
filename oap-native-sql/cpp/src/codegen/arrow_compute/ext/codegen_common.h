#pragma once
#include <arrow/compute/context.h>
#include <arrow/type.h>

#include <string>

#include "codegen/arrow_compute/ext/code_generator_base.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

std::string BaseCodes();

int FileSpinLock(std::string path);

void FileSpinUnLock(int fd);

std::string GetTypeString(std::shared_ptr<arrow::DataType> type);

arrow::Status CompileCodes(std::string codes, std::string signature);

arrow::Status LoadLibrary(std::string signature, arrow::compute::FunctionContext* ctx,
                          std::shared_ptr<CodeGenBase>* out);
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin