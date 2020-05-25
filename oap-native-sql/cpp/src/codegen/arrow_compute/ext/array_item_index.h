#pragma once

#include <cstdint>
namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
struct ArrayItemIndex {
  uint64_t id = 0;
  uint64_t array_id = 0;
  ArrayItemIndex() : array_id(0), id(0) {}
  ArrayItemIndex(uint64_t array_id, uint64_t id) : array_id(array_id), id(id) {}
};
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
