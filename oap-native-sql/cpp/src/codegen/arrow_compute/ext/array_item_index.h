/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
struct ArrayItemIndex {
  uint16_t id = 0;
  uint16_t array_id = 0;
  bool valid = true;
  ArrayItemIndex() : array_id(0), id(0), valid(true) {}
  ArrayItemIndex(bool valid) : array_id(0), id(0), valid(valid) {}
  ArrayItemIndex(uint16_t array_id, uint16_t id)
      : array_id(array_id), id(id), valid(true) {}
};
struct ArrayItemIndexS {
  uint16_t id = 0;
  uint16_t array_id = 0;
  ArrayItemIndexS() : array_id(0), id(0) {}
  ArrayItemIndexS(bool valid) : array_id(0), id(0) {}
  ArrayItemIndexS(uint16_t array_id, uint16_t id)
      : array_id(array_id), id(id) {}
};
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
