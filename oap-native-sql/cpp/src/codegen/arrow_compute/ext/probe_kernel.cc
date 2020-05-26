#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/context.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/kernels/hash.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include <arrow/visitor_inline.h>
#include <dlfcn.h>
#include <gandiva/configuration.h>
#include <gandiva/node.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <unordered_map>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/code_generator_base.h"
#include "codegen/arrow_compute/ext/codegen_common.h"
#include "codegen/arrow_compute/ext/codegen_node_visitor_v2.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "third_party/arrow/utils/hashing.h"

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {

using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;

///////////////  ConditionedProbeArrays  ////////////////
class ConditionedProbeArraysKernel::Impl {
 public:
  Impl(arrow::compute::FunctionContext* ctx,
       std::vector<std::shared_ptr<arrow::Field>> left_key_list,
       std::vector<std::shared_ptr<arrow::Field>> right_key_list,
       std::shared_ptr<gandiva::Node> func_node, int join_type,
       std::vector<std::shared_ptr<arrow::Field>> left_field_list,
       std::vector<std::shared_ptr<arrow::Field>> right_field_list)
      : ctx_(ctx), join_type_(join_type) {
    for (auto key_field : left_key_list) {
      int i = 0;
      for (auto field : left_field_list) {
        if (key_field->name() == field->name()) {
          break;
        }
        i++;
      }
      left_key_indices_.push_back(i);
    }
    for (auto key_field : right_key_list) {
      int i = 0;
      for (auto field : right_field_list) {
        if (key_field->name() == field->name()) {
          break;
        }
        i++;
      }
      right_key_indices_.push_back(i);
    }
    auto status = LoadJITFunction(func_node, left_field_list, right_field_list, &prober_);
    if (!status.ok()) {
      std::cout << "LoadJITFunction failed, msg is " << status.message() << std::endl;
      throw;
    }
  }

  arrow::Status Evaluate(const ArrayList& in_arr_list) {
    // cache in_arr_list for prober data shuffling
    RETURN_NOT_OK(prober_->Evaluate(in_arr_list));
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
    RETURN_NOT_OK(prober_->MakeResultIterator(schema, out));
    return arrow::Status::OK();
  }

 private:
  using ArrayType = typename arrow::TypeTraits<arrow::Int64Type>::ArrayType;

  int join_type_;
  arrow::compute::FunctionContext* ctx_;
  std::vector<int> left_key_indices_;
  std::vector<int> right_key_indices_;
  std::shared_ptr<CodeGenBase> prober_;

  arrow::Status LoadJITFunction(
      std::shared_ptr<gandiva::Node> func_node,
      std::vector<std::shared_ptr<arrow::Field>> left_field_list,
      std::vector<std::shared_ptr<arrow::Field>> right_field_list,
      std::shared_ptr<CodeGenBase>* out) {
    // generate ddl signature
    std::stringstream func_args_ss;
    func_args_ss << func_node->ToString();
    for (auto field : left_field_list) {
      func_args_ss << field->ToString();
    }
    for (auto field : right_field_list) {
      func_args_ss << field->ToString();
    }

    std::stringstream signature_ss;
    signature_ss << std::hex << std::hash<std::string>{}(func_args_ss.str());
    std::string signature = signature_ss.str();
    std::cout << "LoadJITFunction signature is " << signature << std::endl;

    auto file_lock = FileSpinLock("/tmp");
    auto status = LoadLibrary(signature, ctx_, out);
    if (!status.ok()) {
      // process
      auto codes = ProduceCodes(func_node, left_field_list, right_field_list);
      // compile codes
      RETURN_NOT_OK(CompileCodes(codes, signature));
      RETURN_NOT_OK(LoadLibrary(signature, ctx_, out));
    }
    FileSpinUnLock(file_lock);
    return arrow::Status::OK();
  }

  std::string ProduceCodes(std::shared_ptr<gandiva::Node> func_node,
                           std::vector<std::shared_ptr<arrow::Field>> left_field_list,
                           std::vector<std::shared_ptr<arrow::Field>> right_field_list) {
    return BaseCodes() + R"()";
  }

  std::string GetCompFunction() {
    // CodeGen
    std::stringstream codes_ss;
    codes_ss << R"(#include <arrow/type.h>
#include <algorithm>
#define int8 int8_t
#define int16 int16_t
#define int32 int32_t
#define int64 int64_t
#define uint8 uint8_t
#define uint16 uint16_t
#define uint32 uint32_t
#define uint64 uint64_t

struct ArrayItemIndex {
  uint64_t id = 0;
  uint64_t array_id = 0;
  ArrayItemIndex(uint64_t array_id, uint64_t id) : array_id(array_id), id(id) {}
};

class ConditionerBase {
public:
  virtual arrow::Status Submit(
      std::vector<std::function<bool(ArrayItemIndex)>> left_is_null_func_list,
      std::vector<std::function<void *(ArrayItemIndex)>> left_get_func_list,
      std::vector<std::function<bool(int)>> right_is_null_func_list,
      std::vector<std::function<void *(int)>> right_get_func_list,
      std::function<bool(ArrayItemIndex, int)> *out) {
    return arrow::Status::NotImplemented(
        "ConditionerBase Submit is an abstract interface.");
  }
};

class Conditioner : public ConditionerBase {
public:
  arrow::Status Submit(
      std::vector<std::function<bool(ArrayItemIndex)>> left_is_null_func_list,
      std::vector<std::function<void*(ArrayItemIndex)>> left_get_func_list,
      std::vector<std::function<bool(int)>> right_is_null_func_list,
      std::vector<std::function<void*(int)>> right_get_func_list,
      std::function<bool(ArrayItemIndex, int)>* out) override {
    left_is_null_func_list_ = left_is_null_func_list;
    left_get_func_list_ = left_get_func_list;
    right_is_null_func_list_ = right_is_null_func_list;
    right_get_func_list_ = right_get_func_list;
    *out = [this](ArrayItemIndex left_index, int right_index) {

  std::shared_ptr<CodeGenNodeVisitorV2> func_node_visitor;
  int func_count = 0;
  MakeCodeGenNodeVisitorV2(func_node, {left_field_list, right_field_list}, &func_count,
                           &codes_ss, &func_node_visitor);
  codes_ss << "      return (" << func_node_visitor->GetResult() << ");" << std::endl;
  codes_ss << R"(
    };
    return arrow::Status::OK();
  }
private:
  std::vector<std::function<bool(ArrayItemIndex)>> left_is_null_func_list_;
  std::vector<std::function<void *(ArrayItemIndex)>> left_get_func_list_;
  std::vector<std::function<bool(int)>> right_is_null_func_list_;
  std::vector<std::function<void *(int)>> right_get_func_list_;
};

extern "C" void MakeConditioner(std::shared_ptr<ConditionerBase> *out) {
  *out = std::make_shared<Conditioner>();
})";
    return codes_ss.str();
  }

  std::string GetProbeFunction(int join_type) {
    std::stringstream codes_ss;

    switch (join_type) {
      case 0: { /*Inner Join*/
        codes_ss << R"(
          // prepare
          std::unique_ptr<arrow::FixedSizeBinaryBuilder> left_indices_builder;
          auto left_array_type = arrow::fixed_size_binary(sizeof(ArrayItemIndex));
          left_indices_builder.reset(
              new arrow::FixedSizeBinaryBuilder(left_array_type, ctx_->memory_pool()));

          std::unique_ptr<arrow::UInt32Builder> right_indices_builder;
          right_indices_builder.reset(
              new arrow::UInt32Builder(arrow::uint32(), ctx_->memory_pool()));

          auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
          for (int i = 0; i < typed_array->length(); i++) {
            if (!typed_array->IsNull(i)) {
              auto index = hash_table_->Get(typed_array->GetView(i));
              if (index != -1) {
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              }
            }
          }
          // create buffer and null_vector to FixedSizeBinaryArray
          std::shared_ptr<arrow::Array> left_arr_out;
          std::shared_ptr<arrow::Array> right_arr_out;
          RETURN_NOT_OK(left_indices_builder->Finish(&left_arr_out));
          RETURN_NOT_OK(right_indices_builder->Finish(&right_arr_out));
          auto result_schema =
              arrow::schema({arrow::field("left_indices", left_array_type),
                             arrow::field("right_indices", arrow::uint32())});
          *out = arrow::RecordBatch::Make(result_schema, right_arr_out->length(),
                                          {left_arr_out, right_arr_out});
          return arrow::Status::OK();
      )";
      } break;
      case 1: { /*Outer Join*/
        codes_ss << R"(
          std::unique_ptr<arrow::FixedSizeBinaryBuilder> left_indices_builder;
          auto left_array_type = arrow::fixed_size_binary(sizeof(ArrayItemIndex));
          left_indices_builder.reset(
              new arrow::FixedSizeBinaryBuilder(left_array_type, ctx_->memory_pool()));

          std::unique_ptr<arrow::UInt32Builder> right_indices_builder;
          right_indices_builder.reset(
              new arrow::UInt32Builder(arrow::uint32(), ctx_->memory_pool()));

          auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
          for (int i = 0; i < typed_array->length(); i++) {
            if (typed_array->IsNull(i)) {
              auto index = hash_table_->GetNull();
              if (index == -1) {
                RETURN_NOT_OK(left_indices_builder->AppendNull());
                RETURN_NOT_OK(right_indices_builder->Append(i));
              } else {
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              }
            } else {
              auto index = hash_table_->Get(typed_array->GetView(i));
              if (index == -1) {
                RETURN_NOT_OK(left_indices_builder->AppendNull());
                RETURN_NOT_OK(right_indices_builder->Append(i));
              } else {
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    RETURN_NOT_OK(left_indices_builder->Append((uint8_t*)&tmp));
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                  }
                }
              }
            }
          }
          // create buffer and null_vector to FixedSizeBinaryArray
          std::shared_ptr<arrow::Array> left_arr_out;
          std::shared_ptr<arrow::Array> right_arr_out;
          RETURN_NOT_OK(left_indices_builder->Finish(&left_arr_out));
          RETURN_NOT_OK(right_indices_builder->Finish(&right_arr_out));
          auto result_schema =
              arrow::schema({arrow::field("left_indices", left_array_type),
                             arrow::field("right_indices", arrow::uint32())});
          *out = arrow::RecordBatch::Make(result_schema, right_arr_out->length(),
                                          {left_arr_out, right_arr_out});

          return arrow::Status::OK();
          )";
      } break;
      case 2: { /*Anti Join*/
        codes_ss << R"(
          std::unique_ptr<arrow::FixedSizeBinaryBuilder> left_indices_builder;
          auto left_array_type = arrow::fixed_size_binary(sizeof(ArrayItemIndex));
          left_indices_builder.reset(
              new arrow::FixedSizeBinaryBuilder(left_array_type, ctx_->memory_pool()));

          std::unique_ptr<arrow::UInt32Builder> right_indices_builder;
          right_indices_builder.reset(
              new arrow::UInt32Builder(arrow::uint32(), ctx_->memory_pool()));

          auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
          for (int i = 0; i < typed_array->length(); i++) {
            if (!typed_array->IsNull(i)) {
              auto index = hash_table_->Get(typed_array->GetView(i));
              if (index == -1) {
                RETURN_NOT_OK(left_indices_builder->AppendNull());
                RETURN_NOT_OK(right_indices_builder->Append(i));
              } else {
                bool found = false;
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    found = true;
                    break;
                  }
                }
                if (!found) {
                  RETURN_NOT_OK(left_indices_builder->AppendNull());
                  RETURN_NOT_OK(right_indices_builder->Append(i));
                }
              }
            } else {
              auto index = hash_table_->GetNull();
              if (index == -1) {
                RETURN_NOT_OK(left_indices_builder->AppendNull());
                RETURN_NOT_OK(right_indices_builder->Append(i));
              } else {
                bool found = false;
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    found = true;
                    break;
                  }
                }
                if (!found) {
                  RETURN_NOT_OK(left_indices_builder->AppendNull());
                  RETURN_NOT_OK(right_indices_builder->Append(i));
                }
              }
            }
          }

          // create buffer and null_vector to FixedSizeBinaryArray
          std::shared_ptr<arrow::Array> left_arr_out;
          std::shared_ptr<arrow::Array> right_arr_out;
          RETURN_NOT_OK(left_indices_builder->Finish(&left_arr_out));
          RETURN_NOT_OK(right_indices_builder->Finish(&right_arr_out));
          auto result_schema =
              arrow::schema({arrow::field("left_indices", left_array_type),
                             arrow::field("right_indices", arrow::uint32())});
          *out = arrow::RecordBatch::Make(result_schema, right_arr_out->length(),
                                          {left_arr_out, right_arr_out});
          return arrow::Status::OK();
          )";
      } break;
      case 3: { /*Semi Join*/
        // prepare
        codes_ss << R"(
          std::unique_ptr<arrow::FixedSizeBinaryBuilder> left_indices_builder;
          auto left_array_type = arrow::fixed_size_binary(sizeof(ArrayItemIndex));
          left_indices_builder.reset(
              new arrow::FixedSizeBinaryBuilder(left_array_type, ctx_->memory_pool()));

          std::unique_ptr<arrow::UInt32Builder> right_indices_builder;
          right_indices_builder.reset(
              new arrow::UInt32Builder(arrow::uint32(), ctx_->memory_pool()));

          auto typed_array = std::dynamic_pointer_cast<ArrayType>(in);
          for (int i = 0; i < typed_array->length(); i++) {
            if (!typed_array->IsNull(i)) {
              auto index = hash_table_->Get(typed_array->GetView(i));
              if (index != -1) {
                for (auto tmp : memo_index_to_arrayid_[index]) {
                  if (ConditionCheck(tmp, i)) {
                    RETURN_NOT_OK(left_indices_builder->AppendNull());
                    RETURN_NOT_OK(right_indices_builder->Append(i));
                    break;
                  }
                }
              }
            }
          }
          // create buffer and null_vector to FixedSizeBinaryArray
          std::shared_ptr<arrow::Array> left_arr_out;
          std::shared_ptr<arrow::Array> right_arr_out;
          RETURN_NOT_OK(left_indices_builder->Finish(&left_arr_out));
          RETURN_NOT_OK(right_indices_builder->Finish(&right_arr_out));
          auto result_schema =
              arrow::schema({arrow::field("left_indices", left_array_type),
                             arrow::field("right_indices", arrow::uint32())});
          *out = arrow::RecordBatch::Make(result_schema, right_arr_out->length(),
                                          {left_arr_out, right_arr_out});
          return arrow::Status::OK();
          )";
      } break;
      default:
        std::cout << "ConditionedProbeArraysTypedImpl only support join type: InnerJoin, "
                     "RightJoin"
                  << std::endl;
        throw;
    }
    return codes_ss.str();
  }

};  // namespace extra

arrow::Status ConditionedProbeArraysKernel::Make(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> left_key_list,
    std::vector<std::shared_ptr<arrow::Field>> right_key_list,
    std::shared_ptr<gandiva::Node> func_node, int join_type,
    std::vector<std::shared_ptr<arrow::Field>> left_field_list,
    std::vector<std::shared_ptr<arrow::Field>> right_field_list,
    std::shared_ptr<KernalBase>* out) {
  *out = std::make_shared<ConditionedProbeArraysKernel>(
      ctx, left_key_list, right_key_list, func_node, join_type, left_field_list,
      right_field_list);
  return arrow::Status::OK();
}

ConditionedProbeArraysKernel::ConditionedProbeArraysKernel(
    arrow::compute::FunctionContext* ctx,
    std::vector<std::shared_ptr<arrow::Field>> left_key_list,
    std::vector<std::shared_ptr<arrow::Field>> right_key_list,
    std::shared_ptr<gandiva::Node> func_node, int join_type,
    std::vector<std::shared_ptr<arrow::Field>> left_field_list,
    std::vector<std::shared_ptr<arrow::Field>> right_field_list) {
  impl_.reset(new Impl(ctx, left_key_list, right_key_list, func_node, join_type,
                       left_field_list, right_field_list));
  kernel_name_ = "ConditionedProbeArraysKernel";
}

arrow::Status ConditionedProbeArraysKernel::Evaluate(const ArrayList& in) {
  return impl_->Evaluate(in);
}

arrow::Status ConditionedProbeArraysKernel::MakeResultIterator(
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResultIterator<arrow::RecordBatch>>* out) {
  return impl_->MakeResultIterator(schema, out);
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
