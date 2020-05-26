#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <iostream>
#include <sstream>
#include "sparsehash/dense_hash_map"

#include "codegen/arrow_compute/ext/array_item_index.h"
#include "codegen/arrow_compute/ext/kernels_ext.h"
#include "codegen/common/result_iterator.h"
#include "third_party/arrow/utils/hashing.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <unordered_map>

using google::dense_hash_map;

using namespace sparkcolumnarplugin::codegen::arrowcompute::extra;

class CodeGenBase {
public:
  virtual arrow::Status Evaluate(const ArrayList &in) {
    return arrow::Status::NotImplemented(
        "SortBase Evaluate is an abstract interface.");
  }
  virtual arrow::Status Finish(std::shared_ptr<arrow::Array> *out) {
    return arrow::Status::NotImplemented(
        "SortBase Finish is an abstract interface.");
  }
  virtual arrow::Status
  MakeResultIterator(std::shared_ptr<arrow::Schema> schema,
                     std::shared_ptr<ResultIterator<arrow::RecordBatch>> *out) {
    return arrow::Status::NotImplemented(
        "SortBase MakeResultIterator is an abstract interface.");
  }
};

class TypedProberImpl : public CodeGenBase {
public:
  TypedProberImpl(arrow::compute::FunctionContext *ctx) : ctx_(ctx) {
    /*hash_table_ = std::make_shared<arrow::internal::ScalarMemoTable<int64_t>>(
        ctx_->memory_pool());*/
    // Create Hash Kernel
    auto type_list = {arrow::int64()};
    HashAggrArrayKernel::Make(ctx_, type_list, &hash_kernel_);
    memo_index_to_arrayid_.set_empty_key(0);
    memo_index_to_arrayid_.resize(3500001);
  }
  ~TypedProberImpl() {}

  arrow::Status Evaluate(const ArrayList &in) override {
    //// codegen ////
    cached_0_0_.push_back(std::dynamic_pointer_cast<ArrayType_0_0>(in[0]));
    cached_0_1_.push_back(std::dynamic_pointer_cast<ArrayType_0_1>(in[1]));
    cached_0_2_.push_back(std::dynamic_pointer_cast<ArrayType_0_2>(in[2]));
    // do hash
    std::shared_ptr<arrow::Array> hash_in;
    auto concat_kernel_arr_list = {in[0]};
    RETURN_NOT_OK(hash_kernel_->Evaluate(concat_kernel_arr_list, &hash_in));
    /////////////////
    // we should put items into hashmap
    auto start = std::chrono::steady_clock::now();
    auto typed_array = std::dynamic_pointer_cast<arrow::Int64Array>(hash_in);

    /*auto insert_on_found = [this](int32_t i) {
      memo_index_to_arrayid_[i].emplace_back(cur_array_id_, cur_id_);
    };
    auto insert_on_not_found = [this](int32_t i) {
      map_length_++;
      memo_index_to_arrayid_.push_back(
          {ArrayItemIndex(cur_array_id_, cur_id_)});
    };

    cur_id_ = 0;
    total_length_ += typed_array->length();
    int memo_index = 0;
    if (typed_array->null_count() == 0) {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        hash_table_->GetOrInsert(typed_array->GetView(cur_id_), insert_on_found,
                                 insert_on_not_found, &memo_index);
      }
    } else {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        if (typed_array->IsNull(cur_id_)) {
          hash_table_->GetOrInsertNull(insert_on_found, insert_on_not_found);
        } else {
          hash_table_->GetOrInsert(typed_array->GetView(cur_id_),
                                   insert_on_found, insert_on_not_found,
                                   &memo_index);
        }
      }
    }*/
    cur_id_ = 0;
    total_length_ += typed_array->length();
    int memo_index = 0;
    if (typed_array->null_count() == 0) {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        auto data = typed_array->GetView(cur_id_);
        if (memo_index_to_arrayid_.find(data) == memo_index_to_arrayid_.end()) {
          memo_index_to_arrayid_[data] = {
              ArrayItemIndex(cur_array_id_, cur_id_)};
        } else {
          memo_index_to_arrayid_[data].emplace_back(cur_array_id_, cur_id_);
        }
      }
    } else {
      for (; cur_id_ < typed_array->length(); cur_id_++) {
        if (typed_array->IsNull(cur_id_)) {
          null_arrayid_.emplace_back(cur_array_id_, cur_id_);
        } else {
          auto data = typed_array->GetView(cur_id_);
          if (memo_index_to_arrayid_.find(data) ==
              memo_index_to_arrayid_.end()) {
            memo_index_to_arrayid_[data] = {
                ArrayItemIndex(cur_array_id_, cur_id_)};
          } else {
            memo_index_to_arrayid_[data].emplace_back(cur_array_id_, cur_id_);
          }
        }
      }
    }
    cur_array_id_++;
    auto end = std::chrono::steady_clock::now();
    elapse_ +=
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();
    return arrow::Status::OK();
  }

  arrow::Status MakeResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ResultIterator<arrow::RecordBatch>> *out) override {
    printf("Insert to hash map took %ld ms\n", elapse_ / 1000);
    printf("Total item count is %ld, map length is %ld\n", total_length_,
           map_length_);
    auto start = std::chrono::steady_clock::now();
    *out = std::make_shared<ProberResultIterator>(
        ctx_, schema, hash_kernel_, &memo_index_to_arrayid_, &null_arrayid_,
        //// codegen ////
        cached_0_0_, cached_0_1_, cached_0_2_
        /////////////////
    );
    auto end = std::chrono::steady_clock::now();
    auto elapse =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();
    printf("ResultIterator construct took %ld ms\n", elapse / 1000);
    return arrow::Status::OK();
  }

private:
  uint64_t cur_array_id_ = 0;
  uint64_t cur_id_ = 0;
  uint64_t elapse_ = 0;
  uint64_t total_length_ = 0;
  uint64_t map_length_ = 0;
  arrow::compute::FunctionContext *ctx_;
  std::shared_ptr<KernalBase> hash_kernel_;
  // std::shared_ptr<arrow::internal::ScalarMemoTable<int64_t>> hash_table_;
  // std::vector<std::vector<ArrayItemIndex>> memo_index_to_arrayid_;
  dense_hash_map<int64_t, std::vector<ArrayItemIndex>> memo_index_to_arrayid_;
  std::vector<ArrayItemIndex> null_arrayid_;

  using DataType_0_0 = typename arrow::Int64Type;
  using ArrayType_0_0 = typename arrow::TypeTraits<DataType_0_0>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_0_0>> cached_0_0_;

  using DataType_0_1 = typename arrow::Int64Type;
  using ArrayType_0_1 = typename arrow::TypeTraits<DataType_0_1>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_0_1>> cached_0_1_;

  using DataType_0_2 = typename arrow::Int64Type;
  using ArrayType_0_2 = typename arrow::TypeTraits<DataType_0_2>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_0_2>> cached_0_2_;

  using DataType_1_0 = typename arrow::Int64Type;
  using ArrayType_1_0 = typename arrow::TypeTraits<DataType_1_0>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_1_0>> cached_1_0_;

  using DataType_1_1 = typename arrow::Int64Type;
  using ArrayType_1_1 = typename arrow::TypeTraits<DataType_1_1>::ArrayType;
  std::vector<std::shared_ptr<ArrayType_1_1>> cached_1_1_;

  class ProberResultIterator : public ResultIterator<arrow::RecordBatch> {
  public:
    ProberResultIterator(
        arrow::compute::FunctionContext *ctx,
        std::shared_ptr<arrow::Schema> schema,
        std::shared_ptr<KernalBase> hash_kernel,
        dense_hash_map<int64_t, std::vector<ArrayItemIndex>> *memo_index_to_arrayid,
        std::vector<ArrayItemIndex> *null_arrayid,
        //// codegen ////
        const std::vector<std::shared_ptr<ArrayType_0_0>> &cached_0_0,
        const std::vector<std::shared_ptr<ArrayType_0_1>> &cached_0_1,
        const std::vector<std::shared_ptr<ArrayType_0_2>> &cached_0_2
        /////////////////
        )
        : ctx_(ctx), result_schema_(schema), hash_kernel_(hash_kernel),
          memo_index_to_arrayid_(memo_index_to_arrayid),
          null_arrayid_(null_arrayid) {
      //// codegen ////
      cached_0_0_ = cached_0_0;
      cached_0_1_ = cached_0_1;
      cached_0_2_ = cached_0_2;

      std::unique_ptr<arrow::ArrayBuilder> builder_0_0;
      arrow::MakeBuilder(ctx_->memory_pool(), data_type_0_0, &builder_0_0);
      builder_0_0_.reset(arrow::internal::checked_cast<BuilderType_0_0 *>(
          builder_0_0.release()));

      std::unique_ptr<arrow::ArrayBuilder> builder_0_1;
      arrow::MakeBuilder(ctx_->memory_pool(), data_type_0_1, &builder_0_1);
      builder_0_1_.reset(arrow::internal::checked_cast<BuilderType_0_1 *>(
          builder_0_1.release()));

      std::unique_ptr<arrow::ArrayBuilder> builder_0_2;
      arrow::MakeBuilder(ctx_->memory_pool(), data_type_0_2, &builder_0_2);
      builder_0_2_.reset(arrow::internal::checked_cast<BuilderType_0_2 *>(
          builder_0_2.release()));

      std::unique_ptr<arrow::ArrayBuilder> builder_1_0;
      arrow::MakeBuilder(ctx_->memory_pool(), data_type_1_0, &builder_1_0);
      builder_1_0_.reset(arrow::internal::checked_cast<BuilderType_1_0 *>(
          builder_1_0.release()));

      std::unique_ptr<arrow::ArrayBuilder> builder_1_1;
      arrow::MakeBuilder(ctx_->memory_pool(), data_type_1_1, &builder_1_1);
      builder_1_1_.reset(arrow::internal::checked_cast<BuilderType_1_1 *>(
          builder_1_1.release()));
      /////////////////
    }

    std::string ToString() override { return "ProberResultIterator"; }

    arrow::Status
    Process(const ArrayList &in, std::shared_ptr<arrow::RecordBatch> *out,
            const std::shared_ptr<arrow::Array> &selection) override {
      auto length = in[0]->length();
      uint64_t out_length = 0;
      cached_1_0_ = std::dynamic_pointer_cast<ArrayType_1_0>(in[0]);
      cached_1_1_ = std::dynamic_pointer_cast<ArrayType_1_1>(in[1]);

      std::shared_ptr<arrow::Array> hash_in;
      ArrayList concat_kernel_arr_list;
      concat_kernel_arr_list.push_back(in[0]);
      RETURN_NOT_OK(hash_kernel_->Evaluate(concat_kernel_arr_list, &hash_in));
      auto typed_array = std::dynamic_pointer_cast<arrow::Int64Array>(hash_in);
      // auto typed_array = std::dynamic_pointer_cast<arrow::Int64Array>(in[0]);
      for (int i = 0; i < length; i++) {
        if (!typed_array->IsNull(i)) {
          // auto index = hash_table_->Get(typed_array->GetView(i));
          // if (index != -1) {
          auto index = typed_array->GetView(i);
          if ((*memo_index_to_arrayid_).find(index) !=
              (*memo_index_to_arrayid_).end()) {
            for (auto tmp : (*memo_index_to_arrayid_)[index]) {
              if (ConditionCheck(tmp, i)) {
                RETURN_NOT_OK(builder_0_0_->Append(
                    cached_0_0_[tmp.array_id]->GetView(tmp.id)));
                RETURN_NOT_OK(builder_0_1_->Append(
                    cached_0_1_[tmp.array_id]->GetView(tmp.id)));
                RETURN_NOT_OK(builder_0_2_->Append(
                    cached_0_2_[tmp.array_id]->GetView(tmp.id)));
                RETURN_NOT_OK(builder_1_0_->Append(cached_1_0_->GetView(i)));
                RETURN_NOT_OK(builder_1_1_->Append(cached_1_1_->GetView(i)));
                out_length += 1;
              }
            }
          }
        }
      }
      std::shared_ptr<arrow::Array> out_0_0;
      RETURN_NOT_OK(builder_0_0_->Finish(&out_0_0));
      std::shared_ptr<arrow::Array> out_0_1;
      RETURN_NOT_OK(builder_0_1_->Finish(&out_0_1));
      std::shared_ptr<arrow::Array> out_0_2;
      RETURN_NOT_OK(builder_0_2_->Finish(&out_0_2));
      std::shared_ptr<arrow::Array> out_1_0;
      RETURN_NOT_OK(builder_1_0_->Finish(&out_1_0));
      std::shared_ptr<arrow::Array> out_1_1;
      RETURN_NOT_OK(builder_1_1_->Finish(&out_1_1));

      *out = arrow::RecordBatch::Make(
          result_schema_, out_length,
          {out_0_0, out_0_1, out_0_2, out_1_0, out_1_1});
      return arrow::Status::OK();
    }

  private:
    arrow::compute::FunctionContext *ctx_;
    std::shared_ptr<arrow::Schema> result_schema_;
    std::shared_ptr<KernalBase> hash_kernel_;
    //  std::shared_ptr<arrow::internal::ScalarMemoTable<int64_t>> hash_table_;
    //  std::vector<std::vector<ArrayItemIndex>> *memo_index_to_arrayid_;
    dense_hash_map<int64_t, std::vector<ArrayItemIndex>> *memo_index_to_arrayid_;
    std::vector<ArrayItemIndex> *null_arrayid_;

    using DataType_0_0 = typename arrow::Int64Type;
    using ArrayType_0_0 = typename arrow::TypeTraits<DataType_0_0>::ArrayType;
    using BuilderType_0_0 =
        typename arrow::TypeTraits<DataType_0_0>::BuilderType;
    std::vector<std::shared_ptr<ArrayType_0_0>> cached_0_0_;
    std::shared_ptr<arrow::DataType> data_type_0_0 =
        arrow::TypeTraits<DataType_0_0>::type_singleton();
    std::shared_ptr<BuilderType_0_0> builder_0_0_;

    using DataType_0_1 = typename arrow::Int64Type;
    using ArrayType_0_1 = typename arrow::TypeTraits<DataType_0_1>::ArrayType;
    using BuilderType_0_1 =
        typename arrow::TypeTraits<DataType_0_1>::BuilderType;
    std::vector<std::shared_ptr<ArrayType_0_1>> cached_0_1_;
    std::shared_ptr<arrow::DataType> data_type_0_1 =
        arrow::TypeTraits<DataType_0_1>::type_singleton();
    std::shared_ptr<BuilderType_0_1> builder_0_1_;

    using DataType_0_2 = typename arrow::Int64Type;
    using ArrayType_0_2 = typename arrow::TypeTraits<DataType_0_2>::ArrayType;
    using BuilderType_0_2 =
        typename arrow::TypeTraits<DataType_0_2>::BuilderType;
    std::vector<std::shared_ptr<ArrayType_0_2>> cached_0_2_;
    std::shared_ptr<arrow::DataType> data_type_0_2 =
        arrow::TypeTraits<DataType_0_2>::type_singleton();
    std::shared_ptr<BuilderType_0_2> builder_0_2_;

    using DataType_1_0 = typename arrow::Int64Type;
    using ArrayType_1_0 = typename arrow::TypeTraits<DataType_1_0>::ArrayType;
    using BuilderType_1_0 =
        typename arrow::TypeTraits<DataType_1_0>::BuilderType;
    std::shared_ptr<ArrayType_1_0> cached_1_0_;
    std::shared_ptr<arrow::DataType> data_type_1_0 =
        arrow::TypeTraits<DataType_1_0>::type_singleton();
    std::shared_ptr<BuilderType_1_0> builder_1_0_;

    using DataType_1_1 = typename arrow::Int64Type;
    using ArrayType_1_1 = typename arrow::TypeTraits<DataType_1_1>::ArrayType;
    using BuilderType_1_1 =
        typename arrow::TypeTraits<DataType_1_1>::BuilderType;
    std::shared_ptr<ArrayType_1_1> cached_1_1_;
    std::shared_ptr<arrow::DataType> data_type_1_1 =
        arrow::TypeTraits<DataType_1_1>::type_singleton();
    std::shared_ptr<BuilderType_1_1> builder_1_1_;

    inline bool ConditionCheck(ArrayItemIndex left_index, int right_index) {
      if (cached_0_1_[left_index.array_id]->IsNull(left_index.id)) {
        return false;
      }
      auto input_field_1 =
          cached_0_1_[left_index.array_id]->GetView(left_index.id);
      if (cached_1_1_->IsNull(right_index)) {
        return false;
      }
      auto input_field_2 = cached_1_1_->GetView(right_index);
      return (input_field_1 < input_field_2);
    }
  };
};

extern "C" void MakeCodeGen(arrow::compute::FunctionContext *ctx,
                            std::shared_ptr<CodeGenBase> *out) {
  *out = std::make_shared<TypedProberImpl>(ctx);
}
