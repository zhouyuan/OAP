#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>
#include <gandiva/gandiva_aliases.h>
#include <gandiva/node.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <chrono>

#include "codegen/code_generator.h"
#include "codegen/code_generator_factory.h"
#include "codegen/common/result_iterator.h"
#include "tests/test_utils.h"

namespace sparkcolumnarplugin {
namespace codegen {

class BenchmarkArrowComputeSort : public ::testing::Test {
 public:
  void SetUp() override {
    // read input from parquet file
#ifdef BENCHMARK_FILE_PATH
    std::string dir_path = BENCHMARK_FILE_PATH;
#else
    std::string dir_path = "";
#endif
    std::string path = dir_path + "tpcds_websales_sort_big.parquet";
    std::cout << "This Benchmark used file " << path
              << ", please download from server "
                 "vsr200://home/zhouyuan/sparkColumnarPlugin/source_files"
              << std::endl;
    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::string file_name;
    ASSERT_OK_AND_ASSIGN(fs, arrow::fs::FileSystemFromUri(path, &file_name));

    ARROW_ASSIGN_OR_THROW(file, fs->OpenInputFile(file_name));

    parquet::ArrowReaderProperties properties(true);
    properties.set_batch_size(4096);
    auto pool = arrow::default_memory_pool();

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(file), properties, &parquet_reader));
    ASSERT_NOT_OK(
        parquet_reader->GetRecordBatchReader({0}, {0, 1, 2}, &record_batch_reader));

    schema = record_batch_reader->schema();
    std::cout << schema->ToString() << std::endl;

    ////////////////// expr prepration ////////////////
    field_list = record_batch_reader->schema()->fields();
    ret_field_list = record_batch_reader->schema()->fields();
  }

  void StartWithIterator() {
    uint64_t elapse_gen = 0;
    uint64_t elapse_read = 0;
    uint64_t elapse_eval = 0;
    uint64_t elapse_sort = 0;
    uint64_t elapse_shuffle = 0;
    uint64_t num_batches = 0;
    std::shared_ptr<CodeGenerator> sort_expr;
    TIME_MICRO_OR_THROW(elapse_gen, CreateCodeGenerator(schema, {sortArrays_expr},
                                                        {f_indices}, &sort_expr, true));

    std::vector<std::shared_ptr<arrow::RecordBatch>> input_batch_list;
    std::vector<std::shared_ptr<arrow::RecordBatch>> dummy_result_batches;
    std::shared_ptr<ResultIterator<arrow::RecordBatch>> sort_result_iterator;

    std::shared_ptr<arrow::RecordBatch> record_batch;

    do {
      TIME_MICRO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      if (record_batch) {
        TIME_MICRO_OR_THROW(elapse_eval,
                            sort_expr->evaluate(record_batch, &dummy_result_batches));
        num_batches += 1;
      }
    } while (record_batch);
    std::cout << "Readed " << num_batches << " batches." << std::endl;
    TIME_MICRO_OR_THROW(elapse_sort, sort_expr->finish(&sort_result_iterator));
    std::shared_ptr<arrow::RecordBatch> result_batch;

    uint64_t num_output_batches = 0;
    while (sort_result_iterator->HasNext()) {
      TIME_MICRO_OR_THROW(elapse_shuffle, sort_result_iterator->Next(&result_batch));
      num_output_batches++;
    }
    arrow::PrettyPrint(*result_batch.get(), 2, &std::cout);

    std::cout << "==================== Summary ====================\n"
              << "BenchmarkArrowComputeJoin processed " << num_batches
              << " batches\nthen output " << num_output_batches
              << " batches\nCodeGen took " << TIME_TO_STRING(elapse_gen)
              << "\nBatch read took " << TIME_TO_STRING(elapse_read)
              << "\nEvaluation took " << TIME_TO_STRING(elapse_eval) << "\nSort took "
              << TIME_TO_STRING(elapse_sort) << "\nShuffle took "
              << TIME_TO_STRING(elapse_shuffle)
              << ".\n================================================" << std::endl;
  }

 protected:
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
  std::shared_ptr<RecordBatchReader> record_batch_reader;
  std::shared_ptr<arrow::Schema> schema;

  std::vector<std::shared_ptr<::arrow::Field>> field_list;
  std::vector<std::shared_ptr<::arrow::Field>> ret_field_list;

  int primary_key_index = 0;
  std::shared_ptr<arrow::Field> f_indices;
  std::shared_ptr<arrow::Field> f_res;
  ::gandiva::ExpressionPtr sortArrays_expr;
  ::gandiva::ExpressionPtr conditionShuffleExpr;
};

TEST_F(BenchmarkArrowComputeSort, SortBenchmark) {
  ////////////////////// prepare expr_vector ///////////////////////
  auto indices_type = std::make_shared<FixedSizeBinaryType>(16);
  f_indices = field("indices", indices_type);
  f_res = field("res", arrow::uint64());

  std::vector<std::shared_ptr<::gandiva::Node>> gandiva_field_list;
  for (auto field : field_list) {
    gandiva_field_list.push_back(TreeExprBuilder::MakeField(field));
  }
  auto n_left =
      TreeExprBuilder::MakeFunction("codegen_left_schema", gandiva_field_list, uint64());
  auto n_right = TreeExprBuilder::MakeFunction("codegen_right_schema", {}, uint64());
  auto n_sort_to_indices =
      TreeExprBuilder::MakeFunction("sortArraysToIndicesNullsFirstAsc",
                                    {gandiva_field_list[primary_key_index]}, uint64());
  sortArrays_expr = TreeExprBuilder::MakeExpression(n_sort_to_indices, f_indices);

  auto n_conditionedShuffleArrayList =
      TreeExprBuilder::MakeFunction("conditionedShuffleArrayList", {}, uint64());
  auto n_codegen_shuffle = TreeExprBuilder::MakeFunction(
      "codegen_withTwoInputs", {n_conditionedShuffleArrayList, n_left, n_right},
      uint64());
  conditionShuffleExpr = TreeExprBuilder::MakeExpression(n_codegen_shuffle, f_res);

  ///////////////////// Calculation //////////////////
  StartWithIterator();
}

}  // namespace codegen
}  // namespace sparkcolumnarplugin
