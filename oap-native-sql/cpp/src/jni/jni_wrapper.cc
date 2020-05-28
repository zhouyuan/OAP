#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <gandiva/jni/protobuf_utils.h>
#include <jni.h>
#include <iostream>
#include <string>

#include "codegen/code_generator_factory.h"
#include "codegen/common/result_iterator.h"
#include "jni/concurrent_map.h"
#include "jni/jni_common.h"

namespace types {
class ExpressionList;
}  // namespace types

static jclass arrow_record_batch_builder_class;
static jmethodID arrow_record_batch_builder_constructor;

static jclass arrow_field_node_builder_class;
static jmethodID arrow_field_node_builder_constructor;

static jclass arrowbuf_builder_class;
static jmethodID arrowbuf_builder_constructor;

using arrow::jni::ConcurrentMap;
static ConcurrentMap<std::shared_ptr<arrow::Buffer>> buffer_holder_;

static jclass io_exception_class;
static jclass illegal_argument_exception_class;

static jint JNI_VERSION = JNI_VERSION_1_8;

using CodeGenerator = sparkcolumnarplugin::codegen::CodeGenerator;
static arrow::jni::ConcurrentMap<std::shared_ptr<CodeGenerator>> handler_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<ResultIterator<arrow::RecordBatch>>>
    batch_iterator_holder_;

std::shared_ptr<CodeGenerator> GetCodeGenerator(JNIEnv* env, jlong id) {
  auto handler = handler_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return handler;
}

std::shared_ptr<ResultIterator<arrow::RecordBatch>> GetBatchIterator(JNIEnv* env,
                                                                     jlong id) {
  auto handler = batch_iterator_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return handler;
}

jobject MakeRecordBatchBuilder(JNIEnv* env, std::shared_ptr<arrow::Schema> schema,
                               std::shared_ptr<arrow::RecordBatch> record_batch) {
  jobjectArray field_array =
      env->NewObjectArray(schema->num_fields(), arrow_field_node_builder_class, nullptr);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto column = record_batch->column(i);
    auto dataArray = column->data();
    jobject field = env->NewObject(arrow_field_node_builder_class,
                                   arrow_field_node_builder_constructor, column->length(),
                                   column->null_count());
    env->SetObjectArrayElement(field_array, i, field);

    for (auto& buffer : dataArray->buffers) {
      buffers.push_back(buffer);
    }
  }

  jobjectArray arrowbuf_builder_array =
      env->NewObjectArray(buffers.size(), arrowbuf_builder_class, nullptr);

  for (size_t j = 0; j < buffers.size(); ++j) {
    auto buffer = buffers[j];
    uint8_t* data = nullptr;
    int size = 0;
    int64_t capacity = 0;
    if (buffer != nullptr) {
      data = (uint8_t*)buffer->data();
      size = (int)buffer->size();
      capacity = buffer->capacity();
    }
    jobject arrowbuf_builder =
        env->NewObject(arrowbuf_builder_class, arrowbuf_builder_constructor,
                       buffer_holder_.Insert(buffer), data, size, capacity);
    env->SetObjectArrayElement(arrowbuf_builder_array, j, arrowbuf_builder);
  }

  // create RecordBatch
  jobject arrow_record_batch_builder = env->NewObject(
      arrow_record_batch_builder_class, arrow_record_batch_builder_constructor,
      record_batch->num_rows(), field_array, arrowbuf_builder_array);
  return arrow_record_batch_builder;
}

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  io_exception_class = CreateGlobalClassReference(env, "Ljava/io/IOException;");
  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");

  arrow_record_batch_builder_class = CreateGlobalClassReference(
      env, "Lcom/intel/sparkColumnarPlugin/vectorized/ArrowRecordBatchBuilder;");
  arrow_record_batch_builder_constructor =
      GetMethodID(env, arrow_record_batch_builder_class, "<init>",
                  "(I[Lcom/intel/sparkColumnarPlugin/vectorized/ArrowFieldNodeBuilder;"
                  "[Lcom/intel/sparkColumnarPlugin/vectorized/ArrowBufBuilder;)V");

  arrow_field_node_builder_class = CreateGlobalClassReference(
      env, "Lcom/intel/sparkColumnarPlugin/vectorized/ArrowFieldNodeBuilder;");
  arrow_field_node_builder_constructor =
      GetMethodID(env, arrow_field_node_builder_class, "<init>", "(II)V");

  arrowbuf_builder_class = CreateGlobalClassReference(
      env, "Lcom/intel/sparkColumnarPlugin/vectorized/ArrowBufBuilder;");
  arrowbuf_builder_constructor =
      GetMethodID(env, arrowbuf_builder_class, "<init>", "(JJIJ)V");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  env->DeleteGlobalRef(io_exception_class);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);

  env->DeleteGlobalRef(arrow_field_node_builder_class);
  env->DeleteGlobalRef(arrowbuf_builder_class);
  env->DeleteGlobalRef(arrow_record_batch_builder_class);

  buffer_holder_.Clear();
}

JNIEXPORT jlong JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_ExpressionEvaluatorJniWrapper_nativeBuild(
    JNIEnv* env, jobject obj, jbyteArray schema_arr, jbyteArray exprs_arr,
    jbyteArray res_schema_arr, jboolean return_when_finish = false) {
  arrow::Status status;

  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  gandiva::ExpressionVector expr_vector;
  gandiva::FieldVector ret_types;
  msg = MakeExprVector(env, exprs_arr, &expr_vector, &ret_types);
  if (!msg.ok()) {
    std::string error_message =
        "failed to parse expressions protobuf, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  if (res_schema_arr != nullptr) {
    std::shared_ptr<arrow::Schema> resSchema;
    msg = MakeSchema(env, res_schema_arr, &resSchema);
    if (!msg.ok()) {
      std::string error_message = "failed to readSchema, err msg is " + msg.message();
      env->ThrowNew(io_exception_class, error_message.c_str());
    }
    ret_types = resSchema->fields();
  }

  for (auto expr : expr_vector) {
    std::cout << expr->ToString() << std::endl;
  }

  std::shared_ptr<CodeGenerator> handler;
  msg = sparkcolumnarplugin::codegen::CreateCodeGenerator(schema, expr_vector, ret_types,
                                                          &handler, return_when_finish);
  if (!msg.ok()) {
    std::string error_message =
        "nativeBuild: failed to create CodeGenerator, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  return handler_holder_.Insert(std::shared_ptr<CodeGenerator>(handler));
}

JNIEXPORT jlong JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_ExpressionEvaluatorJniWrapper_nativeBuildWithFinish(
    JNIEnv* env, jobject obj, jbyteArray schema_arr, jbyteArray exprs_arr,
    jbyteArray finish_exprs_arr) {
  arrow::Status status;

  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  gandiva::ExpressionVector expr_vector;
  gandiva::FieldVector ret_types;
  msg = MakeExprVector(env, exprs_arr, &expr_vector, &ret_types);
  if (!msg.ok()) {
    std::string error_message =
        "failed to parse expressions protobuf, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  gandiva::ExpressionVector finish_expr_vector;
  gandiva::FieldVector finish_ret_types;
  msg = MakeExprVector(env, finish_exprs_arr, &finish_expr_vector, &finish_ret_types);
  if (!msg.ok()) {
    std::string error_message =
        "failed to parse expressions protobuf, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<CodeGenerator> handler;
  msg = sparkcolumnarplugin::codegen::CreateCodeGenerator(
      schema, expr_vector, ret_types, &handler, true, finish_expr_vector);
  if (!msg.ok()) {
    std::string error_message =
        "nativeBuild: failed to create CodeGenerator, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  return handler_holder_.Insert(std::shared_ptr<CodeGenerator>(handler));
}

JNIEXPORT void JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_ExpressionEvaluatorJniWrapper_nativeSetReturnFields(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr) {
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  msg = handler->SetResSchema(schema);
  if (!msg.ok()) {
    std::string error_message =
        "failed to set result schema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
}

JNIEXPORT void JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_ExpressionEvaluatorJniWrapper_nativeClose(
    JNIEnv* env, jobject obj, jlong id) {
  handler_holder_.Erase(id);
}

JNIEXPORT jobject JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluate(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in);

  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  status = handler->evaluate(in, &out);

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::Schema> res_schema;
  status = handler->getResSchema(&res_schema);
  jobjectArray record_batch_builder_array =
      env->NewObjectArray(out.size(), arrow_record_batch_builder_class, nullptr);
  int i = 0;
  for (auto record_batch : out) {
    jobject record_batch_builder = MakeRecordBatchBuilder(env, res_schema, record_batch);
    env->SetObjectArrayElement(record_batch_builder_array, i++, record_batch_builder);
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return record_batch_builder_array;
}

JNIEXPORT jobject JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_ExpressionEvaluatorJniWrapper_nativeEvaluateWithSelection(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jint selection_vector_count, jlong selection_vector_buf_addr,
    jlong selection_vector_buf_size) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in);

  // Make Array From SelectionVector
  auto selection_vector_buf = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(selection_vector_buf_addr), selection_vector_buf_size);

  auto selection_arraydata = arrow::ArrayData::Make(
      arrow::uint16(), selection_vector_count, {NULLPTR, selection_vector_buf});
  auto selection_array = arrow::MakeArray(selection_arraydata);

  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  status = handler->evaluate(selection_array, in, &out);

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::Schema> res_schema;
  status = handler->getResSchema(&res_schema);
  jobjectArray record_batch_builder_array =
      env->NewObjectArray(out.size(), arrow_record_batch_builder_class, nullptr);
  int i = 0;
  for (auto record_batch : out) {
    jobject record_batch_builder = MakeRecordBatchBuilder(env, res_schema, record_batch);
    env->SetObjectArrayElement(record_batch_builder_array, i++, record_batch_builder);
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return record_batch_builder_array;
}

JNIEXPORT void JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMember(
    JNIEnv* env, jobject obj, jlong id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<arrow::Schema> schema;
  status = handler->getSchema(&schema);

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeEvaluate: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> in;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in);

  status = handler->SetMember(in);

  if (!status.ok()) {
    std::string error_message =
        "nativeEvaluate: evaluate failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
}

JNIEXPORT jobject JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_ExpressionEvaluatorJniWrapper_nativeFinish(
    JNIEnv* env, jobject obj, jlong id) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  status = handler->finish(&out);

  if (!status.ok()) {
    std::string error_message =
        "nativeFinish: finish failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  std::shared_ptr<arrow::Schema> schema;
  status = handler->getResSchema(&schema);

  jobjectArray record_batch_builder_array =
      env->NewObjectArray(out.size(), arrow_record_batch_builder_class, nullptr);
  int i = 0;
  for (auto record_batch : out) {
    jobject record_batch_builder = MakeRecordBatchBuilder(env, schema, record_batch);
    env->SetObjectArrayElement(record_batch_builder_array, i++, record_batch_builder);
  }

  return record_batch_builder_array;
}

JNIEXPORT jlong JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_ExpressionEvaluatorJniWrapper_nativeFinishByIterator(
    JNIEnv* env, jobject obj, jlong id) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> out;
  status = handler->finish(&out);
  if (!status.ok()) {
    std::string error_message =
        "nativeFinishForIterator: finish failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  return batch_iterator_holder_.Insert(
      std::shared_ptr<ResultIterator<arrow::RecordBatch>>(out));
}

JNIEXPORT void JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_ExpressionEvaluatorJniWrapper_nativeSetDependency(
    JNIEnv* env, jobject obj, jlong id, jlong iter_id, int index) {
  arrow::Status status;
  std::shared_ptr<CodeGenerator> handler = GetCodeGenerator(env, id);
  auto iter = GetBatchIterator(env, iter_id);
  status = handler->SetDependency(iter, index);
  if (!status.ok()) {
    std::string error_message =
        "nativeSetDependency: finish failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
}

JNIEXPORT jobject JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_BatchIterator_nativeNext(JNIEnv* env,
                                                                       jobject obj,
                                                                       jlong id) {
  arrow::Status status;
  auto iter = GetBatchIterator(env, id);
  std::shared_ptr<arrow::RecordBatch> out;
  if (!iter->HasNext()) return nullptr;
  status = iter->Next(&out);
  if (!status.ok()) {
    std::string error_message =
        "nativeNext: get Next() failed with error msg " + status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  return MakeRecordBatchBuilder(env, out->schema(), out);
}

JNIEXPORT jobject JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_BatchIterator_nativeProcess(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator(env, id);
  std::shared_ptr<arrow::RecordBatch> out;
  status = iter->Process(in, &out);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcess: ResultIterator process next failed with error msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return MakeRecordBatchBuilder(env, out->schema(), out);
}

JNIEXPORT jobject JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_BatchIterator_nativeProcessWithSelection(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes, jint selection_vector_count,
    jlong selection_vector_buf_addr, jlong selection_vector_buf_size) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator(env, id);
  // Make Array From SelectionVector
  auto selection_vector_buf = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(selection_vector_buf_addr), selection_vector_buf_size);

  auto selection_arraydata = arrow::ArrayData::Make(
      arrow::uint16(), selection_vector_count, {NULLPTR, selection_vector_buf});
  auto selection_array = arrow::MakeArray(selection_arraydata);

  std::shared_ptr<arrow::RecordBatch> out;
  status = iter->Process(in, &out, selection_array);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcess: ResultIterator process next failed with error msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  return MakeRecordBatchBuilder(env, out->schema(), out);
}

JNIEXPORT void JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_BatchIterator_nativeProcessAndCacheOne(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator(env, id);
  status = iter->ProcessAndCacheOne(in);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcessAndCache: ResultIterator process next failed with error msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
}

JNIEXPORT void JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_BatchIterator_nativeProcessAndCacheOneWithSelection(
    JNIEnv* env, jobject obj, jlong id, jbyteArray schema_arr, jint num_rows,
    jlongArray buf_addrs, jlongArray buf_sizes, jint selection_vector_count,
    jlong selection_vector_buf_addr, jlong selection_vector_buf_size) {
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status msg = MakeSchema(env, schema_arr, &schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    std::string error_message =
        "nativeProcess: mismatch in arraylen of buf_addrs and buf_sizes";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  std::shared_ptr<arrow::RecordBatch> batch;
  status =
      MakeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  std::vector<std::shared_ptr<arrow::Array>> in;
  for (int i = 0; i < batch->num_columns(); i++) {
    in.push_back(batch->column(i));
  }

  auto iter = GetBatchIterator(env, id);
  // Make Array From SelectionVector
  auto selection_vector_buf = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(selection_vector_buf_addr), selection_vector_buf_size);

  auto selection_arraydata = arrow::ArrayData::Make(
      arrow::uint16(), selection_vector_count, {NULLPTR, selection_vector_buf});
  auto selection_array = arrow::MakeArray(selection_arraydata);
  status = iter->ProcessAndCacheOne(in, selection_array);

  if (!status.ok()) {
    std::string error_message =
        "nativeProcessAndCache: ResultIterator process next failed with error msg " +
        status.ToString();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
}

JNIEXPORT void JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_BatchIterator_nativeClose(JNIEnv* env,
                                                                        jobject this_obj,
                                                                        jlong id) {
  batch_iterator_holder_.Erase(id);
}

JNIEXPORT void JNICALL
Java_com_intel_sparkColumnarPlugin_vectorized_AdaptorReferenceManager_nativeRelease(
    JNIEnv* env, jobject this_obj, jlong id) {
  // auto buffer_holder_size = buffer_holder_.Size();
  // std::cout << "release buffer, current size is " << buffer_holder_size << std::endl;
  buffer_holder_.Erase(id);
}

#ifdef __cplusplus
}
#endif
