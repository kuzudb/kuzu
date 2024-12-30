#pragma once

#include <memory>

#include "common/vector/value_vector.h"
#include "main/client_context.h"
#include "yyjson.h"

namespace kuzu {
namespace json_extension {

class JsonMutWrapper;

class JsonWrapper {
    JsonWrapper() : ptr{nullptr} {}

public:
    explicit JsonWrapper(yyjson_doc* ptr, std::shared_ptr<char[]> buffer = nullptr)
        : ptr{ptr}, buffer{std::move(buffer)} {}
    ~JsonWrapper();
    JsonWrapper(JsonWrapper& other) = delete;
    JsonWrapper(JsonWrapper&& other) {
        ptr = other.ptr;
        other.ptr = nullptr;
        buffer = std::move(other.buffer);
    }

    yyjson_doc* ptr;
    std::shared_ptr<char[]> buffer;
};

class JsonMutWrapper {
public:
    JsonMutWrapper() : ptr{yyjson_mut_doc_new(nullptr)} {}
    explicit JsonMutWrapper(yyjson_mut_doc* ptr) : ptr{ptr} {}
    ~JsonMutWrapper();
    JsonMutWrapper(JsonMutWrapper& other) = delete;
    JsonMutWrapper(JsonMutWrapper&& other) {
        ptr = other.ptr;
        other.ptr = nullptr;
    }

    yyjson_mut_doc* ptr;
};

JsonWrapper jsonify(const common::ValueVector& vec, uint64_t pos);
yyjson_mut_val* jsonify(JsonMutWrapper& wrapper, const common::ValueVector& vec, uint64_t pos);
yyjson_mut_val* jsonifyAsString(JsonMutWrapper& wrapper, const common::ValueVector& vec,
    uint64_t pos);
// Converts an internal Kuzu Value into json

std::vector<JsonWrapper> jsonifyQueryResult(
    const std::vector<std::shared_ptr<common::ValueVector>>& columns,
    const std::vector<std::string>& names);
// Converts an entire query result into a sequence of json values
common::LogicalType jsonSchema(const JsonWrapper& wrapper, int64_t depth = -1,
    int64_t breadth = -1);
common::LogicalType jsonSchema(yyjson_val* val, int64_t depth, int64_t breadth);
// depth indicates at what nested depth to stop
// breadth indicates the limit of how many children the root nested type is sampled
// -1 means to scan the whole thing
// may return ANY

void readJsonToValueVector(yyjson_val* val, common::ValueVector& vec, uint64_t pos);

std::string jsonToString(const JsonWrapper& wrapper);
std::string jsonToString(const yyjson_val* val);
JsonWrapper stringToJson(const std::string& str);
JsonWrapper stringToJsonNoError(const std::string& str);
// format can be 'unstructured' or 'array'

JsonWrapper mergeJson(const JsonWrapper& A, const JsonWrapper& B);

std::string jsonExtractToString(const JsonWrapper& wrapper, uint64_t pos);
std::string jsonExtractToString(const JsonWrapper& wrapper, std::string path);

uint32_t jsonArraySize(const JsonWrapper& wrapper);

} // namespace json_extension
} // namespace kuzu
