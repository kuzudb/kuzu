#pragma once

#include <memory>

#include "common/copy_constructors.h"
#include "common/vector/value_vector.h"
#include "yyjson.h"

namespace kuzu {
namespace json_extension {

class JsonMutWrapper;

class JsonWrapper {
    JsonWrapper() : ptr{nullptr} {}

public:
    explicit JsonWrapper(yyjson_doc* ptr) : ptr{ptr} {}
    ~JsonWrapper();
    JsonWrapper(JsonWrapper& other) = delete;
    JsonWrapper(JsonWrapper&& other) { std::swap(ptr, other.ptr); }

    yyjson_doc* ptr;
};

class JsonMutWrapper {
public:
    JsonMutWrapper() : ptr{yyjson_mut_doc_new(nullptr)} {}
    explicit JsonMutWrapper(yyjson_mut_doc* ptr) : ptr{ptr} {}
    ~JsonMutWrapper();
    JsonMutWrapper(JsonMutWrapper& other) = delete;
    JsonMutWrapper(JsonMutWrapper&& other) { std::swap(ptr, other.ptr); }

    yyjson_mut_doc* ptr;
};

JsonWrapper jsonify(const common::ValueVector& vec, uint64_t pos);
// Converts an internal Kuzu Value into json
common::LogicalType jsonSchema(const JsonWrapper& wrapper);
void readJsonToValueVector(yyjson_val* val, common::ValueVector& vec, uint64_t pos);

std::string jsonToString(const JsonWrapper& wrapper);
std::string jsonToString(const yyjson_val* val);
JsonWrapper stringToJson(const std::string& str);
JsonWrapper stringToJsonNoError(const std::string& str);
JsonWrapper fileToJson(const std::string& path);

JsonWrapper mergeJson(const JsonWrapper& A, const JsonWrapper& B);

std::string jsonExtractToString(const JsonWrapper& wrapper, uint64_t pos);
std::string jsonExtractToString(const JsonWrapper& wrapper, std::string path);

uint32_t jsonArraySize(const JsonWrapper& wrapper);

bool jsonContains(const JsonWrapper& haystack, const JsonWrapper& needle);

std::vector<std::string> jsonGetKeys(const JsonWrapper& wrapper);

} // namespace json_extension
} // namespace kuzu
