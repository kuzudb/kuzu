#pragma once

#include "common/types/value/value.h"
#include <napi.h>

using namespace kuzu::common;

class Util {
public:
    static Napi::Value ConvertToNapiObject(const Value& value, Napi::Env env);
    static std::unordered_map<std::string, std::unique_ptr<Value>> TransformParametersForExec(
        Napi::Array params,
        const std::unordered_map<std::string, std::shared_ptr<Value>>& parameterMap);

private:
    static Napi::Object ConvertNodeIdToNapiObject(const nodeID_t& nodeId, Napi::Env env);
    static Value TransformNapiValue(
        Napi::Value napiValue, LogicalType* expectedDataType, const std::string& key);
};
