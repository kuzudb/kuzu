#pragma once

#include <chrono>
#include <ctime>
#include <iostream>

#include "main/kuzu.h"
#include <napi.h>

using namespace kuzu::common;

class Util {
public:
    static Napi::Value ConvertToNapiObject(const Value& value, Napi::Env env);
    static std::unordered_map<std::string, std::shared_ptr<Value>> TransformParametersForExec(
        Napi::Array params, std::unordered_map<std::string, std::shared_ptr<Value>>& parameterMap);

private:
    static Napi::Object ConvertPropertiesToNapiObject(
        const std::vector<std::pair<std::string, std::unique_ptr<Value>>>& properties,
        Napi::Env env);
    static Napi::Object ConvertNodeIdToNapiObject(const nodeID_t& nodeId, Napi::Env env);
    static Value TransformNapiValue(
        Napi::Value napiValue, LogicalType expectedDataType, const std::string& key);
};
