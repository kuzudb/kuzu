#include "common/exception/connection.h"
#include "common/types/types.h"
#include "common/types/value/nested.h"
#include "function/function.h"
#include "function/llm_functions.h"
#include "function/scalar_function.h"
#include "httplib.h"
#include "json.hpp"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;
using namespace kuzu::planner;
using namespace kuzu::catalog;
using namespace kuzu::processor;

namespace kuzu {
namespace llm_extension {

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& /*parameterSelVectors*/,
    common::ValueVector& result, common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    // This iteration only supports using Ollama with nomic-embed-text.
    // The user must install and have nomic-embed-text running at
    // http://localhost::11434.
    assert(parameters.size() == 1);
    std::string text = parameters[0]->getValue<ku_string_t>(0).getAsString();
    httplib::Client client("http://localhost:11434");
    nlohmann::json payload = {{"model", "nomic-embed-text"}, {"prompt", text}};
    httplib::Headers headers = {{"Content-Type", "application/json"}};
    auto res = client.Post("/api/embeddings", headers, payload.dump(), "application/json");
    if (!res) {
        throw ConnectionException("Request failed: No response (server not reachable?)\n");
    } else if (res->status != 200) {
        throw ConnectionException("Request failed with status " + std::to_string(res->status) +
                                  "\n Body: " + res->body + "\n");
    }

    std::vector<float> embeddingVec =
        nlohmann::json::parse(res->body)["embedding"].get<std::vector<float>>();
    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {
        auto pos = (*resultSelVector)[selectedPos];
        auto resultEntry = ListVector::addList(&result, embeddingVec.size());
        result.setValue(pos, resultEntry);
        auto resultDataVector = ListVector::getDataVector(&result);
        auto resultPos = resultEntry.offset;
        for (auto i = 0u; i < embeddingVec.size(); i++) {
            resultDataVector->copyFromValue(resultPos++, Value(embeddingVec[i]));
        }
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::vector<LogicalType> types;
    types.push_back(input.arguments[0]->getDataType().copy());
    assert(types.front() == LogicalType::STRING());
    return std::make_unique<FunctionBindData>(std::move(types),
        LogicalType::LIST(LogicalType(LogicalTypeID::FLOAT)));
}

function_set CreateEmbedding::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::LIST, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace llm_extension
} // namespace kuzu
