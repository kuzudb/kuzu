#include "binder/binder.h"
#include "common/exception/binder.h"
#include "common/exception/connection.h"
#include "common/string_utils.h"
#include "expression_evaluator/expression_evaluator_utils.h"
#include "function/llm_functions.h"
#include "function/scalar_function.h"
#include "httplib.h"
#include "json.hpp"
#include "providers/amazon-bedrock.h"
#include "providers/google-gemini.h"
#include "providers/google-vertex.h"
#include "providers/ollama.h"
#include "providers/open-ai.h"
#include "providers/provider.h"
#include "providers/voyage-ai.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;
using namespace kuzu::catalog;
using namespace kuzu::processor;

namespace kuzu {
namespace llm_extension {

static EmbeddingProvider& getInstance(const std::string& provider) {
    static const std::unordered_map<std::string, std::function<EmbeddingProvider&()>>
        providerInstanceMap = {{"open-ai", &OpenAIEmbedding::getInstance},
            {"voyage-ai", &VoyageAIEmbedding::getInstance},
            {"google-vertex", &GoogleVertexEmbedding::getInstance},
            {"google-gemini", &GoogleGeminiEmbedding::getInstance},
            {"amazon-bedrock", &BedrockEmbedding::getInstance},
            {"ollama", &OllamaEmbedding::getInstance}};

    auto providerInstanceIter = providerInstanceMap.find(provider);
    if (providerInstanceIter == providerInstanceMap.end()) {
        throw BinderException("Provider not found: " + provider + "\n" +
                              std::string(EmbeddingProvider::referenceKuzuDocs));
    }
    return providerInstanceIter->second();
}

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& /*parameterSelVectors*/,
    common::ValueVector& result, common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    auto& provider =
        getInstance(StringUtils::getLower(parameters[1]->getValue<ku_string_t>(0).getAsString()));
    auto model = StringUtils::getLower(parameters[2]->getValue<ku_string_t>(0).getAsString());
    httplib::Client client(provider.getClient());
    client.set_connection_timeout(30);
    client.set_read_timeout(30);
    client.set_write_timeout(30);
    std::string path = provider.getPath(model);

    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {
        auto text = parameters[0]->getValue<ku_string_t>(selectedPos).getAsString();
        nlohmann::json payload = provider.getPayload(model, text);
        httplib::Headers headers = provider.getHeaders(payload);
        auto res = client.Post(path, headers, payload.dump(), "application/json");
        if (!res) {
            throw ConnectionException("Request failed: Could not connect to server <" +
                                      provider.getClient() + "> \n" +
                                      std::string(EmbeddingProvider::referenceKuzuDocs));
        } else if (res->status != 200) {
            throw ConnectionException("Request failed with status " + std::to_string(res->status) +
                                      "\n Body: " + res->body + "\n" +
                                      std::string(EmbeddingProvider::referenceKuzuDocs));
        }
        auto embeddingVec = provider.parseResponse(res);
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

static uint64_t parseDimensions(std::shared_ptr<Expression> dimensionsExpr,
    main::ClientContext* context) {
    Binder binder{context};
    auto dimensions =
        evaluator::ExpressionEvaluatorUtils::evaluateConstantExpression(dimensionsExpr, context)
            .getValue<int64_t>();
    if (dimensions <= 0) {
        throw(BinderException("Failed to parse dimensions: " + dimensionsExpr->toString() + '\n' +
                              std::string(EmbeddingProvider::referenceKuzuDocs)));
    }
    return dimensions;
}

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::optional<uint64_t> dimensions = std::nullopt;
    std::optional<std::string> region = std::nullopt;
    auto& provider = getInstance(StringUtils::getLower(input.arguments[1]->toString()));
    if (input.arguments.size() == 5) {
        dimensions = parseDimensions(input.arguments[3], input.context);
        region = StringUtils::getLower(input.arguments[4]->toString());
    } else if (input.arguments.size() == 4) {
        if (input.arguments[3]->dataType == LogicalType(LogicalTypeID::STRING)) {
            region = StringUtils::getLower(input.arguments[3]->toString());
        } else {
            dimensions = parseDimensions(input.arguments[3], input.context);
        }
    }

    provider.configure(dimensions, region);
    const std::string model = StringUtils::getLower(input.arguments[2]->toString());
    if (dimensions.has_value()) {
        provider.checkModel(model);
        return FunctionBindData::getSimpleBindData(input.arguments,
            LogicalType::ARRAY(LogicalType(LogicalTypeID::FLOAT), dimensions.value()));
    }
    auto embeddingDimensions = provider.getEmbeddingDimensions(model);
    return FunctionBindData::getSimpleBindData(input.arguments,
        LogicalType::ARRAY(LogicalType(LogicalTypeID::FLOAT), embeddingDimensions));
}

function_set CreateEmbedding::getFunctionSet() {
    function_set functionSet;

    // Prompt, Provider, Model -> Vector Embedding
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING},
        LogicalTypeID::ARRAY, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));

    // Prompt, Provider, Model, Region -> Vector Embedding
    function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::ARRAY, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));

    // Prompt, Provider, Model, Dimensions -> Vector Embedding
    function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::ARRAY, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));

    // Prompt, Provider, Model, Dimensions, Region -> Vector Embedding
    function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::STRING},
        LogicalTypeID::ARRAY, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));

    return functionSet;
}

} // namespace llm_extension
} // namespace kuzu
