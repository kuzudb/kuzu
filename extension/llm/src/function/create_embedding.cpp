#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/exception/connection.h"
#include "common/string_utils.h"
#include "function/built_in_function_utils.h"
#include "function/llm_functions.h"
#include "function/scalar_function.h"
#include "httplib.h"
#include "json.hpp"
#include "main/client_context.h"
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

class EmbeddingProviderFactory {
public:
    static std::shared_ptr<EmbeddingProvider> getProvider(std::string& provider) {
        StringUtils::toLower(provider);
        static const std::unordered_map<std::string,
            std::function<std::shared_ptr<EmbeddingProvider>()>>
            providerInstanceMap = {{"openai", &OpenAIEmbedding::getInstance},
                // Accepted for backward compatibility.
                {"open-ai", &OpenAIEmbedding::getInstance},
                {"voyageai", &VoyageAIEmbedding::getInstance},
                // Accepted for backward compatibility.
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
    };
};

struct CreateEmbeddingBindData : public FunctionBindData {
    std::shared_ptr<EmbeddingProvider> provider;
    std::string model;

    CreateEmbeddingBindData(std::vector<common::LogicalType> paramTypes,
        common::LogicalType resultType, std::shared_ptr<EmbeddingProvider> provider,
        std::string model)
        : FunctionBindData{std::move(paramTypes), std::move(resultType)},
          provider{std::move(provider)}, model{std::move(model)} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<CreateEmbeddingBindData>(common::LogicalType::copy(paramTypes),
            resultType.copy(), provider, model);
    }
};

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& /*parameterSelVectors*/,
    common::ValueVector& result, common::SelectionVector* resultSelVector, void* dataPtr) {
    auto& bindData = ((FunctionBindData*)(dataPtr))->cast<CreateEmbeddingBindData>();
    auto& provider = bindData.provider;
    auto model = bindData.model;
    httplib::Client client(provider->getClient());
    client.set_connection_timeout(30);
    client.set_read_timeout(30);
    client.set_write_timeout(30);
    std::string path = provider->getPath(model);

    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {
        auto text = parameters[0]->getValue<ku_string_t>(selectedPos).getAsString();
        nlohmann::json payload = provider->getPayload(model, text);
        httplib::Headers headers = provider->getHeaders(model, payload);
        auto res = client.Post(path, headers, payload.dump(), "application/json");
        if (!res) {
            throw ConnectionException("Request failed: Could not connect to server <" +
                                      provider->getClient() + "> \n" +
                                      std::string(EmbeddingProvider::referenceKuzuDocs));
        } else if (res->status != 200) {
            throw ConnectionException("Request failed with status " + std::to_string(res->status) +
                                      "\n Body: " + res->body + "\n" +
                                      std::string(EmbeddingProvider::referenceKuzuDocs));
        }
        auto embeddingVec = provider->parseResponse(res);
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

void validateValAsPositive(int64_t val) {
    if (val <= 0) {
        throw(BinderException(
            common::stringFormat("Dimensions should be greater than 0. Got: {}.\n{}", val,
                std::string(EmbeddingProvider::referenceKuzuDocs))));
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::optional<uint64_t> numConfig = std::nullopt;
    std::optional<std::string> stringConfig = std::nullopt;
    auto clientContext = input.context;
    if (input.arguments[0]->getDataType() != LogicalType::STRING()) {
        static const auto fcnSet = CreateEmbedding::getFunctionSet();
        std::string supportedArgs;
        for (const auto& signature : fcnSet) {
            supportedArgs += signature->signatureToString() + '\n';
        }
        throw(BinderException(
            BuiltInFunctionsUtils::getFunctionMatchFailureMsg(std::string(CreateEmbedding::name),
                ExpressionUtil::getDataTypes(input.arguments), supportedArgs)));
    }
    auto providerNameExpr = ExpressionUtil::applyImplicitCastingIfNecessary(input.context,
        input.arguments[1], LogicalType::STRING());
    auto providerName = ExpressionUtil::evaluateLiteral<std::string>(clientContext,
        providerNameExpr, LogicalType::STRING());
    auto provider = EmbeddingProviderFactory::getProvider(providerName);
    auto modelNameExpr = ExpressionUtil::applyImplicitCastingIfNecessary(input.context,
        input.arguments[2], LogicalType::STRING());
    auto modelName = ExpressionUtil::evaluateLiteral<std::string>(clientContext, modelNameExpr,
        LogicalType::STRING());
    if (input.arguments.size() == 5) {
        auto numConfigExpr = ExpressionUtil::applyImplicitCastingIfNecessary(input.context,
            input.arguments[3], LogicalType::INT64());
        numConfig = ExpressionUtil::evaluateLiteral<int64_t>(clientContext, numConfigExpr,
            LogicalType::INT64(), validateValAsPositive);
        auto stringConfigExpr = ExpressionUtil::applyImplicitCastingIfNecessary(input.context,
            input.arguments[4], LogicalType::STRING());
        stringConfig = ExpressionUtil::evaluateLiteral<std::string>(clientContext, stringConfigExpr,
            LogicalType::STRING());
    } else if (input.arguments.size() == 4) {
        if (input.arguments[3]->dataType == LogicalType(LogicalTypeID::STRING)) {
            auto stringConfigExpr = ExpressionUtil::applyImplicitCastingIfNecessary(input.context,
                input.arguments[3], LogicalType::STRING());
            stringConfig = ExpressionUtil::evaluateLiteral<std::string>(clientContext,
                stringConfigExpr, LogicalType::STRING());
        } else {
            auto numConfigExpr = ExpressionUtil::applyImplicitCastingIfNecessary(input.context,
                input.arguments[3], LogicalType::INT64());
            numConfig = ExpressionUtil::evaluateLiteral<int64_t>(clientContext, numConfigExpr,
                LogicalType::INT64(), validateValAsPositive);
        }
    }
    if (stringConfig.has_value()) {
        StringUtils::toLower(stringConfig.value());
    }

    try {
        provider->configure(numConfig, stringConfig);
    } catch (const std::string& supportedInputs) {
        throw(BinderException(
            StringUtils::rtrimNewlines(BuiltInFunctionsUtils::getFunctionMatchFailureMsg(
                std::string(CreateEmbedding::name) + " for " + providerName,
                ExpressionUtil::getDataTypes(input.arguments), supportedInputs)) +
            '\n' + EmbeddingProvider::referenceKuzuDocs));
    }
    return std::make_unique<CreateEmbeddingBindData>(ExpressionUtil::getDataTypes(input.arguments),
        LogicalType::LIST(LogicalType(LogicalTypeID::FLOAT)), std::move(provider),
        std::move(modelName));
}

function_set CreateEmbedding::getFunctionSet() {
    function_set functionSet;

    // Prompt, Provider, Model -> Vector Embedding
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING},
        LogicalTypeID::LIST, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));

    // Prompt, Provider, Model, Region/Endpoint -> Vector Embedding
    function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::LIST, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));

    // Prompt, Provider, Model, Dimensions -> Vector Embedding
    function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::LIST, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));

    // Prompt, Provider, Model, Dimensions, Region/Endpoint -> Vector Embedding
    function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::STRING},
        LogicalTypeID::LIST, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));

    return functionSet;
}

} // namespace llm_extension
} // namespace kuzu
