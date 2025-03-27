#include "function/string/vector_string_functions.h"
#include "json.hpp"

using json = nlohmann::json;

namespace kuzu {
namespace function {

using namespace common;

// static constexpr uint32_t EMBEDDING_SIZE = 1536;
static constexpr uint32_t EMBEDDING_SIZE = 5;

static std::vector<float> generateEmbedding(const std::string& inputText, const std::string& apiKey,
        const std::string& model) {
    json requestBody = {{"input", inputText}, {"model", model}};
    std::string requestStr = requestBody.dump();

    std::string command = "curl -s https://api.openai.com/v1/embeddings "
                          "-H \"Content-Type: application/json\" "
                          "-H \"Authorization: Bearer " +
                          apiKey +
                          "\" "
                          "-d '" +
                          requestStr + "' 2>/dev/null";

    std::array<char, 8192> buffer;
    std::string result;
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }

    while (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
        result += buffer.data();
    }
    pclose(pipe);

    json response = json::parse(result);
    if (!response.contains("data") || response["data"].empty() ||
        !response["data"][0].contains("embedding")) {
        throw std::runtime_error("Invalid response from OpenAI API");
        }

    return response["data"][0]["embedding"].get<std::vector<float>>();
}

static std::vector<float> mockResult() {
    std::vector<float> result;
    for (auto i = 0u; i < EMBEDDING_SIZE; i++) {
        result.push_back(0.5);
    }
    return result;
}

void execFunc(const std::vector<std::shared_ptr<ValueVector>>& params,
    const std::vector<SelectionVector*>& paramSelVectors, ValueVector& result,
    SelectionVector* resultSelVector, void* ) {
    KU_ASSERT(params.size() == 1 && paramSelVectors.size() == 1);
    result.resetAuxiliaryBuffer();
    auto param = params[0];
    auto paramSelVector = paramSelVectors[0];
    for (auto i = 0u; i < paramSelVector->getSelSize(); ++i) {
        auto inputPos = paramSelVector->getSelectedPositions()[i];
        auto resultPos = resultSelVector->getSelectedPositions()[i];
        result.setNull(resultPos, param->isNull(inputPos));
        auto input = param->getValue<ku_string_t>(inputPos).getAsString();
        // auto embedding = generateEmbedding(
        //     input,
        //     "api-key",
        //     "text-embedding-3-small"
        // );
        auto embedding = mockResult();
        KU_ASSERT(embedding.size() == EMBEDDING_SIZE);
        auto listEntry = ListVector::addList(&result, EMBEDDING_SIZE);
        result.setValue(resultPos, listEntry);
        auto listData = ListVector::getDataVector(&result);
        for (auto j = 0u; j < EMBEDDING_SIZE; ++j) {
            listData->setValue<float>(listEntry.offset + j, embedding[j]);
        }
    }
}

std::unique_ptr<FunctionBindData> embeddingBindFunc(const ScalarBindFuncInput&) {
    auto resultType = LogicalType::ARRAY(LogicalType::FLOAT(), EMBEDDING_SIZE);
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(LogicalType::STRING());
    return std::make_unique<FunctionBindData>(std::move(paramTypes), std::move(resultType));
}

function_set GetEmbeddingFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::ANY);
    function->bindFunc = embeddingBindFunc;
    function->execFunc = execFunc;
    functionSet.emplace_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
