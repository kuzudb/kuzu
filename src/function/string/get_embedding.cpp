#include <array>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "function/string/vector_string_functions.h"
#include "third_party/nlohmann_json/json.hpp"

using json = nlohmann::json;

namespace kuzu {
namespace function {

using namespace kuzu::common;

struct GetEmbedding {
public:
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


    static void operation(common::ku_string_t& str, std::vector<float>& result){
        auto embedding = generateEmbedding(
            str.getAsString(), 
            "api-key", 
            "text-embedding-3-small"
        );
        result.clear();
        for (size_t i = 0; i < embedding.size(); ++i) {
            result.push_back(embedding[i]);
        }
    }
};

function_set GetEmbeddingFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::INT64,
        ScalarFunction::UnaryStringExecFunction<ku_string_t, , GetEmbedding>));
    return functionSet;
}

} // namespace function
} // namespace kuzu
