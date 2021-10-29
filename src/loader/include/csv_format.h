#pragma once

#include <vector>

#include "nlohmann/json.hpp"

using namespace std;

namespace graphflow {
namespace loader {

class CSVFormat {

public:
    vector<char> nodeFileEscapeChars;
    vector<char> nodeFileTokenSeparators;
    vector<char> nodeFileQuoteChars;
    vector<char> relFileEscapeChars;
    vector<char> relFileTokenSeparators;
    vector<char> relFileQuoteChars;

    void parseCsvFormat(unique_ptr<nlohmann::json>& parsedJson);
    void updateNodeFormat(nlohmann::json& parsedNodeFileDescription, uint32_t& nodeIndex);
    void updateRelFormat(nlohmann::json& parsedRelFileDescription, uint32_t& relIndex);

private:
    void getCSVConfig(nlohmann::json& parsedFileDescription, string key, char& val);
};

} // namespace loader
} // namespace graphflow
