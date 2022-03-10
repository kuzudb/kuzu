#pragma once

#include <utility>
#include <vector>

#include "nlohmann/json.hpp"

#include "src/common/include/configs.h"
#include "src/common/types/include/types.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

struct CSVSpecialChars {

    CSVSpecialChars()
        : escapeChar{LoaderConfig::DEFAULT_ESCAPE_CHAR},
          tokenSeparator{LoaderConfig::DEFAULT_TOKEN_SEPARATOR},
          quoteChar{LoaderConfig::DEFAULT_QUOTE_CHAR} {};

    char escapeChar;
    char tokenSeparator;
    char quoteChar;
};

struct LabelFileDescription {
protected:
    LabelFileDescription(string filePath, string labelName, CSVSpecialChars& csvSpecialChars)
        : filePath{move(filePath)}, labelName{move(labelName)}, csvSpecialChars{csvSpecialChars} {}

public:
    string filePath;
    string labelName;
    CSVSpecialChars csvSpecialChars;
};

struct NodeFileDescription : public LabelFileDescription {
    NodeFileDescription(
        string filePath, string labelName, DataType IDType, CSVSpecialChars& csvSpecialChars)
        : LabelFileDescription{move(filePath), move(labelName), csvSpecialChars}, IDType{IDType} {}

    DataType IDType;
};

struct RelFileDescription : public LabelFileDescription {
    RelFileDescription(string filePath, string labelName, string relMultiplicity,
        vector<string> srcNodeLabelNames, vector<string> dstNodeLabelNames,
        CSVSpecialChars& csvSpecialChars)
        : LabelFileDescription{move(filePath), move(labelName), csvSpecialChars},
          relMultiplicity{move(relMultiplicity)}, srcNodeLabelNames{move(srcNodeLabelNames)},
          dstNodeLabelNames{std::move(dstNodeLabelNames)} {}

    string relMultiplicity;
    vector<string> srcNodeLabelNames;
    vector<string> dstNodeLabelNames;
};

class DatasetMetadata {

public:
    void parseJson(unique_ptr<nlohmann::json>& parsedJson, const string& inputDirectory);

private:
    static void getSpecialChars(nlohmann::json& parsedJson, CSVSpecialChars& specialChars);

    static void getSpecialChar(nlohmann::json& parsedJson, const string& key, char& val);

public:
    vector<NodeFileDescription> nodeFileDescriptions{};
    vector<RelFileDescription> relFileDescriptions{};
};

} // namespace loader
} // namespace graphflow
