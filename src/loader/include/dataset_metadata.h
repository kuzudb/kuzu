#pragma once

#include <utility>
#include <vector>

#include "nlohmann/json.hpp"

using namespace std;

namespace graphflow {
namespace loader {

struct CSVSpecialChars {

    CSVSpecialChars() : escapeChar{'\\'}, tokenSeparator{','}, quoteChar{'"'} {};

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
    NodeFileDescription(string filePath, string labelName, string primaryKeyPropertyName,
        CSVSpecialChars& csvSpecialChars)
        : LabelFileDescription{move(filePath), move(labelName), csvSpecialChars},
          primaryKeyPropertyName{move(primaryKeyPropertyName)} {}

    string primaryKeyPropertyName;
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
