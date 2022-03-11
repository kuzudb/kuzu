#pragma once

#include <utility>
#include <vector>

#include "nlohmann/json.hpp"

#include "src/common/include/configs.h"
#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/types/include/types.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

struct LabelFileDescription {
protected:
    LabelFileDescription(string filePath, string labelName, const CSVReaderConfig& csvReaderConfig)
        : filePath{move(filePath)}, labelName{move(labelName)}, csvReaderConfig{csvReaderConfig} {}

public:
    string filePath;
    string labelName;
    CSVReaderConfig csvReaderConfig;
};

struct NodeFileDescription : public LabelFileDescription {
    NodeFileDescription(
        string filePath, string labelName, DataType IDType, CSVReaderConfig& csvSpecialChars)
        : LabelFileDescription{move(filePath), move(labelName), csvSpecialChars}, IDType{IDType} {}

    DataType IDType;
};

struct RelFileDescription : public LabelFileDescription {
    RelFileDescription(string filePath, string labelName, string relMultiplicity,
        vector<string> srcNodeLabelNames, vector<string> dstNodeLabelNames,
        CSVReaderConfig& csvSpecialChars)
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
    static void getSpecialChars(nlohmann::json& parsedJson, CSVReaderConfig& config);

    static void getSpecialChar(nlohmann::json& parsedJson, const string& key, char& val);

public:
    vector<NodeFileDescription> nodeFileDescriptions{};
    vector<RelFileDescription> relFileDescriptions{};
};

} // namespace loader
} // namespace graphflow
