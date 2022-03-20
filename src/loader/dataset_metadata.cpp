#include "src/loader/include/dataset_metadata.h"

#include "src/common/include/exception.h"
#include "src/common/include/file_utils.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

void DatasetMetadata::parseJson(
    unique_ptr<nlohmann::json>& parsedJson, const string& inputDirectory) {
    CSVReaderConfig globalConfig;
    getSpecialChars(*parsedJson, globalConfig);
    auto parsedNodeFileDescriptions = parsedJson->at("nodeFileDescriptions");
    for (auto parsedNodeFileDescription : parsedNodeFileDescriptions) {
        auto labelSpecificConfig{globalConfig};
        getSpecialChars(parsedNodeFileDescription, labelSpecificConfig);
        auto filename = parsedNodeFileDescription.at("filename").get<string>();
        DataType IDType;
        if (parsedNodeFileDescription.contains("IDType")) {
            auto IDTypeString = parsedNodeFileDescription.at("IDType").get<string>();
            IDType = Types::getDataType(IDTypeString);
            if (IDType != STRING && IDType != INT64) {
                throw invalid_argument("Invalid ID DataType `" + IDTypeString +
                                       "`. Allowed type only: STRING and INT64.");
            }
        } else {
            IDType = STRING;
        }
        nodeFileDescriptions.emplace_back(FileUtils::joinPath(inputDirectory, filename),
            parsedNodeFileDescription.at("label").get<string>(), IDType, labelSpecificConfig);
    }
    auto parsedRelFileDescriptions = parsedJson->at("relFileDescriptions");
    for (auto parsedRelFileDescription : parsedRelFileDescriptions) {
        auto labelSpecificSpecialChars{globalConfig};
        getSpecialChars(parsedRelFileDescription, labelSpecificSpecialChars);
        vector<string> srcNodeLabelNames, dstNodeLabelNames;
        for (auto& parsedSrcNodeLabel : parsedRelFileDescription.at("srcNodeLabels")) {
            srcNodeLabelNames.push_back(parsedSrcNodeLabel.get<string>());
        }
        for (auto& parsedDstNodeLabel : parsedRelFileDescription.at("dstNodeLabels")) {
            dstNodeLabelNames.push_back(parsedDstNodeLabel.get<string>());
        }
        auto filename = parsedRelFileDescription.at("filename").get<string>();
        relFileDescriptions.emplace_back(FileUtils::joinPath(inputDirectory, filename),
            parsedRelFileDescription.at("label").get<string>(),
            parsedRelFileDescription.at("multiplicity").get<string>(), srcNodeLabelNames,
            dstNodeLabelNames, labelSpecificSpecialChars);
    }
}

void DatasetMetadata::getSpecialChars(
    nlohmann::json& parsedJson, CSVReaderConfig& csvReaderConfig) {
    getSpecialChar(parsedJson, "tokenSeparator", csvReaderConfig.tokenSeparator);
    getSpecialChar(parsedJson, "quoteCharacter", csvReaderConfig.quoteChar);
    getSpecialChar(parsedJson, "escapeCharacter", csvReaderConfig.escapeChar);
    getSpecialChar(parsedJson, "listBeginCharacter", csvReaderConfig.listBeginChar);
    getSpecialChar(parsedJson, "listEndCharacter", csvReaderConfig.listEndChar);
}

void DatasetMetadata::getSpecialChar(nlohmann::json& parsedJson, const string& key, char& val) {
    if (parsedJson.contains(key)) {
        if (parsedJson.at(key).get<string>().length() != 1) {
            throw LoaderException(key + " must be a single character!");
        }
        val = parsedJson.at(key).get<string>()[0];
    }
}

} // namespace loader
} // namespace graphflow
