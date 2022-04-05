#include "src/loader/include/dataset_metadata.h"

#include "src/common/include/exception.h"
#include "src/common/include/file_utils.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

void DatasetMetadata::parseJson(const nlohmann::json& parsedJson, const string& inputDirectory) {
    CSVReaderConfig globalConfig;
    getSpecialChars(parsedJson, globalConfig);
    auto parsedNodeFileDescriptions = parsedJson.at("nodeFileDescriptions");
    for (auto parsedNodeFileDescription : parsedNodeFileDescriptions) {
        auto labelSpecificConfig{globalConfig};
        getSpecialChars(parsedNodeFileDescription, labelSpecificConfig);
        auto filename = parsedNodeFileDescription.at("filename").get<string>();
        DataType dataTypeForId;
        if (parsedNodeFileDescription.contains("IDType")) {
            auto dataTypeString = parsedNodeFileDescription.at("IDType").get<string>();
            dataTypeForId = Types::dataTypeFromString(dataTypeString);
            if (dataTypeForId.typeID != STRING && dataTypeForId.typeID != INT64) {
                throw LoaderException("Invalid ID DataType '" + dataTypeString +
                                      "'. Allowed type only: STRING and INT64.");
            }
        } else {
            dataTypeForId.typeID = STRING;
        }
        nodeFileDescriptions.emplace_back(FileUtils::joinPath(inputDirectory, filename),
            parsedNodeFileDescription.at("label").get<string>(), dataTypeForId,
            labelSpecificConfig);
    }
    auto parsedRelFileDescriptions = parsedJson.at("relFileDescriptions");
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
    const nlohmann::json& parsedJson, CSVReaderConfig& csvReaderConfig) {
    getSpecialChar(parsedJson, "tokenSeparator", csvReaderConfig.tokenSeparator);
    getSpecialChar(parsedJson, "quoteCharacter", csvReaderConfig.quoteChar);
    getSpecialChar(parsedJson, "escapeCharacter", csvReaderConfig.escapeChar);
    getSpecialChar(parsedJson, "listBeginCharacter", csvReaderConfig.listBeginChar);
    getSpecialChar(parsedJson, "listEndCharacter", csvReaderConfig.listEndChar);
}

void DatasetMetadata::getSpecialChar(
    const nlohmann::json& parsedJson, const string& key, char& val) {
    if (parsedJson.contains(key)) {
        if (parsedJson.at(key).get<string>().length() != 1) {
            throw LoaderException(key + " must be a single character!");
        }
        val = parsedJson.at(key).get<string>()[0];
    }
}

} // namespace loader
} // namespace graphflow
