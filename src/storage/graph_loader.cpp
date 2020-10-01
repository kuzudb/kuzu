#include "src/storage/include/graph_loader.h"

#include <fstream>
#include <memory>
#include <unordered_set>

#include "spdlog/sinks/stdout_color_sinks.h"

#include "src/storage/include/file_reader.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

GraphLoader::GraphLoader(std::string inputDirectory) : inputDirectory(std::move(inputDirectory)) {
    logger = spdlog::stderr_color_st("GraphLoader");
    logger->info("Starting GraphLoader.");
}

std::unique_ptr<Graph> GraphLoader::loadGraph() {
    logger->info("Reading metadata and initilializing `Catalog`.");
    auto metadata = readMetadata();
    auto catalog = std::make_unique<Catalog>();
    setNodeAndRelLabels(*metadata, *catalog);
    auto numNodesPerLabel = std::make_unique<std::vector<uint64_t>>(catalog->getNumNodeLabels());
    loadNodes(*metadata, *catalog, *numNodesPerLabel);
    auto numRelsPerLabel = std::make_unique<std::vector<uint64_t>>(catalog->getNumRelLabels());
    loadRels(*metadata, *catalog, *numRelsPerLabel);
    return std::make_unique<Graph>(
        std::move(catalog), std::move(numNodesPerLabel), std::move(numRelsPerLabel));
}

std::unique_ptr<nlohmann::json> GraphLoader::readMetadata() {
    std::ifstream jsonFile(inputDirectory + "/metadata.json");
    auto parsedJson = std::make_unique<nlohmann::json>();
    jsonFile >> *parsedJson;
    return parsedJson;
}

void GraphLoader::setNodeAndRelLabels(const nlohmann::json &metadata, Catalog &catalog) {
    auto stringToNodeLabelMap = std::make_unique<std::unordered_map<std::string, gfLabel_t>>();
    assignLabels(*stringToNodeLabelMap, metadata.at("nodeFileDescriptions"));
    catalog.setStringToNodeLabelMap(std::move(stringToNodeLabelMap));
    auto stringToRelLabelMap = std::make_unique<std::unordered_map<std::string, gfLabel_t>>();
    assignLabels(*stringToRelLabelMap, metadata.at("relFileDescriptions"));
    catalog.setStringToRelLabelMap(std::move(stringToRelLabelMap));
}

void GraphLoader::assignLabels(std::unordered_map<std::string, gfLabel_t> &stringToLabelMap,
    const nlohmann::json &fileDescriptions) {
    auto label = 0;
    for (auto &descriptor : fileDescriptions) {
        stringToLabelMap.insert({descriptor.at("label"), label++});
    }
}

void GraphLoader::loadNodes(
    const nlohmann::json &metadata, Catalog &catalog, std::vector<uint64_t> &numNodesPerLabel) {
    logger->info("Starting to load nodes.");
    auto nodeLabelPropertyDescriptors =
        std::make_unique<std::vector<std::unique_ptr<std::vector<Property>>>>(
            catalog.getNumNodeLabels());
    logger->info("Starting to count node files.");
    for (uint32_t i = 0; i < catalog.getNumNodeLabels(); i++) {
        auto fname = inputDirectory + "/" +
                     metadata.at("nodeFileDescriptions")[i].at("filename").get<std::string>();
        readHeaderAndCountLines(
            fname, numNodesPerLabel, *nodeLabelPropertyDescriptors, i, metadata);
    }
    catalog.setNodePropertyMap(std::move(nodeLabelPropertyDescriptors));
}

void GraphLoader::loadRels(
    const nlohmann::json &metadata, Catalog &catalog, std::vector<uint64_t> &numRelsPerLabel) {
    logger->info("Starting to load rels.");
    auto relLabelPropertyDescriptors =
        std::make_unique<std::vector<std::unique_ptr<std::vector<Property>>>>(
            catalog.getNumRelLabels());
    logger->info("Starting to count rel files.");
    for (uint32_t i = 0; i < catalog.getNumRelLabels(); i++) {
        auto fname = inputDirectory + "/" +
                     metadata.at("relFileDescriptions")[i].at("filename").get<std::string>();
        readHeaderAndCountLines(fname, numRelsPerLabel, *relLabelPropertyDescriptors, i, metadata);
    }
    catalog.setRelPropertyMap(std::move(relLabelPropertyDescriptors));
}

void GraphLoader::readHeaderAndCountLines(const std::string &fname,
    std::vector<uint64_t> &numPerLabel,
    std::vector<std::unique_ptr<std::vector<Property>>> &labelPropertyDescriptors, gfLabel_t label,
    const nlohmann::json &metadata) {
    logger->info("Counting and parsing header: " + fname);
    auto reader =
        std::make_unique<FileReader>(fname, metadata.at("tokenSeparator").get<std::string>()[0]);
    labelPropertyDescriptors[label] =
        std::move(parseHeader(metadata, std::move(reader->getLine())));
    auto numLines = 0;
    while (reader->hasMoreTokens()) {
        reader->skipLine();
        numLines++;
    }
    numPerLabel[label] = numLines;
}

std::unique_ptr<std::vector<Property>> GraphLoader::parseHeader(
    const nlohmann::json &metadata, std::unique_ptr<std::string> header) {
    auto separator = metadata.at("tokenSeparator").get<std::string>()[0];
    auto splittedHeader = std::make_unique<std::vector<std::string>>();
    size_t startPos = 0;
    size_t endPos = 0;
    while ((endPos = header->find(separator, startPos)) != std::string::npos) {
        splittedHeader->push_back(header->substr(startPos, endPos - startPos));
        startPos = endPos + 1;
    }
    splittedHeader->push_back(header->substr(startPos));
    auto properties = std::make_unique<std::vector<Property>>(splittedHeader->size());
    auto i = 0;
    auto propertyNameSet = std::make_unique<std::unordered_set<std::string>>();
    for (auto &split : *splittedHeader) {
        auto propertyName = split.substr(0, split.find(":"));
        if (propertyNameSet->find(propertyName) != propertyNameSet->end()) {
            throw std::invalid_argument("Same property name in csv file.");
        }
        propertyNameSet->insert(propertyName);
        (*properties)[i].propertyName = propertyName;
        (*properties)[i].dataType = getDataType(split.substr(split.find(":") + 1));
        i++;
    }
    return properties;
}

} // namespace storage
} // namespace graphflow
