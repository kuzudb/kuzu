#include "src/common/loader/include/graph_loader.h"

#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <unordered_set>

#include "spdlog/sinks/stdout_color_sinks.h"

#include "src/common/include/configs.h"
#include "src/common/loader/include/loader_tasks.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace common {

GraphLoader::GraphLoader(string inputDirectory, string outputDirectory)
    : inputDirectory(inputDirectory), outputDirectory(outputDirectory) {
    logger = spdlog::stderr_color_mt("GraphLoader");
    logger->info("Starting GraphLoader.");
}

void GraphLoader::loadGraph() {
    auto metadata = readMetadata();
    Catalog catalog;
    Graph graph;
    setNodeAndRelLabels(*metadata, catalog);
    setCardinalities(*metadata, catalog);
    setSrcDstNodeLabelsForRelLabels(*metadata, catalog);
    loadNodes(*metadata, catalog, graph.numNodesPerLabel);
    loadRels(*metadata, catalog, graph.numRelsPerLabel);
}

unique_ptr<nlohmann::json> GraphLoader::readMetadata() {
    logger->info("Reading metadata and initilializing `Catalog`.");
    ifstream jsonFile(inputDirectory + "/metadata.json");
    auto parsedJson = make_unique<nlohmann::json>();
    jsonFile >> *parsedJson;
    return parsedJson;
}

void GraphLoader::setNodeAndRelLabels(const nlohmann::json &metadata, Catalog &catalog) {
    assignLabels(catalog.stringToNodeLabelMap, metadata.at("nodeFileDescriptions"));
    assignLabels(catalog.stringToRelLabelMap, metadata.at("relFileDescriptions"));
}

void GraphLoader::assignLabels(
    unordered_map<string, gfLabel_t> &stringToLabelMap, const nlohmann::json &fileDescriptions) {
    logger->info("Assigning Labels.");
    auto label = 0;
    for (auto &descriptor : fileDescriptions) {
        stringToLabelMap.insert({descriptor.at("label"), label++});
    }
}

void GraphLoader::setCardinalities(const nlohmann::json &metadata, Catalog &catalog) {
    logger->info("Setting Cardinalities.");
    for (auto &descriptor : metadata.at("relFileDescriptions")) {
        catalog.relLabelToCardinalityMap.push_back(getCardinality(descriptor.at("cardinality")));
    }
}

void GraphLoader::setSrcDstNodeLabelsForRelLabels(
    const nlohmann::json &metadata, Catalog &catalog) {
    catalog.dstNodeLabelToRelLabel.resize(catalog.getNodeLabelsCount());
    catalog.srcNodeLabelToRelLabel.resize(catalog.getNodeLabelsCount());
    catalog.relLabelToSrcNodeLabel.resize(catalog.getRelLabelsCount());
    catalog.relLabelToDstNodeLabel.resize(catalog.getRelLabelsCount());
    auto relLabel = 0;
    for (auto &descriptor : metadata.at("relFileDescriptions")) {
        for (auto &nodeLabelString : descriptor.at("srcNodeLabels")) {
            auto nodeLabel = catalog.getNodeLabelForString(nodeLabelString);
            catalog.srcNodeLabelToRelLabel[nodeLabel].push_back(relLabel);
            catalog.relLabelToSrcNodeLabel[relLabel].push_back(nodeLabel);
        }
        for (auto &nodeLabelString : descriptor.at("dstNodeLabels")) {
            auto nodeLabel = catalog.getNodeLabelForString(nodeLabelString);
            catalog.dstNodeLabelToRelLabel[nodeLabel].push_back(relLabel);
            catalog.relLabelToDstNodeLabel[relLabel].push_back(nodeLabel);
        }
        relLabel++;
    }
}

void GraphLoader::loadNodes(
    const nlohmann::json &metadata, Catalog &catalog, vector<uint64_t> &numNodesPerLabel) {
    logger->info("Starting to load nodes.");
    vector<string> filenames(catalog.getNodeLabelsCount());
    for (gfLabel_t label = 0; label < catalog.getNodeLabelsCount(); label++) {
        auto fileDescription = metadata.at("nodeFileDescriptions")[label];
        filenames[label] = inputDirectory + "/" + fileDescription.at("filename").get<string>();
    }
    vector<uint64_t> numBlocksPerLabel(catalog.getNodeLabelsCount());
    initPropertyMapAndCalcNumBlocksPerLabel(catalog.getNodeLabelsCount(), filenames,
        numBlocksPerLabel, catalog.nodePropertyMap, metadata);
    vector<vector<uint64_t>> numLinesPerBlock(catalog.getNodeLabelsCount());
    countLinesPerBlockAndInitNumPerLabel(catalog.getNodeLabelsCount(), numLinesPerBlock,
        numBlocksPerLabel, numNodesPerLabel, metadata, filenames);
    readNodePropertiesAndSerialize(catalog, metadata, filenames, catalog.nodePropertyMap,
        numNodesPerLabel, numBlocksPerLabel, numLinesPerBlock);
}

void GraphLoader::readNodePropertiesAndSerialize(Catalog &catalog, const nlohmann::json &metadata,
    vector<string> &filenames, vector<vector<Property>> &propertyMap,
    vector<uint64_t> &numNodesPerLabel, vector<uint64_t> &numBlocksPerLabel,
    vector<vector<uint64_t>> &numLinesPerBlock) {
    logger->info("Starting to read and store node properties...");
    for (gfLabel_t label = 0; label < catalog.getNodeLabelsCount(); label++) {
        logger->info("Initializing `RawColumn` for Node Label {0}...", label);
        auto inMemoryColumns = createInMemoryColumnsForNodeProperties(
            propertyMap[label], catalog, numNodesPerLabel[label]);
        gfNodeOffset_t offset = 0;
        logger->info("Reading properties for Node Label {0}...", label);
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            threadPool.execute(LoaderTasks::NodePropertyReaderTask, filenames[label],
                metadata.at("tokenSeparator").get<string>()[0], propertyMap[label], blockId, offset,
                inMemoryColumns.get(), logger);
            offset += numLinesPerBlock[label][blockId];
        }
        threadPool.wait();
        logger->info("Finished reading properties for Node Label {0}...", label);
        for (uint32_t i = 1; i < propertyMap[label].size(); i++) {
            if (nullptr != (*inMemoryColumns)[i].get()) {
                auto fName = outputDirectory + "/v-" + to_string(label) + "-" +
                             propertyMap[label][i].propertyName + ".vcol";
                threadPool.execute(
                    LoaderTasks::ColumnSerializer, fName, (*inMemoryColumns)[i].get(), logger);
            }
        }
        threadPool.wait();
    }
}

unique_ptr<vector<unique_ptr<InMemoryColumnBase>>>
GraphLoader::createInMemoryColumnsForNodeProperties(
    vector<Property> &propertyMap, Catalog &catalog, uint64_t numNodes) {
    auto inMemoryColumns = make_unique<vector<unique_ptr<InMemoryColumnBase>>>();
    for (auto &property : propertyMap) {
        switch (property.dataType) {
        case INT:
            (*inMemoryColumns).push_back(make_unique<InMemoryColumn<gfInt_t>>(numNodes));
            break;
        case DOUBLE:
            (*inMemoryColumns).push_back(make_unique<InMemoryColumn<gfDouble_t>>(numNodes));
            break;
        case BOOLEAN:
            (*inMemoryColumns).push_back(make_unique<InMemoryColumn<gfBool_t>>(numNodes));
            break;
        default:
            logger->warn("Ignoring string properties.");
            (*inMemoryColumns).push_back(nullptr);
        }
    }
    return inMemoryColumns;
}

void GraphLoader::loadRels(
    const nlohmann::json &metadata, Catalog &catalog, vector<uint64_t> &numRelsPerLabel) {
    logger->info("Starting to load rels.");
    vector<string> filenames(catalog.getRelLabelsCount());
    for (gfLabel_t label = 0; label < catalog.getRelLabelsCount(); label++) {
        auto fileDescription = metadata.at("relFileDescriptions")[label];
        filenames[label] = inputDirectory + "/" + fileDescription.at("filename").get<string>();
    }
    vector<uint64_t> numBlocksPerLabel(catalog.getRelLabelsCount());
    initPropertyMapAndCalcNumBlocksPerLabel(catalog.getRelLabelsCount(), filenames,
        numBlocksPerLabel, catalog.relPropertyMap, metadata);
    vector<vector<uint64_t>> numLinesPerBlock(catalog.getRelLabelsCount());
    countLinesPerBlockAndInitNumPerLabel(catalog.getRelLabelsCount(), numLinesPerBlock,
        numBlocksPerLabel, numRelsPerLabel, metadata, filenames);
}

void GraphLoader::initPropertyMapAndCalcNumBlocksPerLabel(gfLabel_t numLabels,
    vector<string> &filenames, vector<uint64_t> &numPerLabel, vector<vector<Property>> &propertyMap,
    const nlohmann::json &metadata) {
    propertyMap.resize(numLabels);
    for (gfLabel_t label = 0; label < numLabels; label++) {
        logger->info("Parsing header: " + filenames[label]);
        ifstream f(filenames[label], ios_base::in);
        string header;
        getline(f, header);
        parseHeader(metadata, header, propertyMap[label]);
        f.seekg(0, ios_base::end);
        numPerLabel[label] = 1 + (f.tellg() / CSV_READING_BLOCK_SIZE);
    }
}

void GraphLoader::parseHeader(
    const nlohmann::json &metadata, string &header, vector<Property> &labelPropertyDescriptors) {
    auto separator = metadata.at("tokenSeparator").get<string>()[0];
    auto splittedHeader = make_unique<vector<string>>();
    size_t startPos = 0;
    size_t endPos = 0;
    while ((endPos = header.find(separator, startPos)) != string::npos) {
        splittedHeader->push_back(header.substr(startPos, endPos - startPos));
        startPos = endPos + 1;
    }
    splittedHeader->push_back(header.substr(startPos));
    auto propertyNameSet = make_unique<unordered_set<string>>();
    for (auto &split : *splittedHeader) {
        auto propertyName = split.substr(0, split.find(":"));
        if (propertyNameSet->find(propertyName) != propertyNameSet->end()) {
            throw invalid_argument("Same property name in csv file.");
        }
        propertyNameSet->insert(propertyName);
        labelPropertyDescriptors.push_back(
            Property(propertyName, getDataType(split.substr(split.find(":") + 1))));
    }
}

void GraphLoader::countLinesPerBlockAndInitNumPerLabel(gfLabel_t numLabels,
    vector<vector<uint64_t>> &numLinesPerBlock, vector<uint64_t> &numBlocksPerLabel,
    vector<uint64_t> &numPerLabel, const nlohmann::json &metadata, vector<string> &filenames) {
    logger->info("Counting number of (nodes/rels) in labels...");
    for (gfLabel_t label = 0; label < numLabels; label++) {
        numLinesPerBlock[label].resize(numBlocksPerLabel[label]);
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            threadPool.execute(LoaderTasks::fileBlockLinesCounterTask, filenames[label],
                metadata.at("tokenSeparator").get<string>()[0], &numLinesPerBlock, label, blockId,
                logger);
        }
    }
    threadPool.wait();
    numPerLabel.resize(numLabels);
    for (gfLabel_t label = 0; label < numLabels; label++) {
        numPerLabel[label] = 0;
        numLinesPerBlock[label][0]--;
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            numPerLabel[label] += numLinesPerBlock[label][blockId];
        }
        logger->debug("Label {0}: {1}", label, numPerLabel[label]);
    }
    logger->info("Finished counting number of (nodes/rels) in labels...");
}

} // namespace common
} // namespace graphflow
