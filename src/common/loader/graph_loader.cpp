#include "src/common/loader/include/graph_loader.h"

#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <unordered_set>

#include "src/common/include/configs.h"
#include "src/common/loader/include/csv_reader.h"
#include "src/common/loader/include/nodes_loader.h"
#include "src/common/loader/include/rels_loader.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace common {

void GraphLoader::loadGraph() {
    logger->info("Starting GraphLoader.");
    auto metadata = readMetadata();
    Catalog catalog{};
    assignLabels(catalog.stringToNodeLabelMap, metadata->at("nodeFileDescriptions"));
    assignLabels(catalog.stringToRelLabelMap, metadata->at("relFileDescriptions"));
    setCardinalities(catalog, *metadata);
    setSrcDstNodeLabelsForRelLabels(catalog, *metadata);
    Graph graph;
    auto nodeIDMaps = loadNodes(*metadata, graph, catalog);
    loadRels(*metadata, graph, catalog, move(nodeIDMaps));
    logger->info("Writing catalog.bin");
    catalog.saveToFile(outputDirectory);
    logger->info("Writing graph.bin");
    graph.saveToFile(outputDirectory);
    logger->info("done.");
}

unique_ptr<nlohmann::json> GraphLoader::readMetadata() {
    logger->info("Reading metadata and initilializing `Catalog`.");
    ifstream jsonFile(inputDirectory + "/metadata.json");
    auto parsedJson = make_unique<nlohmann::json>();
    jsonFile >> *parsedJson;
    return parsedJson;
}

void GraphLoader::assignLabels(
    unordered_map<string, gfLabel_t> &stringToLabelMap, const nlohmann::json &fileDescriptions) {
    auto label = 0;
    for (auto &descriptor : fileDescriptions) {
        stringToLabelMap.insert({descriptor.at("label"), label++});
    }
}

void GraphLoader::setCardinalities(Catalog &catalog, const nlohmann::json &metadata) {
    for (auto &descriptor : metadata.at("relFileDescriptions")) {
        catalog.relLabelToCardinalityMap.push_back(getCardinality(descriptor.at("cardinality")));
    }
}

void GraphLoader::setSrcDstNodeLabelsForRelLabels(
    Catalog &catalog, const nlohmann::json &metadata) {
    catalog.dstNodeLabelToRelLabel.resize(catalog.getNodeLabelsCount());
    catalog.srcNodeLabelToRelLabel.resize(catalog.getNodeLabelsCount());
    catalog.relLabelToSrcNodeLabels.resize(catalog.getRelLabelsCount());
    catalog.relLabelToDstNodeLabels.resize(catalog.getRelLabelsCount());
    auto relLabel = 0;
    for (auto &descriptor : metadata.at("relFileDescriptions")) {
        for (auto &nodeLabelString : descriptor.at("srcNodeLabels")) {
            auto nodeLabel = catalog.stringToNodeLabelMap[nodeLabelString];
            catalog.srcNodeLabelToRelLabel[nodeLabel].push_back(relLabel);
            catalog.relLabelToSrcNodeLabels[relLabel].push_back(nodeLabel);
        }
        for (auto &nodeLabelString : descriptor.at("dstNodeLabels")) {
            auto nodeLabel = catalog.stringToNodeLabelMap[nodeLabelString];
            catalog.dstNodeLabelToRelLabel[nodeLabel].push_back(relLabel);
            catalog.relLabelToDstNodeLabels[relLabel].push_back(nodeLabel);
        }
        relLabel++;
    }
}

unique_ptr<vector<shared_ptr<NodeIDMap>>> GraphLoader::loadNodes(
    const nlohmann::json &metadata, Graph &graph, Catalog &catalog) {
    vector<string> fnames(catalog.getNodeLabelsCount());
    vector<uint64_t> numBlocksPerLabel(catalog.getNodeLabelsCount());
    vector<vector<uint64_t>> numLinesPerBlock(catalog.getNodeLabelsCount());
    inferFilenamesInitPropertyMapAndCountLinesPerBlock(catalog.getNodeLabelsCount(),
        metadata.at("nodeFileDescriptions"), fnames, numBlocksPerLabel, catalog.nodePropertyMaps,
        metadata.at("tokenSeparator").get<string>()[0]);
    countLinesPerBlockAndInitNumPerLabel(catalog.getNodeLabelsCount(), numLinesPerBlock,
        numBlocksPerLabel, metadata.at("tokenSeparator").get<string>()[0], fnames,
        graph.numNodesPerLabel);
    auto nodeIDMaps = make_unique<vector<shared_ptr<NodeIDMap>>>();
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        (*nodeIDMaps).push_back(make_shared<NodeIDMap>(graph.numNodesPerLabel[nodeLabel]));
    }
    NodesLoader nodesLoader{threadPool, catalog, metadata, outputDirectory};
    nodesLoader.load(fnames, graph.numNodesPerLabel, numBlocksPerLabel,
        numLinesPerBlock, *nodeIDMaps);
    return nodeIDMaps;
}

void GraphLoader::loadRels(const nlohmann::json &metadata, Graph &graph, Catalog &catalog,
    unique_ptr<vector<shared_ptr<NodeIDMap>>> nodeIDMaps) {
    logger->info("Starting to load rels.");
    vector<string> fnames(catalog.getRelLabelsCount());
    vector<uint64_t> numBlocksPerLabel(catalog.getRelLabelsCount());
    inferFilenamesInitPropertyMapAndCountLinesPerBlock(catalog.getRelLabelsCount(),
        metadata.at("relFileDescriptions"), fnames, numBlocksPerLabel, catalog.relPropertyMaps,
        metadata.at("tokenSeparator").get<string>()[0]);
    RelsLoader relsLoader{threadPool, graph, catalog, metadata, outputDirectory};
    relsLoader.load(fnames, move(nodeIDMaps), numBlocksPerLabel);
}

void GraphLoader::inferFilenamesInitPropertyMapAndCountLinesPerBlock(gfLabel_t numLabels,
    nlohmann::json filedescriptions, vector<string> &filenames, vector<uint64_t> &numBlocksPerLabel,
    vector<vector<Property>> &propertyMap, const char tokenSeparator) {
    for (gfLabel_t label = 0; label < numLabels; label++) {
        auto fileDescription = filedescriptions[label];
        filenames[label] = inputDirectory + "/" + fileDescription.at("filename").get<string>();
    }
    initPropertyMapAndCalcNumBlocksPerLabel(
        numLabels, filenames, numBlocksPerLabel, propertyMap, tokenSeparator);
}

void GraphLoader::initPropertyMapAndCalcNumBlocksPerLabel(gfLabel_t numLabels,
    vector<string> &filenames, vector<uint64_t> &numPerLabel, vector<vector<Property>> &propertyMap,
    const char tokenSeparator) {
    propertyMap.resize(numLabels);
    for (gfLabel_t label = 0; label < numLabels; label++) {
        logger->info("Parsing header: " + filenames[label]);
        ifstream f(filenames[label], ios_base::in);
        string header;
        getline(f, header);
        parseHeader(tokenSeparator, header, propertyMap[label]);
        f.seekg(0, ios_base::end);
        numPerLabel[label] = 1 + (f.tellg() / CSV_READING_BLOCK_SIZE);
    }
}

void GraphLoader::parseHeader(
    const char tokenSeparator, string &header, vector<Property> &labelPropertyDescriptors) {
    auto splittedHeader = make_unique<vector<string>>();
    size_t startPos = 0, endPos = 0;
    while ((endPos = header.find(tokenSeparator, startPos)) != string::npos) {
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
    const char tokenSeparator, vector<string> &fnames, vector<uint64_t> &numPerLabel) {
    logger->info("Counting number of (nodes/rels) in labels.");
    for (gfLabel_t label = 0; label < numLabels; label++) {
        numLinesPerBlock[label].resize(numBlocksPerLabel[label]);
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            threadPool.execute(fileBlockLinesCounterTask, fnames[label], tokenSeparator,
                &numLinesPerBlock, label, blockId, logger);
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
    }
    logger->info("done.");
}

void GraphLoader::fileBlockLinesCounterTask(string fname, char tokenSeparator,
    vector<vector<uint64_t>> *numLinesPerBlock, gfLabel_t label, uint32_t blockId,
    shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", fname, blockId);
    CSVReader reader(fname, tokenSeparator, blockId);
    (*numLinesPerBlock)[label][blockId] = 0ull;
    while (reader.hasMore()) {
        (*numLinesPerBlock)[label][blockId]++;
        reader.skipLine();
    }
    logger->debug("end   {0} {1} {2}", fname, blockId, (*numLinesPerBlock)[label][blockId]);
}

} // namespace common
} // namespace graphflow
