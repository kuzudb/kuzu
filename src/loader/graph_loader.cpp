#include "src/loader/include/graph_loader.h"

#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <unordered_set>

#include "src/common/include/configs.h"
#include "src/loader/include/csv_reader.h"
#include "src/loader/include/nodes_loader.h"
#include "src/loader/include/rels_loader.h"

using namespace graphflow::storage;

namespace graphflow {
namespace loader {

void GraphLoader::loadGraph() {
    logger->info("Starting GraphLoader.");
    auto metadata = readMetadata();
    assignLabels(catalog.stringToNodeLabelMap, metadata->at("nodeFileDescriptions"));
    assignLabels(catalog.stringToRelLabelMap, metadata->at("relFileDescriptions"));
    setCardinalities(*metadata);
    setSrcDstNodeLabelsForRelLabels(*metadata);
    auto nodeIDMaps = loadNodes(*metadata);
    loadRels(*metadata, *nodeIDMaps);
    logger->info("Writing catalog.bin");
    catalog.saveToFile(outputDirectory);
    logger->info("Writing graph.bin");
    graph.saveToFile(outputDirectory);
    logger->info("Done GraphLoader.");
}

unique_ptr<nlohmann::json> GraphLoader::readMetadata() {
    logger->info("Reading metadata and initilializing `Catalog`.");
    ifstream jsonFile(inputDirectory + "/metadata.json");
    auto parsedJson = make_unique<nlohmann::json>();
    jsonFile >> *parsedJson;
    return parsedJson;
}

void GraphLoader::assignLabels(stringToLabelMap_t& map, const nlohmann::json& fileDescriptions) {
    auto label = 0;
    for (auto& descriptor : fileDescriptions) {
        auto labelString = descriptor.at("label").get<string>();
        auto labelCharArray = new char[labelString.size() + 1];
        memcpy(labelCharArray, labelString.c_str(), labelString.size());
        labelCharArray[labelString.size()] = 0;
        map.insert({labelCharArray, label++});
    }
}

void GraphLoader::setCardinalities(const nlohmann::json& metadata) {
    for (auto& descriptor : metadata.at("relFileDescriptions")) {
        catalog.relLabelToCardinalityMap.push_back(getCardinality(descriptor.at("cardinality")));
    }
}

void GraphLoader::setSrcDstNodeLabelsForRelLabels(const nlohmann::json& metadata) {
    catalog.dstNodeLabelToRelLabel.resize(catalog.getNodeLabelsCount());
    catalog.srcNodeLabelToRelLabel.resize(catalog.getNodeLabelsCount());
    catalog.relLabelToSrcNodeLabels.resize(catalog.getRelLabelsCount());
    catalog.relLabelToDstNodeLabels.resize(catalog.getRelLabelsCount());
    auto relLabel = 0;
    for (auto& descriptor : metadata.at("relFileDescriptions")) {
        for (auto& nodeLabel : descriptor.at("srcNodeLabels")) {
            auto labelString = nodeLabel.get<string>();
            auto labelIdx = catalog.stringToNodeLabelMap[labelString.c_str()];
            catalog.srcNodeLabelToRelLabel[labelIdx].push_back(relLabel);
            catalog.relLabelToSrcNodeLabels[relLabel].push_back(labelIdx);
        }
        for (auto& nodeLabel : descriptor.at("dstNodeLabels")) {
            auto labelString = nodeLabel.get<string>();
            auto labelIdx = catalog.stringToNodeLabelMap[labelString.c_str()];
            catalog.dstNodeLabelToRelLabel[labelIdx].push_back(relLabel);
            catalog.relLabelToDstNodeLabels[relLabel].push_back(labelIdx);
        }
        relLabel++;
    }
}

unique_ptr<vector<unique_ptr<NodeIDMap>>> GraphLoader::loadNodes(const nlohmann::json& metadata) {
    logger->info("Starting to load nodes.");
    vector<string> fnames(catalog.getNodeLabelsCount());
    vector<uint64_t> numBlocksPerLabel(catalog.getNodeLabelsCount());
    vector<vector<uint64_t>> numLinesPerBlock(catalog.getNodeLabelsCount());
    inferFilenamesInitPropertyMapAndCalcNumBlocks(catalog.getNodeLabelsCount(),
        metadata.at("nodeFileDescriptions"), fnames, numBlocksPerLabel, catalog.nodePropertyMaps,
        metadata.at("tokenSeparator").get<string>()[0]);
    countNodesAndInitUnstrPropertyMaps(numLinesPerBlock, numBlocksPerLabel,
        metadata.at("tokenSeparator").get<string>()[0], fnames);
    auto nodeIDMaps = make_unique<vector<unique_ptr<NodeIDMap>>>(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        (*nodeIDMaps)[nodeLabel] = make_unique<NodeIDMap>(graph.numNodesPerLabel[nodeLabel]);
    }
    NodesLoader nodesLoader{threadPool, graph, catalog, metadata, outputDirectory};
    nodesLoader.load(fnames, numBlocksPerLabel, numLinesPerBlock, *nodeIDMaps);
    logger->info("Creating reverse NodeIDMaps.");
    for (auto& nodeIDMap : *nodeIDMaps) {
        threadPool.execute([&](NodeIDMap* x) { x->createNodeIDToOffsetMap(); }, nodeIDMap.get());
    }
    threadPool.wait();
    return nodeIDMaps;
}

void GraphLoader::loadRels(
    const nlohmann::json& metadata, vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    logger->info("Starting to load rels.");
    vector<string> fnames(catalog.getRelLabelsCount());
    vector<uint64_t> numBlocksPerLabel(catalog.getRelLabelsCount());
    inferFilenamesInitPropertyMapAndCalcNumBlocks(catalog.getRelLabelsCount(),
        metadata.at("relFileDescriptions"), fnames, numBlocksPerLabel, catalog.relPropertyMaps,
        metadata.at("tokenSeparator").get<string>()[0]);
    RelsLoader relsLoader{threadPool, graph, catalog, metadata, nodeIDMaps, outputDirectory};
    relsLoader.load(fnames, numBlocksPerLabel);
}

void GraphLoader::inferFilenamesInitPropertyMapAndCalcNumBlocks(label_t numLabels,
    nlohmann::json filedescriptions, vector<string>& filenames, vector<uint64_t>& numBlocksPerLabel,
    vector<unordered_map<string, Property>>& propertyMaps, const char tokenSeparator) {
    for (label_t label = 0; label < numLabels; label++) {
        auto fileDescription = filedescriptions[label];
        filenames[label] = inputDirectory + "/" + fileDescription.at("filename").get<string>();
    }
    propertyMaps.resize(numLabels);
    logger->info("Parsing headers.");
    for (label_t label = 0; label < numLabels; label++) {
        logger->debug("path=`{0}`", filenames[label]);
        ifstream f(filenames[label], ios_base::in);
        string header;
        getline(f, header);
        parseHeader(tokenSeparator, header, propertyMaps[label]);
        f.seekg(0, ios_base::end);
        numBlocksPerLabel[label] = 1 + (f.tellg() / CSV_READING_BLOCK_SIZE);
    }
    logger->info("Done.");
}

void GraphLoader::parseHeader(
    const char tokenSeparator, string& header, unordered_map<string, Property>& propertyMap) {
    auto splittedHeader = make_unique<vector<string>>();
    size_t startPos = 0, endPos = 0;
    while ((endPos = header.find(tokenSeparator, startPos)) != string::npos) {
        splittedHeader->push_back(header.substr(startPos, endPos - startPos));
        startPos = endPos + 1;
    }
    splittedHeader->push_back(header.substr(startPos));
    auto propertyNameSet = make_unique<unordered_set<string>>();
    uint32_t propertyIdx = 0;
    for (auto& split : *splittedHeader) {
        auto propertyName = split.substr(0, split.find(":"));
        if (propertyNameSet->find(propertyName) != propertyNameSet->end()) {
            throw invalid_argument("Same property name in csv file.");
        }
        propertyNameSet->insert(propertyName);
        auto dataType = getDataType(split.substr(split.find(":") + 1));
        if (NODE != dataType && LABEL != dataType) {
            propertyMap.insert({propertyName, Property{dataType, propertyIdx++}});
        }
    }
}

void GraphLoader::countNodesAndInitUnstrPropertyMaps(vector<vector<uint64_t>>& numLinesPerBlock,
    vector<uint64_t>& numBlocksPerLabel, const char tokenSeparator, vector<string>& fnames) {
    logger->info("Counting number of (nodes/rels) in labels.");
    auto numLabels = catalog.getNodeLabelsCount();
    vector<vector<unordered_set<const char*, charArrayHasher, charArrayEqualTo>>>
        unstructuredPropertyKeys(numLabels);
    for (label_t label = 0; label < numLabels; label++) {
        unstructuredPropertyKeys[label].resize(numBlocksPerLabel[label]);
        numLinesPerBlock[label].resize(numBlocksPerLabel[label]);
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            threadPool.execute(countLinesAndScanUnstrPropertiesInBlockTask, fnames[label],
                tokenSeparator, catalog.nodePropertyMaps[label].size(),
                &unstructuredPropertyKeys[label][blockId], &numLinesPerBlock, label, blockId,
                logger);
        }
    }
    threadPool.wait();
    catalog.nodeUnstrPropertyMaps.resize(numLabels);
    for (label_t label = 0; label < numLabels; label++) {
        initUnstrPropertyMapForLabel(label, unstructuredPropertyKeys[label]);
    }
    graph.numNodesPerLabel.resize(numLabels);
    for (label_t label = 0; label < numLabels; label++) {
        graph.numNodesPerLabel[label] = 0;
        numLinesPerBlock[label][0]--;
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            graph.numNodesPerLabel[label] += numLinesPerBlock[label][blockId];
        }
    }
    logger->info("Done.");
}

void GraphLoader::initUnstrPropertyMapForLabel(label_t label,
    vector<unordered_set<const char*, charArrayHasher, charArrayEqualTo>>& unstrPropertyKeys) {
    unordered_set<const char*, charArrayHasher, charArrayEqualTo> unionOfUnstrPropertyKeys;
    for (auto& blockUnstrPropertyKeys : unstrPropertyKeys) {
        for (auto property : blockUnstrPropertyKeys) {
            auto found = unionOfUnstrPropertyKeys.find(property);
            if (found != unionOfUnstrPropertyKeys.end()) {
                delete[] * found;
            } else {
                unionOfUnstrPropertyKeys.insert(property);
            }
        }
    }
    auto propertyIdx = 0u;
    for (auto& propertyString : unionOfUnstrPropertyKeys) {
        catalog.nodeUnstrPropertyMaps[label].emplace(
            string(propertyString), Property(UNKNOWN, propertyIdx++));
    }
}

void GraphLoader::countLinesAndScanUnstrPropertiesInBlockTask(string fname, char tokenSeparator,
    const uint32_t numProperties,
    unordered_set<const char*, charArrayHasher, charArrayEqualTo>* unstrPropertyKeys,
    vector<vector<uint64_t>>* numLinesPerBlock, label_t label, uint32_t blockId,
    shared_ptr<spdlog::logger> logger) {
    logger->trace("Start: path=`{0}` blkIdx={1}", fname, blockId);
    CSVReader reader(fname, tokenSeparator, blockId);
    (*numLinesPerBlock)[label][blockId] = 0ull;
    while (reader.hasNextLine()) {
        (*numLinesPerBlock)[label][blockId]++;
        reader.hasNextToken();
        for (auto i = 0u; i < numProperties; ++i) {
            reader.hasNextToken();
        }
        while (reader.hasNextToken()) {
            auto uPropertyString = reader.getString();
            *strchr(uPropertyString, ':') = 0;
            if (unstrPropertyKeys->find(uPropertyString) == unstrPropertyKeys->end()) {
                auto len = strlen(uPropertyString);
                auto uPropertyCopy = new char[len + 1];
                memcpy(uPropertyCopy, uPropertyString, len);
                uPropertyCopy[len] = 0;
                unstrPropertyKeys->insert(uPropertyCopy);
            }
        }
    }
    logger->trace("End: path=`{0}` blkIdx={1}", fname, blockId);
}

} // namespace loader
} // namespace graphflow
