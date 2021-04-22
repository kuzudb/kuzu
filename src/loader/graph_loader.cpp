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

    // populate data fields in Catalog
    assignIdxToLabels(catalog.stringToNodeLabelMap, metadata->at("nodeFileDescriptions"));
    assignIdxToLabels(catalog.stringToRelLabelMap, metadata->at("relFileDescriptions"));
    setCardinalitiesOfRelLabels(*metadata);
    setSrcDstNodeLabelsForRelLabels(*metadata);

    auto nodeIDMaps = loadNodes(*metadata);

    logger->info("Creating reverse NodeIDMaps.");
    for (auto& nodeIDMap : *nodeIDMaps) {
        threadPool.execute([&](NodeIDMap* x) { x->createNodeIDToOffsetMap(); }, nodeIDMap.get());
    }
    threadPool.wait();

    loadRels(*metadata, *nodeIDMaps);

    // write catalog and graph objects to file
    logger->info("Writing Catalog object.");
    catalog.saveToFile(outputDirectory);
    logger->info("Writing Graph object.");
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

void GraphLoader::assignIdxToLabels(
    stringToLabelMap_t& map, const nlohmann::json& fileDescriptions) {
    auto label = 0;
    for (auto& descriptor : fileDescriptions) {
        auto labelString = descriptor.at("label").get<string>();
        auto labelCharArray = new char[labelString.size() + 1];
        memcpy(labelCharArray, labelString.c_str(), labelString.size());
        labelCharArray[labelString.size()] = 0;
        map.insert({labelCharArray, label++});
    }
}

void GraphLoader::setCardinalitiesOfRelLabels(const nlohmann::json& metadata) {
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

    // get the name of csv file for each node label
    vector<string> fnames(catalog.getNodeLabelsCount());
    inferFnamesFromMetadataFileDesriptions(
        catalog.getNodeLabelsCount(), metadata.at("nodeFileDescriptions"), fnames);

    // initialize node's propertyKey map and count number of blocks in each label's csv file
    vector<uint64_t> numBlocksPerLabel(catalog.getNodeLabelsCount());
    initPropertyKeyMapAndCalcNumBlocks(catalog.getNodeLabelsCount(), fnames, numBlocksPerLabel,
        catalog.nodePropertyKeyMaps, metadata.at("tokenSeparator").get<string>()[0]);

    // count number of lines and get unstructured propertyKeys in each block of each label.
    labelBlockUnstrPropertyKeys_t labelBlockUnstrPropertyKeys(catalog.getNodeLabelsCount());
    vector<vector<uint64_t>> numLinesPerBlock(catalog.getNodeLabelsCount());
    countLinesAndGetUnstrPropertyKeys(numLinesPerBlock, labelBlockUnstrPropertyKeys,
        numBlocksPerLabel, metadata.at("tokenSeparator").get<string>()[0], fnames);

    // initialize unstructured property key map in Catalog.
    initNodeUnstrPropertyKeyMaps(labelBlockUnstrPropertyKeys);

    // create NodeIDMaps and call NodesLoader.
    auto nodeIDMaps = make_unique<vector<unique_ptr<NodeIDMap>>>(catalog.getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        (*nodeIDMaps)[nodeLabel] = make_unique<NodeIDMap>(graph.numNodesPerLabel[nodeLabel]);
    }
    NodesLoader nodesLoader{threadPool, graph, catalog, metadata, outputDirectory};
    nodesLoader.load(fnames, numBlocksPerLabel, numLinesPerBlock, *nodeIDMaps);

    return nodeIDMaps;
}

void GraphLoader::loadRels(
    const nlohmann::json& metadata, vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    logger->info("Starting to load rels.");

    // get the name of csv file for each label
    vector<string> fnames(catalog.getRelLabelsCount());
    inferFnamesFromMetadataFileDesriptions(
        catalog.getRelLabelsCount(), metadata.at("relFileDescriptions"), fnames);

    // initialize rel's propertyKey map and count number of blocks in each label's csv file
    vector<uint64_t> numBlocksPerLabel(catalog.getRelLabelsCount());
    initPropertyKeyMapAndCalcNumBlocks(catalog.getRelLabelsCount(), fnames, numBlocksPerLabel,
        catalog.relPropertyKeyMaps, metadata.at("tokenSeparator").get<string>()[0]);

    RelsLoader relsLoader{threadPool, graph, catalog, metadata, nodeIDMaps, outputDirectory};
    relsLoader.load(fnames, numBlocksPerLabel);
}

void GraphLoader::inferFnamesFromMetadataFileDesriptions(
    label_t numLabels, nlohmann::json filedescriptions, vector<string>& filenames) {
    for (label_t label = 0; label < numLabels; label++) {
        auto fileDescription = filedescriptions[label];
        filenames[label] = inputDirectory + "/" + fileDescription.at("filename").get<string>();
    }
}

void GraphLoader::initPropertyKeyMapAndCalcNumBlocks(label_t numLabels, vector<string>& filenames,
    vector<uint64_t>& numBlocksPerLabel,
    vector<unordered_map<string, PropertyKey>>& propertyKeyMaps, const char tokenSeparator) {
    propertyKeyMaps.resize(numLabels);
    logger->info("Parsing headers and calculating number of blocks in a file.");
    for (label_t label = 0; label < numLabels; label++) {
        logger->debug("path=`{0}`", filenames[label]);
        ifstream f(filenames[label], ios_base::in);
        string header;
        getline(f, header);
        parseHeader(tokenSeparator, header, propertyKeyMaps[label]);
        f.seekg(0, ios_base::end);
        numBlocksPerLabel[label] = 1 + (f.tellg() / CSV_READING_BLOCK_SIZE);
    }
    logger->info("Done parsing headers.");
}

// Parses the header of a CSV file. The header contains the name and dataType of each column
// separated by a given `tokenSeparator`.
void GraphLoader::parseHeader(
    const char tokenSeparator, string& header, unordered_map<string, PropertyKey>& propertyKeyMap) {
    auto splittedHeader = make_unique<vector<string>>();
    size_t startPos = 0, endPos = 0;
    while ((endPos = header.find(tokenSeparator, startPos)) != string::npos) {
        splittedHeader->push_back(header.substr(startPos, endPos - startPos));
        startPos = endPos + 1;
    }
    splittedHeader->push_back(header.substr(startPos));
    auto propertyKeyNameSet = make_unique<unordered_set<string>>();
    uint32_t propertyIdx = 0;
    for (auto& split : *splittedHeader) {
        auto nameEndPos = split.find(":");
        if (nameEndPos == string::npos) {
            throw invalid_argument("Cannot find dataType in column head `" + split + "`.");
        }
        auto propertyKeyName = split.substr(0, split.find(":"));
        if (propertyKeyNameSet->find(propertyKeyName) != propertyKeyNameSet->end()) {
            throw invalid_argument("Same property name in csv file.");
        }
        propertyKeyNameSet->insert(propertyKeyName);
        auto dataType = getDataType(split.substr(split.find(":") + 1));
        if (NODE != dataType && LABEL != dataType) {
            propertyKeyMap.insert({propertyKeyName, PropertyKey{dataType, propertyIdx++}});
        }
    }
}

void GraphLoader::countLinesAndGetUnstrPropertyKeys(vector<vector<uint64_t>>& numLinesPerBlock,
    labelBlockUnstrPropertyKeys_t& labelBlockUnstrPropertyKeys, vector<uint64_t>& numBlocksPerLabel,
    const char tokenSeparator, vector<string>& fnames) {
    logger->info("Counting number of lines in each label.");
    auto numLabels = catalog.getNodeLabelsCount();
    for (label_t label = 0; label < numLabels; label++) {
        labelBlockUnstrPropertyKeys[label].resize(numBlocksPerLabel[label]);
        numLinesPerBlock[label].resize(numBlocksPerLabel[label]);
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            threadPool.execute(countLinesAndScanUnstrPropertiesInBlockTask, fnames[label],
                tokenSeparator, catalog.nodePropertyKeyMaps[label].size(),
                &labelBlockUnstrPropertyKeys[label][blockId], &numLinesPerBlock, label, blockId,
                logger);
        }
    }
    threadPool.wait();
    graph.numNodesPerLabel.resize(numLabels);
    for (label_t label = 0; label < numLabels; label++) {
        graph.numNodesPerLabel[label] = 0;
        numLinesPerBlock[label][0]--;
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            graph.numNodesPerLabel[label] += numLinesPerBlock[label][blockId];
        }
    }
    logger->info("Done counting lines.");
}

// Initializes node's unstructured propertyKeyMaps, similar to how the the structured
// propertyKeyMaps are initialized. The only difference is that the dataType of the propertyKey is
// UNKNOWN since it can vary from node to node.
void GraphLoader::initNodeUnstrPropertyKeyMaps(
    labelBlockUnstrPropertyKeys_t& labelBlockUnstrPropertyKeys) {
    logger->debug("Initialize node unstructured PropertyKey maps.");
    catalog.nodeUnstrPropertyKeyMaps.resize(catalog.getNodeLabelsCount());
    for (label_t label = 0; label < catalog.getNodeLabelsCount(); label++) {
        unordered_set<const char*, charArrayHasher, charArrayEqualTo> unionOfUnstrPropertyKeys;
        for (auto& unstrPropertyKeys : labelBlockUnstrPropertyKeys[label]) {
            for (auto& propertyKey : unstrPropertyKeys) {
                auto found = unionOfUnstrPropertyKeys.find(propertyKey);
                if (found != unionOfUnstrPropertyKeys.end()) {
                    delete[] * found;
                } else {
                    unionOfUnstrPropertyKeys.insert(propertyKey);
                }
            }
        }
        auto propertyIdx = 0u;
        for (auto& propertyKeyString : unionOfUnstrPropertyKeys) {
            catalog.nodeUnstrPropertyKeyMaps[label].emplace(
                string(propertyKeyString), PropertyKey(UNSTRUCTURED, propertyIdx++));
        }
    }
    logger->debug("Done initializing node unstructured PropertyKey maps.");
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
            auto unstrPropertyName = reader.getString();
            *strchr(unstrPropertyName, ':') = 0;
            if (unstrPropertyKeys->find(unstrPropertyName) == unstrPropertyKeys->end()) {
                auto len = strlen(unstrPropertyName);
                auto uPropertyCopy = new char[len + 1];
                memcpy(uPropertyCopy, unstrPropertyName, len);
                uPropertyCopy[len] = 0;
                unstrPropertyKeys->insert(uPropertyCopy);
            }
        }
    }
    logger->trace("End: path=`{0}` blkIdx={1}", fname, blockId);
}

} // namespace loader
} // namespace graphflow
