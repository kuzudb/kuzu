#include "src/loader/include/graph_loader.h"

#include <iostream>
#include <memory>
#include <unordered_set>

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"
#include "src/common/include/csv_reader/csv_reader.h"
#include "src/loader/include/nodes_loader.h"
#include "src/loader/include/rels_loader.h"

using namespace graphflow::storage;

namespace graphflow {
namespace loader {

GraphLoader::GraphLoader(
    const string& inputDirectory, const string& outputDirectory, uint32_t numThreads)
    : logger{spdlog::stdout_logger_mt("loader")}, threadPool{ThreadPool(numThreads)},
      inputDirectory(inputDirectory), outputDirectory(outputDirectory) {}

GraphLoader::~GraphLoader() {
    spdlog::drop("loader");
};

void GraphLoader::loadGraph() {
    logger->info("Starting GraphLoader.");
    auto metadata = readMetadata();

    graph.catalog = make_unique<Catalog>();

    // populate data fields in Catalog
    assignIdxToLabels(graph.catalog->stringToNodeLabelMap, metadata->at("nodeFileDescriptions"));
    assignIdxToLabels(graph.catalog->stringToRelLabelMap, metadata->at("relFileDescriptions"));
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
    graph.catalog->saveToFile(outputDirectory);
    logger->info("Writing Graph object.");
    graph.saveToFile(outputDirectory);

    logger->info("Done GraphLoader.");
}

unique_ptr<nlohmann::json> GraphLoader::readMetadata() {
    logger->info("Reading metadata and initilializing `Catalog`.");
    ifstream jsonFile(inputDirectory + "/metadata.json");
    if (!jsonFile.is_open()) {
        throw invalid_argument("Unable to open metadata.json");
    }
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
        graph.catalog->relLabelToCardinalityMap.push_back(
            getCardinality(descriptor.at("cardinality")));
    }
}

void GraphLoader::setSrcDstNodeLabelsForRelLabels(const nlohmann::json& metadata) {
    graph.catalog->dstNodeLabelToRelLabel.resize(graph.catalog->getNodeLabelsCount());
    graph.catalog->srcNodeLabelToRelLabel.resize(graph.catalog->getNodeLabelsCount());
    graph.catalog->relLabelToSrcNodeLabels.resize(graph.catalog->getRelLabelsCount());
    graph.catalog->relLabelToDstNodeLabels.resize(graph.catalog->getRelLabelsCount());
    auto relLabel = 0;
    for (auto& descriptor : metadata.at("relFileDescriptions")) {
        for (auto& nodeLabel : descriptor.at("srcNodeLabels")) {
            auto labelString = nodeLabel.get<string>();
            auto labelIdx = graph.catalog->stringToNodeLabelMap[labelString.c_str()];
            graph.catalog->srcNodeLabelToRelLabel[labelIdx].push_back(relLabel);
            graph.catalog->relLabelToSrcNodeLabels[relLabel].push_back(labelIdx);
        }
        for (auto& nodeLabel : descriptor.at("dstNodeLabels")) {
            auto labelString = nodeLabel.get<string>();
            auto labelIdx = graph.catalog->stringToNodeLabelMap[labelString.c_str()];
            graph.catalog->dstNodeLabelToRelLabel[labelIdx].push_back(relLabel);
            graph.catalog->relLabelToDstNodeLabels[relLabel].push_back(labelIdx);
        }
        relLabel++;
    }
}

// Enforce primary key constraint: 1) a primary key must be specified for a node label; 2) one
// node label can only contains one primary key; 3) data type of primary key must be INT32 or
// STRING.
void GraphLoader::checkNodePrimaryKeyConstraints() {
    for (auto& propertyKeyMap : graph.catalog->nodePropertyKeyMaps) {
        bool hasPrimaryKey = false;
        for (auto& property : propertyKeyMap) {
            if (property.second.isPrimaryKey) {
                if (hasPrimaryKey) {
                    throw invalid_argument(
                        "More than one primary key is specified for the same label.");
                } else if (property.second.dataType != INT32 &&
                           property.second.dataType != STRING) {
                    throw invalid_argument("The data type of the primary key cannot be " +
                                           dataTypeToString(property.second.dataType) +
                                           ", only INT32 and STRING are allowed.");
                } else {
                    hasPrimaryKey = true;
                }
            }
        }
        if (!hasPrimaryKey) {
            throw invalid_argument("A primary key is required for a node label.");
        }
    }
}

unique_ptr<vector<unique_ptr<NodeIDMap>>> GraphLoader::loadNodes(const nlohmann::json& metadata) {
    logger->info("Starting to load nodes.");

    // get the name of csv file for each node label
    vector<string> fnames(graph.catalog->getNodeLabelsCount());
    inferFnamesFromMetadataFileDesriptions(
        graph.catalog->getNodeLabelsCount(), metadata.at("nodeFileDescriptions"), fnames);

    // initialize node's propertyKey map and count number of blocks in each label's csv file
    vector<uint64_t> numBlocksPerLabel(graph.catalog->getNodeLabelsCount());
    initPropertyKeyMapAndCalcNumBlocks(graph.catalog->getNodeLabelsCount(), fnames,
        numBlocksPerLabel, graph.catalog->nodePropertyKeyMaps,
        metadata.at("tokenSeparator").get<string>()[0]);
    auto primaryKeys = getNodePrimaryKeysFromMetadata(
        graph.catalog->getNodeLabelsCount(), metadata.at("nodeFileDescriptions"));
    initNodePropertyPrimaryKeys(primaryKeys);
    checkNodePrimaryKeyConstraints();

    // count number of lines and get unstructured propertyKeys in each block of each label.
    labelBlockUnstrPropertyKeys_t labelBlockUnstrPropertyKeys(graph.catalog->getNodeLabelsCount());
    vector<vector<uint64_t>> numLinesPerBlock(graph.catalog->getNodeLabelsCount());
    countLinesAndGetUnstrPropertyKeys(numLinesPerBlock, labelBlockUnstrPropertyKeys,
        numBlocksPerLabel, metadata.at("tokenSeparator").get<string>()[0], fnames);

    // initialize unstructured property key map in Catalog.
    initNodeUnstrPropertyKeyMaps(labelBlockUnstrPropertyKeys);

    // create NodeIDMaps and call NodesLoader.
    auto nodeIDMaps =
        make_unique<vector<unique_ptr<NodeIDMap>>>(graph.catalog->getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < graph.catalog->getNodeLabelsCount(); nodeLabel++) {
        (*nodeIDMaps)[nodeLabel] = make_unique<NodeIDMap>(graph.numNodesPerLabel[nodeLabel]);
    }
    NodesLoader nodesLoader{threadPool, graph, metadata, outputDirectory};
    nodesLoader.load(fnames, numBlocksPerLabel, numLinesPerBlock, *nodeIDMaps);

    return nodeIDMaps;
}

void GraphLoader::loadRels(
    const nlohmann::json& metadata, vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    logger->info("Starting to load rels.");

    // get the name of csv file for each label
    vector<string> fnames(graph.catalog->getRelLabelsCount());
    inferFnamesFromMetadataFileDesriptions(
        graph.catalog->getRelLabelsCount(), metadata.at("relFileDescriptions"), fnames);

    // initialize rel's propertyKey map and count number of blocks in each label's csv file
    vector<uint64_t> numBlocksPerLabel(graph.catalog->getRelLabelsCount());
    initPropertyKeyMapAndCalcNumBlocks(graph.catalog->getRelLabelsCount(), fnames,
        numBlocksPerLabel, graph.catalog->relPropertyKeyMaps,
        metadata.at("tokenSeparator").get<string>()[0]);

    RelsLoader relsLoader{threadPool, graph, metadata, nodeIDMaps, outputDirectory};
    relsLoader.load(fnames, numBlocksPerLabel);
}

void GraphLoader::inferFnamesFromMetadataFileDesriptions(
    label_t numLabels, nlohmann::json filedescriptions, vector<string>& filenames) {
    for (label_t label = 0; label < numLabels; label++) {
        auto fileDescription = filedescriptions[label];
        filenames[label] =
            FileUtils::joinPath(inputDirectory, fileDescription.at("filename").get<string>());
    }
}

vector<string> GraphLoader::getNodePrimaryKeysFromMetadata(
    label_t numLabels, nlohmann::json fileDescriptions) {
    vector<string> primaryKeys(numLabels);
    for (label_t label = 0; label < numLabels; label++) {
        auto fileDescription = fileDescriptions[label];
        // Currently, we assume that there is only one primary key column for each node label.
        auto primaryKeyEntry = fileDescription.find("primaryKey");
        if (primaryKeyEntry != fileDescription.end()) {
            primaryKeys[label] = primaryKeyEntry->get<string>();
        }
    }
    return primaryKeys;
}

void GraphLoader::initPropertyKeyMapAndCalcNumBlocks(label_t numLabels, vector<string>& filenames,
    vector<uint64_t>& numBlocksPerLabel,
    vector<unordered_map<string, PropertyKey>>& propertyKeyMaps, const char tokenSeparator) {
    propertyKeyMaps.resize(numLabels);
    logger->info("Parsing headers and calculating number of blocks in a file.");
    for (label_t label = 0; label < numLabels; label++) {
        logger->debug("path=`{0}`", filenames[label]);
        ifstream f(filenames[label], ios_base::in);
        if (!f.is_open()) {
            throw invalid_argument("Unable to open file " + filenames[label]);
        }
        string header;
        getline(f, header);
        parseHeader(tokenSeparator, header, propertyKeyMaps[label]);
        f.seekg(0, ios_base::end);
        numBlocksPerLabel[label] = 1 + (f.tellg() / CSV_READING_BLOCK_SIZE);
    }
    logger->info("Done parsing headers.");
}

void GraphLoader::initNodePropertyPrimaryKeys(vector<string>& primaryKeys) {
    for (auto labelId = 0u; labelId < graph.catalog->getNodeLabelsCount(); labelId++) {
        auto primaryKeyName = primaryKeys[labelId];
        if (primaryKeyName.empty()) {
            throw invalid_argument("Primary key is empty or not specified.");
        }
        auto& propertyMap = graph.catalog->nodePropertyKeyMaps[labelId];
        auto propertyKeyEntry = propertyMap.find(primaryKeyName);
        if (propertyKeyEntry != propertyMap.end()) {
            propertyKeyEntry->second.isPrimaryKey = true;
        } else {
            throw invalid_argument("Specified primary key " + primaryKeyName + " is not found.");
        }
    }
}

// Parses the header of a CSV file. The header contains the name and dataType of each column
// separated by a given `tokenSeparator`.
void GraphLoader::parseHeader(
    const char tokenSeparator, string& header, unordered_map<string, PropertyKey>& propertyKeyMap) {
    auto propertyHeaders = StringUtils::split(header, string(1, tokenSeparator));
    uint32_t propertyIdx = 0;
    for (auto& propertyHeader : propertyHeaders) {
        auto propertyDescriptors = StringUtils::split(propertyHeader, PROPERTY_DATATYPE_SEPARATOR);
        if (propertyDescriptors.size() < 2) {
            throw invalid_argument("Cannot find dataType in column head `" + propertyHeader + "`.");
        }
        auto propertyKeyName = propertyDescriptors[0];
        if (propertyKeyMap.find(propertyKeyName) != propertyKeyMap.end()) {
            throw invalid_argument(
                "Property name " + propertyKeyName + " already exists in the node label.");
        }
        auto dataType = getDataType(propertyDescriptors[1]);
        if (NODE != dataType && LABEL != dataType) {
            propertyKeyMap.insert(
                {propertyKeyName, PropertyKey{dataType, propertyIdx++, false /* isPrimaryKey */}});
        }
    }
}

void GraphLoader::countLinesAndGetUnstrPropertyKeys(vector<vector<uint64_t>>& numLinesPerBlock,
    labelBlockUnstrPropertyKeys_t& labelBlockUnstrPropertyKeys, vector<uint64_t>& numBlocksPerLabel,
    const char tokenSeparator, vector<string>& fnames) {
    logger->info("Counting number of lines in each label.");
    auto numLabels = graph.catalog->getNodeLabelsCount();
    for (label_t label = 0; label < numLabels; label++) {
        labelBlockUnstrPropertyKeys[label].resize(numBlocksPerLabel[label]);
        numLinesPerBlock[label].resize(numBlocksPerLabel[label]);
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            threadPool.execute(countLinesAndScanUnstrPropertiesInBlockTask, fnames[label],
                tokenSeparator, graph.catalog->nodePropertyKeyMaps[label].size(),
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
// propertyKeyMaps are initialized. The only difference is that the dataType of the propertyKey
// is UNKNOWN since it can vary from node to node.
void GraphLoader::initNodeUnstrPropertyKeyMaps(
    labelBlockUnstrPropertyKeys_t& labelBlockUnstrPropertyKeys) {
    logger->debug("Initialize node unstructured PropertyKey maps.");
    graph.catalog->nodeUnstrPropertyKeyMaps.resize(graph.catalog->getNodeLabelsCount());
    for (label_t label = 0; label < graph.catalog->getNodeLabelsCount(); label++) {
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
            graph.catalog->nodeUnstrPropertyKeyMaps[label].emplace(string(propertyKeyString),
                PropertyKey(UNSTRUCTURED, propertyIdx++, false /* isPrimaryKey */));
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
