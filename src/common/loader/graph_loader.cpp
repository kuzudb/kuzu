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

GraphLoader::GraphLoader(string inputDirectory, string outputDirectory, uint32_t numThreads)
    : threadPool{ThreadPool(numThreads)}, inputDirectory(inputDirectory),
      outputDirectory(outputDirectory) {
    logger = spdlog::stderr_color_mt("GraphLoader");
    logger->info("Starting GraphLoader.");
}

void GraphLoader::loadGraph() {
    auto metadata = readMetadata();
    Catalog catalog{*metadata};
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

unique_ptr<vector<shared_ptr<NodeIDMap>>> GraphLoader::loadNodes(
    const nlohmann::json &metadata, Graph &graph, Catalog &catalog) {
    logger->info("Starting to load nodes.");
    vector<string> filenames(catalog.getNodeLabelsCount());
    vector<uint64_t> numBlocksPerLabel(catalog.getNodeLabelsCount());
    vector<vector<uint64_t>> numLinesPerBlock(catalog.getNodeLabelsCount());
    inferFilenamesInitPropertyMapAndCountLinesPerBlock(catalog.getNodeLabelsCount(),
        metadata.at("nodeFileDescriptions"), filenames, numBlocksPerLabel, catalog.nodePropertyMap,
        numLinesPerBlock, metadata.at("tokenSeparator").get<string>()[0], graph.numNodesPerLabel);
    auto nodeIDMaps = make_unique<vector<shared_ptr<NodeIDMap>>>();
    for (auto nodeLabel = 0u; nodeLabel < catalog.getNodeLabelsCount(); nodeLabel++) {
        (*nodeIDMaps).push_back(make_shared<NodeIDMap>(graph.numNodesPerLabel[nodeLabel]));
    }
    readNodePropertiesAndSerialize(catalog, metadata, filenames, catalog.nodePropertyMap,
        graph.numNodesPerLabel, numBlocksPerLabel, numLinesPerBlock, *nodeIDMaps);
    return nodeIDMaps;
}

void GraphLoader::readNodePropertiesAndSerialize(Catalog &catalog, const nlohmann::json &metadata,
    vector<string> &filenames, vector<vector<Property>> &propertyMaps,
    vector<uint64_t> &numNodesPerLabel, vector<uint64_t> &numBlocksPerLabel,
    vector<vector<uint64_t>> &numLinesPerBlock, vector<shared_ptr<NodeIDMap>> &nodeIDMappings) {
    logger->info("Starting to read and store node properties.");
    vector<shared_ptr<pair<unique_ptr<mutex>, vector<uint32_t>>>> files(
        catalog.getNodeLabelsCount());
    vector<gfNodeOffset_t> frontierOffsets(catalog.getNodeLabelsCount());
    for (gfLabel_t label = 0; label < catalog.getNodeLabelsCount(); label++) {
        files[label] = createFilesForNodeProperties(catalog, label, propertyMaps[label]);
        frontierOffsets[label] = 0;
    }
    auto maxNumBlocks = *max_element(begin(numBlocksPerLabel), end(numBlocksPerLabel));
    for (auto blockId = 0; blockId < maxNumBlocks; blockId++) {
        for (gfLabel_t label = 0; label < catalog.getNodeLabelsCount(); label++) {
            if (blockId < numBlocksPerLabel[label]) {
                threadPool.execute(LoaderTasks::nodePropertyReaderTask, filenames[label],
                    metadata.at("tokenSeparator").get<string>()[0], propertyMaps[label], blockId,
                    numLinesPerBlock[label][blockId], frontierOffsets[label], files[label],
                    nodeIDMappings[label], logger);
                frontierOffsets[label] += numLinesPerBlock[label][blockId];
            }
        }
    }
    threadPool.wait();
}

shared_ptr<pair<unique_ptr<mutex>, vector<uint32_t>>> GraphLoader::createFilesForNodeProperties(
    Catalog &catalog, gfLabel_t label, vector<Property> &propertyMap) {
    auto files = make_shared<pair<unique_ptr<mutex>, vector<uint32_t>>>();
    files->first = make_unique<mutex>();
    files->second.resize(propertyMap.size());
    for (auto i = 0u; i < propertyMap.size(); i++) {
        if (NODE == propertyMap[i].dataType) {
            continue;
        } else if (STRING == propertyMap[i].dataType) {
            logger->warn("Ignoring string properties.");
        } else {
            auto fname = NodePropertyStore::getColumnFname(
                outputDirectory, label, propertyMap[i].propertyName);
            files->second[i] = open(fname.c_str(), O_WRONLY | O_CREAT, 0666);
            if (-1u == files->second[i]) {
                invalid_argument("cannot create file: " + fname);
            }
        }
    }
    return files;
}

void GraphLoader::loadRels(const nlohmann::json &metadata, Graph &graph, Catalog &catalog,
    unique_ptr<vector<shared_ptr<NodeIDMap>>> nodeIDMaps) {
    logger->info("Starting to load rels.");
    vector<string> filenames(catalog.getRelLabelsCount());
    vector<uint64_t> numBlocksPerLabel(catalog.getRelLabelsCount());
    vector<vector<uint64_t>> numLinesPerBlock(catalog.getRelLabelsCount());
    inferFilenamesInitPropertyMapAndCountLinesPerBlock(catalog.getRelLabelsCount(),
        metadata.at("relFileDescriptions"), filenames, numBlocksPerLabel, catalog.relPropertyMap,
        numLinesPerBlock, metadata.at("tokenSeparator").get<string>()[0], graph.numRelsPerLabel);
    createSingleCardinalityAdjListsIndexes(graph, catalog,
        metadata.at("tokenSeparator").get<string>()[0], numBlocksPerLabel, numLinesPerBlock,
        filenames, *nodeIDMaps);
}

void GraphLoader::inferFilenamesInitPropertyMapAndCountLinesPerBlock(gfLabel_t numLabels,
    nlohmann::json filedescriptions, vector<string> &filenames, vector<uint64_t> &numBlocksPerLabel,
    vector<vector<Property>> &propertyMap, vector<vector<uint64_t>> &numLinesPerBlock,
    const char tokenSeparator, vector<uint64_t> &numPerLabel) {
    for (gfLabel_t label = 0; label < numLabels; label++) {
        auto fileDescription = filedescriptions[label];
        filenames[label] = inputDirectory + "/" + fileDescription.at("filename").get<string>();
    }
    initPropertyMapAndCalcNumBlocksPerLabel(
        numLabels, filenames, numBlocksPerLabel, propertyMap, tokenSeparator);
    countLinesPerBlockAndInitNumPerLabel(
        numLabels, numLinesPerBlock, numBlocksPerLabel, tokenSeparator, filenames);
    numPerLabel.resize(numLabels);
    for (gfLabel_t label = 0; label < numLabels; label++) {
        numPerLabel[label] = 0;
        numLinesPerBlock[label][0]--;
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            numPerLabel[label] += numLinesPerBlock[label][blockId];
        }
    }
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
    const char tokenSeparator, vector<string> &filenames) {
    logger->info("Counting number of (nodes/rels) in labels.");
    for (gfLabel_t label = 0; label < numLabels; label++) {
        numLinesPerBlock[label].resize(numBlocksPerLabel[label]);
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            threadPool.execute(LoaderTasks::fileBlockLinesCounterTask, filenames[label],
                tokenSeparator, &numLinesPerBlock, label, blockId, logger);
        }
    }
    threadPool.wait();
    logger->info("done.");
}

void GraphLoader::createSingleCardinalityAdjListsIndexes(Graph &graph, Catalog &catalog,
    const char tokenSeparator, vector<uint64_t> &numBlocksPerLabel,
    vector<vector<uint64_t>> &numLinesPerBlock, vector<string> &filenames,
    vector<shared_ptr<NodeIDMap>> &nodeIDMaps) {
    logger->info("Creating Single Cardinality Adjacency Lists Indexes.");
    for (auto relLabel = 0u; relLabel < catalog.getRelLabelsCount(); relLabel++) {
        vector<bool> isSingleCardinality(2);
        for (auto direction : DIRECTIONS) {
            isSingleCardinality[direction] = catalog.isSingleCaridinalityInDir(relLabel, direction);
        }
        if (isSingleCardinality[FORWARD] || isSingleCardinality[BACKWARD]) {
            auto inMemColAdjLists =
                make_shared<vector<shared_ptr<vector<unique_ptr<InMemColAdjList>>>>>(2);
            auto nodeLabels = make_shared<vector<vector<gfLabel_t>>>(2);
            auto srcDstNodeIDMaps = make_shared<vector<vector<shared_ptr<NodeIDMap>>>>(2);
            for (auto direction : DIRECTIONS) {
                (*nodeLabels)[direction] = catalog.getNodeLabelsForRelLabelDir(relLabel, direction);
                for (auto nodeLabel : (*nodeLabels)[direction]) {
                    (*srcDstNodeIDMaps)[direction].push_back(nodeIDMaps[nodeLabel]);
                }
            }
            for (auto direction : DIRECTIONS) {
                if (isSingleCardinality[direction]) {
                    (*inMemColAdjLists)[direction] = createInMemColAdjListsIndex(
                        graph, catalog, nodeLabels, relLabel, direction);
                }
            }
            auto finishedBlocksCounter = make_shared<atomic<int>>();
            for (auto blockId = 0u; blockId < numBlocksPerLabel[relLabel]; blockId++) {
                threadPool.execute(LoaderTasks::readColumnAdjListsTask, filenames[relLabel],
                    blockId, tokenSeparator, isSingleCardinality, nodeLabels, inMemColAdjLists,
                    srcDstNodeIDMaps, finishedBlocksCounter, numBlocksPerLabel[relLabel],
                    catalog.relPropertyMap[relLabel].size() > 2, logger);
            }
        }
        threadPool.wait();
    }
    logger->info("done.");
}

shared_ptr<vector<unique_ptr<InMemColAdjList>>> GraphLoader::createInMemColAdjListsIndex(
    Graph &graph, Catalog &catalog, shared_ptr<vector<vector<gfLabel_t>>> nodeLabels,
    gfLabel_t relLabel, Direction direction) {
    auto columnAdjListScheme = AdjListsIndexes::getColumnAdjListsIndexScheme(
        (*nodeLabels)[!direction], graph.numNodesPerLabel, catalog.getNodeLabelsCount());
    auto columns = make_shared<vector<unique_ptr<InMemColAdjList>>>(catalog.getNodeLabelsCount());
    for (auto boundNodeLabel : (*nodeLabels)[direction]) {
        auto fname = AdjListsIndexes::getColumnAdjListIndexFname(
            outputDirectory, relLabel, boundNodeLabel, direction);
        (*columns)[boundNodeLabel] =
            make_unique<InMemColAdjList>(fname, graph.numNodesPerLabel[boundNodeLabel],
                columnAdjListScheme.first, columnAdjListScheme.second);
    }
    return columns;
}

} // namespace common
} // namespace graphflow
