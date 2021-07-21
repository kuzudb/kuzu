#include "src/loader/include/graph_loader.h"

#include <exception>
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

GraphLoader::GraphLoader(string inputDirectory, string outputDirectory, uint32_t numThreads)
    : logger{spdlog::stdout_logger_mt("loader")}, threadPool{ThreadPool(numThreads)},
      inputDirectory(std::move(inputDirectory)),
      outputDirectory(std::move(outputDirectory)), tokenSeparator{DEFAULT_TOKEN_SEPARATOR} {}

GraphLoader::~GraphLoader() {
    spdlog::drop("loader");
}

void GraphLoader::loadGraph() {
    try {
        logger->info("Starting GraphLoader.");
        vector<NodeFileDescription> nodeFileDescriptions;
        vector<RelFileDescription> relFileDescriptions;
        try {
            readAndParseMetadata(nodeFileDescriptions, relFileDescriptions);
        } catch (exception& e) {
            throw invalid_argument("Metadata JSON file parse error: " + string(e.what()));
        }
        graph.catalog = make_unique<Catalog>();

        auto nodeIDMaps = loadNodes(nodeFileDescriptions);

        logger->info("Creating reverse NodeIDMaps.");
        for (auto& nodeIDMap : *nodeIDMaps) {
            threadPool.execute(
                [&](NodeIDMap* x) { x->createNodeIDToOffsetMap(); }, nodeIDMap.get());
        }
        threadPool.wait();
        logger->info("Done creating reverse NodeIDMaps.");

        loadRels(relFileDescriptions, *nodeIDMaps);

        // write catalog and graph objects to file
        logger->info("Writing Catalog object.");
        graph.catalog->saveToFile(outputDirectory);
        logger->info("Writing Graph object.");
        graph.saveToFile(outputDirectory);

        logger->info("Done GraphLoader.");
    } catch (exception& e) {
        logger->info("Inside GraphLoader::loadGraph()");
        logger->error(e.what());
        throw e;
    }
}

void GraphLoader::readAndParseMetadata(vector<NodeFileDescription>& nodeFileDescriptions,
    vector<RelFileDescription>& relFileDescriptions) {
    logger->info("Reading and parsing metadata from metadata.json.");
    auto metadataJSONPath = FileUtils::joinPath(inputDirectory, DEFAULT_METADATA_JSON_FILENAME);
    ifstream jsonFile(metadataJSONPath);
    auto parsedJson = make_unique<nlohmann::json>();
    jsonFile >> *parsedJson;
    this->tokenSeparator = parsedJson->at("tokenSeparator").get<string>()[0];
    auto parsedNodeFileDescriptions = parsedJson->at("nodeFileDescriptions");
    for (auto& parsedNodeFileDescription : parsedNodeFileDescriptions) {
        auto filename = parsedNodeFileDescription.at("filename").get<string>();
        nodeFileDescriptions.emplace_back(FileUtils::joinPath(inputDirectory, filename),
            parsedNodeFileDescription.at("label").get<string>(),
            parsedNodeFileDescription.at("primaryKey").get<string>());
    }
    auto parsedRelFileDescriptions = parsedJson->at("relFileDescriptions");
    for (auto& parsedRelFileDescription : parsedRelFileDescriptions) {
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
            dstNodeLabelNames);
    }
    logger->info("Done reading and parsing metadata from metadata.json.");
}

void GraphLoader::readCSVHeaderAndCalcNumBlocks(const vector<string>& filePaths,
    vector<uint64_t>& numBlocksPerLabel, vector<string>& fileHeaders) {
    logger->info("Parsing csv headers and calculating number of blocks in files.");
    string fileHeader;
    for (auto i = 0u; i < filePaths.size(); i++) {
        ifstream inf(filePaths[i], ios_base::in);
        if (!inf.is_open()) {
            throw invalid_argument("Cannot open file " + filePaths[i] + ".");
        }
        do {
            getline(inf, fileHeader);
        } while (fileHeader.empty() || fileHeader.at(0) == CSVReader::COMMENT_LINE_CHAR);
        fileHeaders[i] = fileHeader;
        inf.seekg(0, ios_base::end);
        numBlocksPerLabel[i] = 1 + (inf.tellg() / CSV_READING_BLOCK_SIZE);
    }
}

// Add node and rel labels into graph catalog.
// Note that inside this function, only structured properties of node labels are parsed and added
// into the catalog.
void GraphLoader::addNodeLabelsIntoGraphCatalog(
    const vector<NodeFileDescription>& fileDescriptions, vector<string>& fileHeaders) {
    for (auto i = 0u; i < fileDescriptions.size(); i++) {
        auto propertyDefinitions = parseHeader(fileHeaders[i]);
        graph.catalog->addNodeLabel(fileDescriptions[i].labelName, move(propertyDefinitions),
            fileDescriptions[i].primaryKeyPropertyName);
    }
}

void GraphLoader::addRelLabelsIntoGraphCatalog(
    const vector<RelFileDescription>& fileDescriptions, vector<string>& fileHeaders) {
    for (auto i = 0u; i < fileDescriptions.size(); i++) {
        auto propertyDefinitions = parseHeader(fileHeaders[i]);
        graph.catalog->addRelLabel(fileDescriptions[i].labelName,
            getRelMultiplicity(fileDescriptions[i].relMultiplicity), move(propertyDefinitions),
            fileDescriptions[i].srcNodeLabelNames, fileDescriptions[i].dstNodeLabelNames);
    }
}

// Parses the header of a CSV file. The header contains the name and dataType of each structured
// property separated by a given `tokenSeparator`.
vector<PropertyDefinition> GraphLoader::parseHeader(string& header) const {
    auto propertyHeaders = StringUtils::split(header, string(1, tokenSeparator));
    unordered_set<string> propertyNameSet;
    vector<PropertyDefinition> propertyDefinitions;
    uint64_t propertyId = 0;
    for (auto& propertyHeader : propertyHeaders) {
        auto propertyDescriptors = StringUtils::split(propertyHeader, PROPERTY_DATATYPE_SEPARATOR);
        if (propertyDescriptors.size() < 2) {
            throw invalid_argument("Cannot find dataType in column head `" + propertyHeader + "`.");
        }
        auto propertyName = propertyDescriptors[0];
        if (propertyNameSet.find(propertyName) != propertyNameSet.end()) {
            throw invalid_argument(
                "Property name " + propertyName + " already exists in the node label.");
        }
        propertyNameSet.insert(propertyName);
        auto dataType = TypeUtils::getDataType(propertyDescriptors[1]);
        if (NODE != dataType && LABEL != dataType) {
            propertyDefinitions.emplace_back(propertyName, propertyId++, dataType);
        }
    }
    return propertyDefinitions;
}

unique_ptr<vector<unique_ptr<NodeIDMap>>> GraphLoader::loadNodes(
    const vector<NodeFileDescription>& nodeFileDescriptions) {
    logger->info("Starting to load nodes.");

    // initialize node's propertyKey map and count number of blocks in each label's csv file
    vector<uint64_t> numBlocksPerLabel(nodeFileDescriptions.size());
    vector<string> fileHeaderPerLabel(nodeFileDescriptions.size());
    vector<string> filePaths(nodeFileDescriptions.size());
    for (auto i = 0u; i < nodeFileDescriptions.size(); i++) {
        filePaths[i] = nodeFileDescriptions[i].filePath;
    }
    readCSVHeaderAndCalcNumBlocks(filePaths, numBlocksPerLabel, fileHeaderPerLabel);
    addNodeLabelsIntoGraphCatalog(nodeFileDescriptions, fileHeaderPerLabel);

    // count number of lines and get unstructured propertyKeys in each block of each label.
    vector<vector<uint64_t>> numLinesPerBlock(graph.catalog->getNodeLabelsCount());
    vector<vector<unordered_set<string>>> labelBlockUnstrProperties(
        graph.catalog->getNodeLabelsCount());
    countLinesAndAddUnstrPropertiesInCatalog(
        numLinesPerBlock, labelBlockUnstrProperties, numBlocksPerLabel, filePaths);

    // create NodeIDMaps and call NodesLoader.
    auto nodeIDMaps =
        make_unique<vector<unique_ptr<NodeIDMap>>>(graph.catalog->getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < graph.catalog->getNodeLabelsCount(); nodeLabel++) {
        (*nodeIDMaps)[nodeLabel] = make_unique<NodeIDMap>(graph.numNodesPerLabel[nodeLabel]);
    }
    NodesLoader nodesLoader{threadPool, graph, outputDirectory, tokenSeparator};
    nodesLoader.load(filePaths, numBlocksPerLabel, numLinesPerBlock, *nodeIDMaps);

    logger->info("Done loading nodes.");
    return nodeIDMaps;
}

void GraphLoader::loadRels(const vector<RelFileDescription>& relFileDescriptions,
    vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    logger->info("Starting to load rels.");

    vector<uint64_t> numBlocksPerLabel(relFileDescriptions.size());
    vector<string> fileHeaderPerLabel(relFileDescriptions.size());
    vector<string> filePaths(relFileDescriptions.size());
    for (auto i = 0u; i < relFileDescriptions.size(); i++) {
        filePaths[i] = relFileDescriptions[i].filePath;
    }
    readCSVHeaderAndCalcNumBlocks(filePaths, numBlocksPerLabel, fileHeaderPerLabel);
    addRelLabelsIntoGraphCatalog(relFileDescriptions, fileHeaderPerLabel);

    RelsLoader relsLoader{threadPool, graph, outputDirectory, tokenSeparator, nodeIDMaps};
    relsLoader.load(filePaths, numBlocksPerLabel);
    logger->info("Done loading rels.");
}

void GraphLoader::countLinesAndAddUnstrPropertiesInCatalog(
    vector<vector<uint64_t>>& numLinesPerBlock,
    vector<vector<unordered_set<string>>>& labelBlockUnstrProperties,
    vector<uint64_t>& numBlocksPerLabel, const vector<string>& filePaths) {
    logger->info("Counting number of lines in each label.");
    auto numLabels = graph.catalog->getNodeLabelsCount();
    for (label_t labelId = 0; labelId < numLabels; labelId++) {
        auto& labelUnstrProperties = labelBlockUnstrProperties[labelId];
        labelUnstrProperties.resize(numBlocksPerLabel[labelId]);
        numLinesPerBlock[labelId].resize(numBlocksPerLabel[labelId]);
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[labelId]; blockId++) {
            threadPool.execute(countLinesAndScanUnstrPropertiesInBlockTask, filePaths[labelId],
                tokenSeparator, graph.catalog->getStructuredNodeProperties(labelId).size(),
                &labelUnstrProperties[blockId], &numLinesPerBlock, labelId, blockId, logger);
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
    logger->info("Done counting number of lines in each label.");
    for (label_t labelId = 0; labelId < numLabels; labelId++) {
        for (auto& unstrProperties : labelBlockUnstrProperties[labelId]) {
            for (auto& propertyName : unstrProperties) {
                graph.catalog->addNodeUnstrProperty(labelId, propertyName);
            }
        }
    }
    logger->info("unstrProperties added into the catalog.");
}

void GraphLoader::countLinesAndScanUnstrPropertiesInBlockTask(const string& fName,
    char tokenSeparator, uint32_t numStructuredProperties,
    unordered_set<string>* unstrPropertyNameSet, vector<vector<uint64_t>>* numLinesPerBlock,
    label_t label, uint32_t blockId, const shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: path=`{0}` blkIdx={1}", fName, blockId);
    CSVReader reader(fName, tokenSeparator, blockId);
    (*numLinesPerBlock)[label][blockId] = 0ull;
    while (reader.hasNextLine()) {
        (*numLinesPerBlock)[label][blockId]++;
        for (auto i = 0u; i < numStructuredProperties; ++i) {
            reader.hasNextToken();
        }
        while (reader.hasNextToken()) {
            auto unstrPropertyStr = reader.getString();
            auto unstrPropertyName =
                StringUtils::split(unstrPropertyStr, UNSTR_PROPERTY_SEPARATOR)[0];
            if (unstrPropertyNameSet->find(unstrPropertyName) == unstrPropertyNameSet->end()) {
                unstrPropertyNameSet->insert(unstrPropertyName);
            }
        }
    }
    logger->trace("End: path=`{0}` blkIdx={1}", fName, blockId);
}

} // namespace loader
} // namespace graphflow
