#include "src/loader/include/graph_loader.h"

#include <iostream>
#include <memory>
#include <unordered_set>

#include "src/common/include/configs.h"
#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/include/exception.h"
#include "src/loader/include/nodes_loader.h"
#include "src/loader/include/rels_loader.h"

using namespace std::chrono;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

GraphLoader::GraphLoader(string inputDirectory, string outputDirectory, uint32_t numThreads)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")},
      inputDirectory(std::move(inputDirectory)), outputDirectory(std::move(outputDirectory)) {
    taskScheduler = make_unique<TaskScheduler>(numThreads);
}

GraphLoader::~GraphLoader() {
    spdlog::drop("loader");
}

void GraphLoader::loadGraph() {
    FileUtils::createDir(outputDirectory);
    TimeMetric timer(true);
    timer.start();
    try {
        logger->info("Starting GraphLoader.");
        try {
            readAndParseMetadata(datasetMetadata);
        } catch (exception& e) {
            throw invalid_argument("Metadata JSON file parse error: " + string(e.what()));
        }
        progressBar = make_unique<LoaderProgressBar>();
        graph.catalog = make_unique<Catalog>();

        auto nodeIDMaps = loadNodes();

        progressBar->addAndStartNewJob("Constructing reverse nodeIDMaps", nodeIDMaps->size());
        for (auto& nodeIDMap : *nodeIDMaps) {
            taskScheduler->scheduleTask(LoaderTaskFactory::createLoaderTask(
                [&](NodeIDMap* x, LoaderProgressBar* progressBar) {
                    x->createNodeIDToOffsetMap();
                    progressBar->incrementTaskFinished();
                },
                nodeIDMap.get(), progressBar.get()));
        }
        taskScheduler->waitAllTasksToCompleteOrError();

        loadRels(*nodeIDMaps);

        // write catalog and graph objects to file
        progressBar->addAndStartNewJob("Writing Catalog and Graph objects", 1);
        graph.catalog->saveToFile(outputDirectory);
        graph.saveToFile(outputDirectory);
        progressBar->incrementTaskFinished();
        timer.stop();
        progressBar->clearLastFinishedLine();
        logger->info("Done GraphLoader.");
        logger->info("Time taken: . " + to_string(timer.getElapsedTimeMS() / 1000.0) + " seconds.");
    } catch (exception& e) {
        logger->error("Encountered an error while loading graph: {}", e.what());
        logger->info("Last Loader Job:");
        progressBar->printProgressInfoInGreenFont();
        progressBar->setDefaultFont();
        logger->info("Stopping GraphLoader.");
        cleanup();
        throw LoaderException(e.what());
    }
}

void GraphLoader::cleanup() {
    logger->info("Cleaning up.");
    FileUtils::removeDir(outputDirectory);
}

void GraphLoader::readAndParseMetadata(DatasetMetadata& metadata) {
    logger->info("Reading and parsing metadata from metadata.json.");
    auto metadataJSONPath =
        FileUtils::joinPath(inputDirectory, LoaderConfig::DEFAULT_METADATA_JSON_FILENAME);
    ifstream jsonFile(metadataJSONPath);
    auto parsedJson = make_unique<nlohmann::json>();
    jsonFile >> *parsedJson;
    metadata.parseJson(parsedJson, inputDirectory);
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
        } while (fileHeader.empty() || fileHeader.at(0) == LoaderConfig::COMMENT_LINE_CHAR);
        fileHeaders[i] = fileHeader;
        inf.seekg(0, ios_base::end);
        numBlocksPerLabel[i] = 1 + (inf.tellg() / LoaderConfig::CSV_READING_BLOCK_SIZE);
    }
}

void GraphLoader::verifyColHeaderDefinitionsForNodeFile(
    vector<PropertyDefinition>& colHeaderDefinitions, const NodeFileDescription& fileDescription) {
    bool hasID_FIELD = false;
    for (auto& colHeaderDefinition : colHeaderDefinitions) {
        auto name = colHeaderDefinition.name;
        if (name == LoaderConfig::START_ID_FIELD || name == LoaderConfig::START_ID_LABEL_FIELD ||
            name == LoaderConfig::START_ID_FIELD || name == LoaderConfig::END_ID_LABEL_FIELD) {
            throw invalid_argument(
                "Column headers definitions of a node file contains a mandatory field `" + name +
                "` that is not allowed.");
        }
        if (name == LoaderConfig::ID_FIELD) {
            colHeaderDefinition.dataType = fileDescription.IDType;
            colHeaderDefinition.isPrimaryKey = true;
            hasID_FIELD = true;
        }
    }
    if (!hasID_FIELD) {
        throw invalid_argument(
            "Column header definitions of a node file does not contains the mandatory field `" +
            string(LoaderConfig::ID_FIELD) + "`.");
    }
}

// Add node and rel labels into graph catalog.
// Note that inside this function, only structured properties of node labels are parsed and added
// into the catalog.
void GraphLoader::addNodeLabelsIntoGraphCatalog(
    const vector<NodeFileDescription>& fileDescriptions, vector<string>& fileHeaders) {
    for (auto i = 0u; i < fileDescriptions.size(); i++) {
        auto colHeaderDefinitions =
            parseCSVFileHeader(fileHeaders[i], fileDescriptions[i].csvReaderConfig.tokenSeparator);
        verifyColHeaderDefinitionsForNodeFile(colHeaderDefinitions, fileDescriptions[i]);
        // register the node label and its structured properties in the catalog.
        graph.catalog->addNodeLabel(fileDescriptions[i].labelName, move(colHeaderDefinitions));
    }
}

void GraphLoader::verifyColHeaderDefinitionsForRelFile(
    vector<PropertyDefinition>& colHeaderDefinitions) {
    auto numMandatoryFields = 0;
    for (auto& colHeaderDefinition : colHeaderDefinitions) {
        auto name = colHeaderDefinition.name;
        if (name == LoaderConfig::START_ID_FIELD || name == LoaderConfig::START_ID_LABEL_FIELD ||
            name == LoaderConfig::END_ID_FIELD || name == LoaderConfig::END_ID_LABEL_FIELD) {
            numMandatoryFields++;
        }
        if (name == LoaderConfig::ID_FIELD) {
            throw invalid_argument("Column header definitions of a rel file cannot contain "
                                   "the mandatory field `ID`.");
        }
    }
    if (numMandatoryFields != 4) {
        throw invalid_argument("Column header definitions of a rel file does not contains all "
                               "the mandatory field.");
    }
}

void GraphLoader::addRelLabelsIntoGraphCatalog(
    const vector<RelFileDescription>& fileDescriptions, vector<string>& fileHeaders) {
    for (auto i = 0u; i < fileDescriptions.size(); i++) {
        auto colHeaderDefinitions = parseCSVFileHeader(
            fileHeaders[i], datasetMetadata.relFileDescriptions[i].csvReaderConfig.tokenSeparator);
        verifyColHeaderDefinitionsForRelFile(colHeaderDefinitions);
        graph.catalog->addRelLabel(fileDescriptions[i].labelName,
            getRelMultiplicity(fileDescriptions[i].relMultiplicity), move(colHeaderDefinitions),
            fileDescriptions[i].srcNodeLabelNames, fileDescriptions[i].dstNodeLabelNames);
    }
}

// Parses CSV file header. This header contains 1) column headers for mandatory fields that is
// required in the file and 2) column headers for structured properties in the file.
vector<PropertyDefinition> GraphLoader::parseCSVFileHeader(string& header, char tokenSeparator) {
    auto colHeaders = StringUtils::split(header, string(1, tokenSeparator));
    unordered_set<string> columnNameSet;
    vector<PropertyDefinition> colHeaderDefinitions;
    for (auto& colHeader : colHeaders) {
        auto colHeaderComponents =
            StringUtils::split(colHeader, LoaderConfig::PROPERTY_DATATYPE_SEPARATOR);
        if (colHeaderComponents.size() < 2) {
            throw invalid_argument("Incomplete column header `" + colHeader + "`.");
        }
        PropertyDefinition colHeaderDefinition;
        // Check each one of the mandatory field column header.
        if (colHeaderComponents[0].empty()) {
            colHeaderDefinition.name = colHeaderComponents[1];
            if (colHeaderDefinition.name == LoaderConfig::ID_FIELD) {
                colHeaderDefinition.dataType.typeID = NODE;
                colHeaderDefinition.isPrimaryKey = true;
            } else if (colHeaderDefinition.name == LoaderConfig::START_ID_FIELD ||
                       colHeaderDefinition.name == LoaderConfig::END_ID_FIELD) {
                colHeaderDefinition.dataType.typeID = NODE;
            } else if (colHeaderDefinition.name == LoaderConfig::START_ID_LABEL_FIELD ||
                       colHeaderDefinition.name == LoaderConfig::END_ID_LABEL_FIELD) {
                colHeaderDefinition.dataType.typeID = LABEL;
            } else {
                throw invalid_argument(
                    "Invalid mandatory field column header `" + colHeaderDefinition.name + "`.");
            }
        } else {
            colHeaderDefinition.name = colHeaderComponents[0];
            colHeaderDefinition.dataType = Types::dataTypeFromString(colHeaderComponents[1]);
            if (colHeaderDefinition.dataType.typeID == NODE ||
                colHeaderDefinition.dataType.typeID == LABEL) {
                throw invalid_argument(
                    "Property column header cannot be of system types NODE or LABEL.");
            }
        }
        if (columnNameSet.find(colHeaderDefinition.name) != columnNameSet.end()) {
            throw invalid_argument("Column " + colHeaderDefinition.name +
                                   " already appears previously in the column headers.");
        }
        columnNameSet.insert(colHeaderDefinition.name);
        colHeaderDefinitions.push_back(colHeaderDefinition);
    }
    return colHeaderDefinitions;
}

unique_ptr<vector<unique_ptr<NodeIDMap>>> GraphLoader::loadNodes() {
    logger->info("Starting to load nodes.");
    auto& nodeFileDescriptions = datasetMetadata.nodeFileDescriptions;
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
    logger->info("Begin constructing nodeIDMaps. Number of nodeLabelsCount to add: {}",
        graph.catalog->getNodeLabelsCount());
    for (auto nodeLabel = 0u; nodeLabel < graph.catalog->getNodeLabelsCount(); nodeLabel++) {
        (*nodeIDMaps)[nodeLabel] = make_unique<NodeIDMap>(graph.numNodesPerLabel[nodeLabel]);
    }
    logger->info("End constructing nodeIDMaps.");
    NodesLoader nodesLoader{*taskScheduler, graph, outputDirectory,
        datasetMetadata.nodeFileDescriptions, progressBar.get()};
    nodesLoader.load(filePaths, numBlocksPerLabel, numLinesPerBlock, *nodeIDMaps);
    return nodeIDMaps;
}

void GraphLoader::loadRels(vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    logger->info("Starting to load rels.");
    auto& relFileDescriptions = datasetMetadata.relFileDescriptions;
    vector<uint64_t> numBlocksPerLabel(relFileDescriptions.size());
    vector<string> fileHeaderPerLabel(relFileDescriptions.size());
    vector<string> filePaths(relFileDescriptions.size());
    for (auto i = 0u; i < relFileDescriptions.size(); i++) {
        filePaths[i] = relFileDescriptions[i].filePath;
    }
    readCSVHeaderAndCalcNumBlocks(filePaths, numBlocksPerLabel, fileHeaderPerLabel);
    addRelLabelsIntoGraphCatalog(relFileDescriptions, fileHeaderPerLabel);
    RelsLoader relsLoader{*taskScheduler, graph, outputDirectory, nodeIDMaps,
        datasetMetadata.relFileDescriptions, progressBar.get()};
    relsLoader.load(numBlocksPerLabel);
}

void GraphLoader::countLinesAndAddUnstrPropertiesInCatalog(
    vector<vector<uint64_t>>& numLinesPerBlock,
    vector<vector<unordered_set<string>>>& labelBlockUnstrProperties,
    vector<uint64_t>& numBlocksPerLabel, const vector<string>& filePaths) {
    auto numLabels = graph.catalog->getNodeLabelsCount();
    for (label_t labelId = 0; labelId < numLabels; labelId++) {
        auto& labelUnstrProperties = labelBlockUnstrProperties[labelId];
        labelUnstrProperties.resize(numBlocksPerLabel[labelId]);
        numLinesPerBlock[labelId].resize(numBlocksPerLabel[labelId]);
        progressBar->addAndStartNewJob(
            "Counting lines in the file for node: " + graph.catalog->getNodeLabelName(labelId),
            numBlocksPerLabel[labelId]);
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[labelId]; blockId++) {
            taskScheduler->scheduleTask(LoaderTaskFactory::createLoaderTask(
                countLinesAndScanUnstrPropertiesInBlockTask, filePaths[labelId],
                datasetMetadata.nodeFileDescriptions[labelId].csvReaderConfig,
                graph.catalog->getStructuredNodeProperties(labelId).size(),
                &labelUnstrProperties[blockId], &numLinesPerBlock, labelId, blockId, logger,
                progressBar.get()));
        }
        taskScheduler->waitAllTasksToCompleteOrError();
    }
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
    const CSVReaderConfig& csvReaderConfig, uint32_t numStructuredProperties,
    unordered_set<string>* unstrPropertyNameSet, vector<vector<uint64_t>>* numLinesPerBlock,
    label_t label, uint32_t blockId, const shared_ptr<spdlog::logger>& logger,
    LoaderProgressBar* progressBar) {
    logger->trace("Start: path=`{0}` blkIdx={1}", fName, blockId);
    CSVReader reader(fName, csvReaderConfig, blockId);
    (*numLinesPerBlock)[label][blockId] = 0ull;
    while (reader.hasNextLine()) {
        (*numLinesPerBlock)[label][blockId]++;
        for (auto i = 0u; i < numStructuredProperties; ++i) {
            reader.hasNextToken();
        }
        while (reader.hasNextToken()) {
            auto unstrPropertyStr = reader.getString();
            auto unstrPropertyName =
                StringUtils::split(unstrPropertyStr, LoaderConfig::UNSTR_PROPERTY_SEPARATOR)[0];
            if (unstrPropertyNameSet->find(unstrPropertyName) == unstrPropertyNameSet->end()) {
                unstrPropertyNameSet->insert(unstrPropertyName);
            }
        }
    }
    logger->trace("End: path=`{0}` blkIdx={1}", fName, blockId);
    progressBar->incrementTaskFinished();
}

} // namespace loader
} // namespace graphflow
