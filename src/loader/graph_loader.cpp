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
      inputDirectory(std::move(inputDirectory)),
      outputDirectory(std::move(outputDirectory)), catalog{nullptr} {
    taskScheduler = make_unique<TaskScheduler>(numThreads);
    catalog = make_unique<Catalog>();
    auto bufferManager = make_unique<BufferManager>();
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
        readAndParseMetadata(datasetMetadata);
        progressBar = make_unique<LoaderProgressBar>();

        auto nodeIDMaps = loadNodes();

        progressBar->addAndStartNewJob("Constructing reverse nodeIDMaps", nodeIDMaps->size());
        for (auto& nodeIDMap : *nodeIDMaps) {
            taskScheduler->scheduleTask(LoaderTaskFactory::createLoaderTask(
                [&](NodeIDMap* x, LoaderProgressBar* progressBar_) {
                    x->createNodeIDToOffsetMap();
                    progressBar_->incrementTaskFinished();
                },
                nodeIDMap.get(), progressBar.get()));
        }
        taskScheduler->waitAllTasksToCompleteOrError();

        loadRels(move(nodeIDMaps));

        progressBar->addAndStartNewJob("Writing Catalog object to file", 1);
        catalog->saveToFile(outputDirectory);
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
    try {
        auto metadataJSONPath =
            FileUtils::joinPath(inputDirectory, LoaderConfig::DEFAULT_METADATA_JSON_FILENAME);
        ifstream jsonFile(metadataJSONPath);
        auto parsedJson = make_unique<nlohmann::json>();
        jsonFile >> *parsedJson;
        metadata.parseJson(*parsedJson, inputDirectory);
    } catch (exception& e) {
        throw LoaderException("Metadata JSON file parse error: " + string(e.what()));
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
            throw LoaderException("Cannot open file " + filePaths[i] + ".");
        }
        do {
            getline(inf, fileHeader);
        } while (fileHeader.empty() || fileHeader.at(0) == LoaderConfig::COMMENT_LINE_CHAR);
        fileHeaders[i] = fileHeader;
        inf.seekg(0, ios_base::end);
        numBlocksPerLabel[i] = 1 + (inf.tellg() / LoaderConfig::CSV_READING_BLOCK_SIZE);
    }
}

// Add node and rel labels into storageManager catalog.
// Note that inside this function, only structured properties of node labels are parsed and added
// into the catalog.
void GraphLoader::addNodeLabelsIntoGraphCatalog(
    const vector<NodeFileDescription>& fileDescriptions, vector<string>& fileHeaders) {
    for (auto i = 0u; i < fileDescriptions.size(); i++) {
        auto colHeaderDefinitions = parseCSVFileHeader(fileHeaders[i], fileDescriptions[i]);
        // Register the node label and its structured properties in the catalog.
        catalog->addNodeLabel(fileDescriptions[i].labelName, move(colHeaderDefinitions));
    }
}

void GraphLoader::addRelLabelsIntoGraphCatalog(
    const vector<RelFileDescription>& fileDescriptions, vector<string>& fileHeaders) {
    for (auto i = 0u; i < fileDescriptions.size(); i++) {
        auto colHeaderDefinitions =
            parseCSVFileHeader(fileHeaders[i], datasetMetadata.getRelFileDescription(i));
        catalog->addRelLabel(fileDescriptions[i].labelName,
            getRelMultiplicityFromString(fileDescriptions[i].relMultiplicity),
            move(colHeaderDefinitions), fileDescriptions[i].srcNodeLabelNames,
            fileDescriptions[i].dstNodeLabelNames);
    }
}

// Parses CSV file header. This header contains 1) column headers for mandatory fields that is
// required in the file and 2) column headers for structured properties in the file.
vector<PropertyDefinition> GraphLoader::parseCSVFileHeader(
    string& header, const LabelFileDescription& fileDescription) {
    auto colHeaders =
        StringUtils::split(header, string(1, fileDescription.csvReaderConfig.tokenSeparator));
    unordered_set<string> columnNameSet;
    vector<PropertyDefinition> colHeaderDefinitions;
    for (auto& colHeader : colHeaders) {
        auto colHeaderComponents =
            StringUtils::split(colHeader, LoaderConfig::PROPERTY_DATATYPE_SEPARATOR);
        if (colHeaderComponents.size() < 2) {
            throw LoaderException("Incomplete column header '" + colHeader + "'.");
        }
        PropertyDefinition colHeaderDefinition;
        // Check each one of the mandatory field column header.
        if (colHeaderComponents[0].empty()) {
            colHeaderDefinition.name = colHeaderComponents[1];
            if (colHeaderDefinition.name == LoaderConfig::ID_FIELD) {
                colHeaderDefinition.dataType = ((NodeFileDescription&)fileDescription).IDType;
                colHeaderDefinition.isPrimaryKey = true;
            } else if (colHeaderDefinition.name == LoaderConfig::START_ID_FIELD ||
                       colHeaderDefinition.name == LoaderConfig::END_ID_FIELD) {
                colHeaderDefinition.dataType.typeID = NODE;
            } else if (colHeaderDefinition.name == LoaderConfig::START_ID_LABEL_FIELD ||
                       colHeaderDefinition.name == LoaderConfig::END_ID_LABEL_FIELD) {
                colHeaderDefinition.dataType.typeID = LABEL;
            } else {
                throw LoaderException(
                    "Invalid mandatory field column header '" + colHeaderDefinition.name + "'.");
            }
        } else {
            colHeaderDefinition.name = colHeaderComponents[0];
            colHeaderDefinition.dataType = Types::dataTypeFromString(colHeaderComponents[1]);
            if (colHeaderDefinition.dataType.typeID == NODE ||
                colHeaderDefinition.dataType.typeID == LABEL) {
                throw LoaderException(
                    "PropertyDefinition column header cannot be of system types NODE or LABEL.");
            }
        }
        if (columnNameSet.find(colHeaderDefinition.name) != columnNameSet.end()) {
            throw LoaderException("Column " + colHeaderDefinition.name +
                                  " already appears previously in the column headers.");
        }
        columnNameSet.insert(colHeaderDefinition.name);
        colHeaderDefinitions.push_back(colHeaderDefinition);
    }
    return colHeaderDefinitions;
}

unique_ptr<vector<unique_ptr<NodeIDMap>>> GraphLoader::loadNodes() {
    logger->info("Starting to load nodes.");
    // Add each node label to catalog and count number of blocks in each label's csv file
    vector<uint64_t> numBlocksPerLabel(datasetMetadata.getNumNodeFiles());
    vector<string> fileHeaderPerLabel(datasetMetadata.getNumNodeFiles());
    vector<string> filePaths(datasetMetadata.getNumNodeFiles());
    for (auto i = 0u; i < datasetMetadata.getNumNodeFiles(); i++) {
        filePaths[i] = datasetMetadata.getNodeFileDescription(i).filePath;
    }
    readCSVHeaderAndCalcNumBlocks(filePaths, numBlocksPerLabel, fileHeaderPerLabel);
    addNodeLabelsIntoGraphCatalog(datasetMetadata.getNodeFileDescriptions(), fileHeaderPerLabel);

    // count number of lines and get unstructured propertyKeys in each block of each label.
    vector<vector<uint64_t>> numLinesPerBlock(catalog->getNumNodeLabels());
    vector<vector<unordered_set<string>>> labelBlockUnstrProperties(catalog->getNumNodeLabels());
    countLinesAndAddUnstrPropertiesInCatalog(
        numLinesPerBlock, labelBlockUnstrProperties, numBlocksPerLabel, filePaths);

    // create NodeIDMaps and call NodesLoader.
    auto nodeIDMaps = make_unique<vector<unique_ptr<NodeIDMap>>>(catalog->getNumNodeLabels());
    logger->info("Begin constructing nodeIDMaps. Number of nodeLabelsCount to add: {}",
        catalog->getNumNodeLabels());
    for (auto nodeLabel = 0u; nodeLabel < catalog->getNumNodeLabels(); nodeLabel++) {
        (*nodeIDMaps)[nodeLabel] = make_unique<NodeIDMap>(catalog->getNumNodes(nodeLabel));
    }
    logger->info("End constructing nodeIDMaps.");
    NodesLoader nodesLoader(*taskScheduler, *catalog, outputDirectory,
        datasetMetadata.getNodeFileDescriptions(), progressBar.get());
    nodesLoader.load(filePaths, numBlocksPerLabel, numLinesPerBlock, *nodeIDMaps);
    return nodeIDMaps;
}

void GraphLoader::loadRels(unique_ptr<vector<unique_ptr<NodeIDMap>>> nodeIDMaps) {
    logger->info("Starting to load rels.");
    vector<uint64_t> numBlocksPerLabel(datasetMetadata.getNumRelFiles());
    vector<string> fileHeaderPerLabel(datasetMetadata.getNumRelFiles());
    vector<string> filePaths(datasetMetadata.getNumRelFiles());
    for (auto i = 0u; i < datasetMetadata.getNumRelFiles(); i++) {
        filePaths[i] = datasetMetadata.getRelFileDescription(i).filePath;
    }
    readCSVHeaderAndCalcNumBlocks(filePaths, numBlocksPerLabel, fileHeaderPerLabel);
    addRelLabelsIntoGraphCatalog(datasetMetadata.getRelFileDescriptions(), fileHeaderPerLabel);

    RelsLoader relsLoader{*taskScheduler, *catalog, outputDirectory, *nodeIDMaps,
        datasetMetadata.getRelFileDescriptions(), progressBar.get()};
    relsLoader.load(numBlocksPerLabel);
}

void GraphLoader::countLinesAndAddUnstrPropertiesInCatalog(
    vector<vector<uint64_t>>& numLinesPerBlock,
    vector<vector<unordered_set<string>>>& labelBlockUnstrProperties,
    vector<uint64_t>& numBlocksPerLabel, const vector<string>& filePaths) {
    auto numLabels = catalog->getNumNodeLabels();
    for (label_t labelId = 0; labelId < numLabels; labelId++) {
        auto& labelUnstrProperties = labelBlockUnstrProperties[labelId];
        labelUnstrProperties.resize(numBlocksPerLabel[labelId]);
        numLinesPerBlock[labelId].resize(numBlocksPerLabel[labelId]);
        progressBar->addAndStartNewJob(
            "Counting lines in the file for node: " + catalog->getNodeLabelName(labelId),
            numBlocksPerLabel[labelId]);
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[labelId]; blockId++) {
            taskScheduler->scheduleTask(LoaderTaskFactory::createLoaderTask(
                countLinesAndScanUnstrPropertiesInBlockTask, filePaths[labelId],
                datasetMetadata.getNodeFileDescription(labelId).csvReaderConfig,
                catalog->getStructuredNodeProperties(labelId).size(),
                &labelUnstrProperties[blockId], &numLinesPerBlock, labelId, blockId, logger,
                progressBar.get()));
        }
        taskScheduler->waitAllTasksToCompleteOrError();
    }
    for (label_t label = 0; label < numLabels; label++) {
        auto& node = catalog->getNode(label);
        numLinesPerBlock[label][0]--;
        for (uint64_t blockId = 0; blockId < numBlocksPerLabel[label]; blockId++) {
            node.numNodes += numLinesPerBlock[label][blockId];
        }
    }
    logger->info("Done counting number of lines in each label.");
    for (label_t labelId = 0; labelId < numLabels; labelId++) {
        for (auto& unstrProperties : labelBlockUnstrProperties[labelId]) {
            for (auto& propertyName : unstrProperties) {
                catalog->addNodeUnstrProperty(labelId, propertyName);
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
            unstrPropertyNameSet->insert(unstrPropertyName);
        }
    }
    logger->trace("End: path=`{0}` blkIdx={1}", fName, blockId);
    progressBar->incrementTaskFinished();
}

} // namespace loader
} // namespace graphflow
