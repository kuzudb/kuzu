#include "src/loader/include/graph_loader.h"

#include "nlohmann/json.hpp"
#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"
#include "src/common/include/exception.h"
#include "src/loader/in_mem_builder/include/in_mem_node_builder.h"
#include "src/loader/in_mem_builder/include/in_mem_rel_builder.h"
#include "src/storage/store/include/nodes_metadata.h"

using namespace std::chrono;

namespace graphflow {
namespace loader {

GraphLoader::GraphLoader(string inputDirectory, string outputDirectory, uint32_t numThreads,
    uint64_t defaultPageBufferPoolSize, uint64_t largePageBufferPoolSize)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")},
      inputDirectory{std::move(inputDirectory)}, outputDirectory{std::move(outputDirectory)} {
    taskScheduler = make_unique<TaskScheduler>(numThreads);
    catalog = make_unique<Catalog>();
    bufferManager = make_unique<BufferManager>(defaultPageBufferPoolSize, largePageBufferPoolSize);
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
        loadNodes();
        loadRels();
        progressBar->addAndStartNewJob("Saving Catalog to file.", 1);
        catalog->getReadOnlyVersion()->saveToFile(outputDirectory, false /* isForWALRecord */);
        progressBar->incrementTaskFinished();

        progressBar->addAndStartNewJob("Saving NodesMetadata to file.", 1);
        NodesMetadata::saveToFile(outputDirectory, maxNodeOffsetsPerNodeLabel, logger);
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

void GraphLoader::loadNodes() {
    logger->info("Starting to load nodes.");
    for (auto i = 0u; i < datasetMetadata.getNumNodeFiles(); ++i) {
        auto nodeBuilder = make_unique<InMemNodeBuilder>(datasetMetadata.getNodeFileDescription(i),
            outputDirectory, *taskScheduler, *catalog, progressBar.get());
        auto numNodesLoaded = nodeBuilder->load();
        maxNodeOffsetsPerNodeLabel.push_back(numNodesLoaded - 1);
    }
    logger->info("Done loading nodes.");
}

void GraphLoader::loadRels() {
    logger->info("Starting to load rels.");
    uint64_t numRelsLoaded = 0;
    for (auto relLabel = 0u; relLabel < datasetMetadata.getNumRelFiles(); relLabel++) {
        auto relBuilder =
            make_unique<InMemRelBuilder>(datasetMetadata.getRelFileDescription(relLabel),
                outputDirectory, *taskScheduler, *catalog, maxNodeOffsetsPerNodeLabel,
                numRelsLoaded, bufferManager.get(), progressBar.get());
        numRelsLoaded += relBuilder->load();
    }
    logger->info("Done loading rels.");
}

} // namespace loader
} // namespace graphflow
