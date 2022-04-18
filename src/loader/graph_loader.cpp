#include "src/loader/include/graph_loader.h"

#include "nlohmann/json.hpp"

#include "src/common/include/configs.h"
#include "src/common/include/exception.h"
#include "src/loader/include/in_mem_structure/builder/in_mem_node_builder.h"
#include "src/loader/include/in_mem_structure/builder/in_mem_rel_builder.h"
#include "src/loader/include/loader_task.h"

using namespace std::chrono;

namespace graphflow {
namespace loader {

GraphLoader::GraphLoader(string inputDirectory, string outputDirectory, uint32_t numThreads)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")},
      inputDirectory{std::move(inputDirectory)}, outputDirectory{std::move(outputDirectory)} {
    taskScheduler = make_unique<TaskScheduler>(numThreads);
    catalog = make_unique<Catalog>();
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

        logger->info("Creating nodeIDToOffset maps.");
        progressBar->addAndStartNewJob("Constructing reverse nodeIDMaps", nodeIDMaps.size());
        for (auto& nodeIDMap : nodeIDMaps) {
            taskScheduler->scheduleTask(LoaderTaskFactory::createLoaderTask(
                [&](NodeIDMap* x, LoaderProgressBar* progressBar_) {
                    x->createNodeIDToOffsetMap();
                    progressBar_->incrementTaskFinished();
                },
                nodeIDMap.get(), progressBar.get()));
        }
        taskScheduler->waitAllTasksToCompleteOrError();
        logger->info("Done creating nodeIDToOffset maps.");

        loadRels(nodeIDMaps);

        progressBar->addAndStartNewJob("Saving Catalog to file.", 1);
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

vector<unique_ptr<NodeIDMap>> GraphLoader::loadNodes() {
    logger->info("Starting to load nodes.");
    auto numNodeLabels = datasetMetadata.getNumNodeFiles();
    vector<unique_ptr<NodeIDMap>> nodeIDMaps(numNodeLabels);
    for (auto nodeLabel = 0u; nodeLabel < numNodeLabels; nodeLabel++) {
        auto nodeBuilder = make_unique<InMemNodeBuilder>(nodeLabel,
            datasetMetadata.getNodeFileDescription(nodeLabel), outputDirectory, *taskScheduler,
            *catalog, progressBar.get());
        nodeIDMaps[nodeLabel] = nodeBuilder->load();
    }
    logger->info("Done loading nodes.");
    return nodeIDMaps;
}

void GraphLoader::loadRels(const vector<unique_ptr<NodeIDMap>>& nodeIDMaps) {
    logger->info("Starting to load rels.");
    auto numRelLabels = datasetMetadata.getNumRelFiles();
    for (auto relLabel = 0u; relLabel < numRelLabels; relLabel++) {
        auto relBuilder =
            make_unique<InMemRelBuilder>(relLabel, datasetMetadata.getRelFileDescription(relLabel),
                outputDirectory, *taskScheduler, *catalog, nodeIDMaps, progressBar.get());
        relBuilder->load();
    }
    logger->info("Done loading rels.");
}

} // namespace loader
} // namespace graphflow
