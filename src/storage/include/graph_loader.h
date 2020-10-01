#ifndef GRAPHFLOW_STORAGE_GRAPHLOADER_H_
#define GRAPHFLOW_STORAGE_GRAPHLOADER_H_

#include <vector>

#include "nlohmann/json.hpp"
#include "spdlog/spdlog.h"

#include "src/common/include/types.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class GraphLoader {

    std::shared_ptr<spdlog::logger> logger;
    const std::string inputDirectory;

public:
    GraphLoader(std::string inputDirectory);
    ~GraphLoader() = default;
    std::unique_ptr<Graph> loadGraph();

private:
    std::unique_ptr<nlohmann::json> readMetadata();
    void setNodeAndRelLabels(const nlohmann::json &metadata, Catalog &catalog);

    void loadNodes(
        const nlohmann::json &metadata, Catalog &catalog, std::vector<uint64_t> &numNodesPerLabel);
    void countNodes(std::vector<uint64_t> &numNodesPerLabel,
        std::vector<std::unique_ptr<std::vector<Property>>> &nodeLabelPropertyDescriptors);

    void loadRels(
        const nlohmann::json &metadata, Catalog &catalog, std::vector<uint64_t> &numRelsPerLabel);
    void countRels(std::vector<uint64_t> &numNodesPerLabel,
        std::vector<std::unique_ptr<std::vector<Property>>> &nodeLabelPropertyDescriptors);

    void assignLabels(std::unordered_map<std::string, gfLabel_t> &stringToLabelMap,
        const nlohmann::json &fileDescriptions);
    void readHeaderAndCountLines(const std::string &fname, std::vector<uint64_t> &numPerLabel,
        std::vector<std::unique_ptr<std::vector<Property>>> &labelPropertyDescriptors,
        gfLabel_t label, const nlohmann::json &metadata);
    std::unique_ptr<std::vector<Property>> parseHeader(
        const nlohmann::json &metadata, std::unique_ptr<std::string> header);
};

} // namespace storage
} // namespace graphflow

#endif // GRAPHFLOW_STORAGE_GRAPHLOADER_H_
