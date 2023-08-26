#pragma once

#include "ddl.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

class CreateRDFGraph : public DDL {
public:
    CreateRDFGraph(catalog::Catalog* catalog, std::string rdfGraphName, const DataPos& outputPos,
        uint32_t id, const std::string& paramsString,
        storage::NodesStatisticsAndDeletedIDs* resourcesNodesStatistics,
        storage::RelsStatistics* triplesRelsStatistics, storage::StorageManager& storageManager)
        : DDL{PhysicalOperatorType::CREATE_RDF_GRAPH, catalog, outputPos, id, paramsString},
          rdfGraphName{std::move(rdfGraphName)}, resourcesNodesStatistics{resourcesNodesStatistics},
          triplesRelsStatistics{triplesRelsStatistics}, storageManager{storageManager} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateRDFGraph>(catalog, rdfGraphName, outputPos, id, paramsString,
            resourcesNodesStatistics, triplesRelsStatistics, storageManager);
    }

protected:
    std::string rdfGraphName;
    storage::NodesStatisticsAndDeletedIDs* resourcesNodesStatistics;
    storage::RelsStatistics* triplesRelsStatistics;
    storage::StorageManager& storageManager;
};

} // namespace processor
} // namespace kuzu
