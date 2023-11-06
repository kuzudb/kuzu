#pragma once

#include "ddl.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

class CreateRdfGraph : public DDL {
public:
    CreateRdfGraph(catalog::Catalog* catalog, storage::StorageManager* storageManager,
        std::unique_ptr<binder::BoundCreateTableInfo> info, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::CREATE_RDF_GRAPH, catalog, outputPos, id, paramsString},
          storageManager{storageManager},
          nodesStatistics{storageManager->getNodesStatisticsAndDeletedIDs()},
          relsStatistics{storageManager->getRelsStatistics()}, info{std::move(info)} {}

    void executeDDLInternal() final;

    std::string getOutputMsg() final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CreateRdfGraph>(
            catalog, storageManager, info->copy(), outputPos, id, paramsString);
    }

private:
    storage::StorageManager* storageManager;
    storage::NodesStoreStatsAndDeletedIDs* nodesStatistics;
    storage::RelsStoreStats* relsStatistics;
    std::unique_ptr<binder::BoundCreateTableInfo> info;
};

} // namespace processor
} // namespace kuzu
