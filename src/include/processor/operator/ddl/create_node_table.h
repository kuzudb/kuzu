#pragma once

#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class CreateNodeTable : public DDL {
public:
    CreateNodeTable(catalog::Catalog* catalog, storage::StorageManager* storageManager,
        std::unique_ptr<binder::BoundCreateTableInfo> info, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::CREATE_NODE_TABLE, catalog, outputPos, id, paramsString},
          storageManager{storageManager}, info{std::move(info)} {}

    void executeDDLInternal() final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CreateNodeTable>(
            catalog, storageManager, info->copy(), outputPos, id, paramsString);
    }

private:
    storage::StorageManager* storageManager;
    std::unique_ptr<binder::BoundCreateTableInfo> info;
};

} // namespace processor
} // namespace kuzu
