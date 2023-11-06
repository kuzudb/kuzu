#pragma once

#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class CreateRelTable : public DDL {
public:
    CreateRelTable(catalog::Catalog* catalog, storage::StorageManager* storageManager,
        std::unique_ptr<binder::BoundCreateTableInfo> info, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::CREATE_REL_TABLE, catalog, outputPos, id, paramsString},
          storageManager{storageManager}, info{std::move(info)} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateRelTable>(
            catalog, storageManager, info->copy(), outputPos, id, paramsString);
    }

private:
    storage::StorageManager* storageManager;
    std::unique_ptr<binder::BoundCreateTableInfo> info;
};

} // namespace processor
} // namespace kuzu
