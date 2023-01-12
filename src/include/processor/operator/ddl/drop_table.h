#pragma once

#include "ddl.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class DropTable : public DDL {
public:
    DropTable(Catalog* catalog, table_id_t tableID, const DataPos& outputPos, uint32_t id,
        const string& paramsString)
        : DDL{PhysicalOperatorType::DROP_TABLE, catalog, outputPos, id, paramsString},
          tableID{tableID} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropTable>(catalog, tableID, outputPos, id, paramsString);
    }

protected:
    table_id_t tableID;
};

} // namespace processor
} // namespace kuzu
