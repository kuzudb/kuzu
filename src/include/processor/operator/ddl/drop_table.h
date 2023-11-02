#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class DropTable : public DDL {
public:
    DropTable(catalog::Catalog* catalog, common::table_id_t tableID, const DataPos& outputPos,
        uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::DROP_TABLE, catalog, outputPos, id, paramsString},
          tableID{tableID} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropTable>(catalog, tableID, outputPos, id, paramsString);
    }

protected:
    common::table_id_t tableID;
};

} // namespace processor
} // namespace kuzu
