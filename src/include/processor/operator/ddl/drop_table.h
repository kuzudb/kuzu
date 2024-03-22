#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class DropTable : public DDL {
public:
    DropTable(std::string tableName, common::table_id_t tableID, const DataPos& outputPos,
        uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::DROP_TABLE, outputPos, id, paramsString},
          tableName{std::move(tableName)}, tableID{tableID} {}

    void executeDDLInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropTable>(tableName, tableID, outputPos, id, paramsString);
    }

protected:
    std::string tableName;
    common::table_id_t tableID;
};

} // namespace processor
} // namespace kuzu
