#pragma once

#include "catalog/catalog.h"
#include "ddl.h"

namespace kuzu {
namespace processor {

class RenameTable final : public DDL {
public:
    RenameTable(common::table_id_t tableID, std::string newName, const DataPos& outputPos,
        uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::RENAME_TABLE, outputPos, id, paramsString}, tableID{tableID},
          newName{std::move(newName)} {}

    void executeDDLInternal(ExecutionContext* context) override {
        context->clientContext->getCatalog()->renameTable(tableID, newName);
    }

    std::string getOutputMsg() override { return "Table renamed"; }

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<RenameTable>(tableID, newName, outputPos, id, paramsString);
    }

protected:
    common::table_id_t tableID;
    std::string newName;
};

} // namespace processor
} // namespace kuzu
