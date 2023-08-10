#pragma once

#include "processor/operator/ddl/create_table.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace processor {

class CreateRelTable : public CreateTable {
public:
    CreateRelTable(catalog::Catalog* catalog, std::string tableName,
        std::vector<std::unique_ptr<catalog::Property>> properties,
        catalog::RelMultiplicity relMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString, storage::RelsStatistics* relsStatistics)
        : CreateTable{PhysicalOperatorType::CREATE_REL_TABLE, catalog, std::move(tableName),
              std::move(properties), outputPos, id, paramsString},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          relsStatistics{relsStatistics} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateRelTable>(catalog, tableName,
            catalog::Property::copyProperties(properties), relMultiplicity, srcTableID, dstTableID,
            outputPos, id, paramsString, relsStatistics);
    }

private:
    catalog::RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    storage::RelsStatistics* relsStatistics;
};

} // namespace processor
} // namespace kuzu
