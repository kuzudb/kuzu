#pragma once

#include "processor/operator/ddl/ddl.h"
#include "storage/stats/rels_statistics.h"

namespace kuzu {
namespace processor {

class CreateRelTableGroup : public DDL {
public:
    CreateRelTableGroup(catalog::Catalog* catalog, storage::RelsStatistics* relsStatistics,
        std::unique_ptr<binder::BoundCreateTableInfo> info, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::CREATE_REL_TABLE, catalog, outputPos, id, paramsString},
          relsStatistics{relsStatistics}, info{std::move(info)} {}

    void executeDDLInternal() override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateRelTableGroup>(
            catalog, relsStatistics, info->copy(), outputPos, id, paramsString);
    }

private:
    storage::RelsStatistics* relsStatistics;
    std::unique_ptr<binder::BoundCreateTableInfo> info;
};

} // namespace processor
} // namespace kuzu
