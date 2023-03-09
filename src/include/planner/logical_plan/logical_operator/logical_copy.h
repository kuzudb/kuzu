#pragma once

#include "base_logical_operator.h"
#include "catalog/catalog_structs.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace planner {

class LogicalCopy : public LogicalOperator {

public:
    LogicalCopy(const common::CopyDescription& copyDescription, common::table_id_t tableID,
        std::string tableName)
        : LogicalOperator{LogicalOperatorType::COPY_CSV},
          copyDescription{copyDescription}, tableID{tableID}, tableName{std::move(tableName)} {}

    inline void computeFactorizedSchema() override { createEmptySchema(); }
    inline void computeFlatSchema() override { createEmptySchema(); }

    inline std::string getExpressionsForPrinting() const override { return tableName; }

    inline common::CopyDescription getCopyDescription() const { return copyDescription; }

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopy>(copyDescription, tableID, tableName);
    }

private:
    common::CopyDescription copyDescription;
    common::table_id_t tableID;
    // Used for printing only.
    std::string tableName;
};

} // namespace planner
} // namespace kuzu
