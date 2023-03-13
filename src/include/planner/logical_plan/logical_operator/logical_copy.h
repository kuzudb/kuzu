#pragma once

#include "base_logical_operator.h"
#include "catalog/catalog_structs.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace planner {

class LogicalCopy : public LogicalOperator {

public:
    LogicalCopy(const common::CSVReaderConfig& csvReaderConfig, common::table_id_t tableID,
        std::string tableName, std::shared_ptr<LogicalOperator> child,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalOperator{LogicalOperatorType::COPY_CSV, std::move(child)},
          csvReaderConfig{csvReaderConfig}, tableID{tableID}, tableName{std::move(tableName)},
          outputExpression{std::move(outputExpression)} {}

    inline void computeFactorizedSchema() override {
        copyChildSchema(0);
        auto groupPos = schema->createGroup();
        schema->insertToGroupAndScope(outputExpression, groupPos);
        schema->setGroupAsSingleState(groupPos);
    }
    inline void computeFlatSchema() override {
        copyChildSchema(0);
        schema->createGroup();
        schema->insertToGroupAndScope(outputExpression, 0);
    }

    inline std::string getExpressionsForPrinting() const override { return tableName; }

    inline common::CSVReaderConfig getCSVReaderConfig() const { return csvReaderConfig; }

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopy>(
            csvReaderConfig, tableID, tableName, children[0], outputExpression);
    }

    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

private:
    common::CSVReaderConfig csvReaderConfig;
    common::table_id_t tableID;
    // Used for printing only.
    std::string tableName;
    std::shared_ptr<binder::Expression> outputExpression;
};

} // namespace planner
} // namespace kuzu
