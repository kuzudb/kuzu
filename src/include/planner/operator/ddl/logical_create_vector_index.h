#pragma once

#include "binder/expression/expression.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/vector_index/vector_index_config.h"
#include "planner/operator/ddl/logical_ddl.h"
#include "planner/operator/logical_operator.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

class LogicalCreateVectorIndex : public LogicalDDL {
public:
    LogicalCreateVectorIndex(std::string tableName, std::string propertyName, table_id_t tableID,
        property_id_t propertyID, common::VectorIndexConfig vectorIndexConfig,
        std::shared_ptr<Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::CREATE_VECTOR_INDEX, std::move(tableName),
              std::move(outputExpression)},
          propertyName(std::move(propertyName)), tableID{tableID}, propertyID{propertyID},
          vectorIndexConfig{vectorIndexConfig} {}

    inline std::string getExpressionsForPrinting() const override {
        return stringFormat("CreateVectorIndex_{}_{}", tableName, propertyName);
    }

    inline std::string getPropertyName() const { return propertyName; }

    inline table_id_t getTableID() const { return tableID; }

    inline property_id_t getPropertyID() const { return propertyID; }

    inline common::VectorIndexConfig getVectorIndexConfig() const { return vectorIndexConfig; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalCreateVectorIndex>(tableName, propertyName, tableID,
            propertyID, vectorIndexConfig, outputExpression);
    }

private:
    std::string propertyName;
    table_id_t tableID;
    property_id_t propertyID;
    common::VectorIndexConfig vectorIndexConfig;
};

} // namespace planner
} // namespace kuzu
