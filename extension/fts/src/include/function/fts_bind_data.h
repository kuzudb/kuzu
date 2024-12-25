#pragma once

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/types/value/value.h"
#include "main/client_context.h"

namespace kuzu {
namespace fts_extension {

struct FTSBindData : public function::SimpleTableFuncBindData {
    std::string tableName;
    common::table_id_t tableID;
    std::string indexName;

    FTSBindData(std::string tableName, common::table_id_t tableID, std::string indexName,
        binder::expression_vector columns)
        : function::SimpleTableFuncBindData{std::move(columns), 1 /* maxOffset */},
          tableName{std::move(tableName)}, tableID{tableID}, indexName{std::move(indexName)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<FTSBindData>(*this);
    }
};

} // namespace fts_extension
} // namespace kuzu
