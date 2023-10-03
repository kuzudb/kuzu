#include "function/table_functions/show_tables.h"

#include "catalog/catalog_content.h"
#include "catalog/table_schema.h"
#include "common/vector/value_vector.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ShowTablesBindData : public TableFuncBindData {
    std::vector<catalog::TableSchema*> tables;

    ShowTablesBindData(std::vector<catalog::TableSchema*> tables,
        std::vector<common::LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        common::offset_t maxOffset)
        : tables{std::move(tables)}, TableFuncBindData{std::move(returnTypes),
                                         std::move(returnColumnNames), maxOffset} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<ShowTablesBindData>(
            tables, returnTypes, returnColumnNames, maxOffset);
    }
};

void ShowTablesFunction::tableFunc(std::pair<offset_t, offset_t> morsel,
    function::TableFuncBindData* bindData, std::vector<ValueVector*> outputVectors) {
    auto tables = reinterpret_cast<function::ShowTablesBindData*>(bindData)->tables;
    auto numTablesToOutput = morsel.second - morsel.first;
    for (auto i = 0u; i < numTablesToOutput; i++) {
        auto tableSchema = tables[morsel.first + i];
        outputVectors[0]->setValue(i, tableSchema->tableName);
        std::string typeString = TableTypeUtils::toString(tableSchema->tableType);
        outputVectors[1]->setValue(i, typeString);
        outputVectors[2]->setValue(i, tableSchema->comment);
    }
    outputVectors[0]->state->selVector->selectedSize = numTablesToOutput;
}

std::unique_ptr<TableFuncBindData> ShowTablesFunction::bindFunc(main::ClientContext* context,
    kuzu::function::TableFuncBindInput input, catalog::CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("name");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    returnColumnNames.emplace_back("type");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    returnColumnNames.emplace_back("comment");
    returnTypes.emplace_back(LogicalTypeID::STRING);

    return std::make_unique<ShowTablesBindData>(catalog->getTableSchemas(), std::move(returnTypes),
        std::move(returnColumnNames), catalog->getTableCount());
}

} // namespace function
} // namespace kuzu
