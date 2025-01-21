#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/index_catalog_entry.h"
#include "function/table/simple_table_functions.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct IndexInfo {
    std::string tableName;
    std::string indexName;
    std::string indexType;
    std::vector<std::string> properties;
    bool dependencyLoaded;
    std::string indexDefinition;

    IndexInfo(std::string tableName, std::string indexName, std::string indexType,
        std::vector<std::string> properties, bool dependencyLoaded, std::string indexDefinition)
        : tableName{std::move(tableName)}, indexName{std::move(indexName)},
          indexType{std::move(indexType)}, properties{std::move(properties)},
          dependencyLoaded{dependencyLoaded}, indexDefinition{std::move(indexDefinition)} {}
};

struct ShowIndexesBindData final : SimpleTableFuncBindData {
    std::vector<IndexInfo> indexesInfo;

    ShowIndexesBindData(std::vector<IndexInfo> indexesInfo, binder::expression_vector columns,
        offset_t maxOffset)
        : SimpleTableFuncBindData{std::move(columns), maxOffset},
          indexesInfo{std::move(indexesInfo)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowIndexesBindData>(*this);
    }
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    const auto sharedState = input.sharedState->ptrCast<SimpleTableFuncSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto& indexesInfo = input.bindData->constPtrCast<ShowIndexesBindData>()->indexesInfo;
    auto numTuplesToOutput = morsel.endOffset - morsel.startOffset;
    auto& propertyVector = dataChunk.getValueVectorMutable(3);
    auto propertyDataVec = ListVector::getDataVector(&propertyVector);
    for (auto i = 0u; i < numTuplesToOutput; i++) {
        auto indexInfo = indexesInfo[morsel.startOffset + i];
        dataChunk.getValueVectorMutable(0).setValue(i, indexInfo.tableName);
        dataChunk.getValueVectorMutable(1).setValue(i, indexInfo.indexName);
        dataChunk.getValueVectorMutable(2).setValue(i, indexInfo.indexType);
        auto listEntry = ListVector::addList(&propertyVector, indexInfo.properties.size());
        for (auto j = 0u; j < indexInfo.properties.size(); j++) {
            propertyDataVec->setValue(listEntry.offset + j, indexInfo.properties[j]);
        }
        propertyVector.setValue(i, listEntry);
        dataChunk.getValueVectorMutable(4).setValue(i, indexInfo.dependencyLoaded);
        dataChunk.getValueVectorMutable(5).setValue(i, indexInfo.indexDefinition);
    }
    return numTuplesToOutput;
}

static binder::expression_vector bindColumns(binder::Binder& binder) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("table name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("index name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("index type");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("property names");
    columnTypes.emplace_back(LogicalType::LIST(LogicalType::STRING()));
    columnNames.emplace_back("extension loaded");
    columnTypes.emplace_back(LogicalType::BOOL());
    columnNames.emplace_back("index definition");
    columnTypes.emplace_back(LogicalType::STRING());
    return binder.createVariables(columnNames, columnTypes);
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<IndexInfo> indexesInfo;
    auto catalog = context->getCatalog();
    auto indexEntries = catalog->getIndexEntries(context->getTransaction());
    for (auto indexEntry : indexEntries) {
        auto tableName =
            catalog->getTableCatalogEntry(context->getTransaction(), indexEntry->getTableID())
                ->getName();
        auto indexName = indexEntry->getIndexName();
        auto indexType = indexEntry->getIndexType();
        auto properties = indexEntry->getProperties();
        auto dependencyLoaded = indexEntry->isLoaded();
        std::string indexDefinition;
        if (dependencyLoaded) {
            auto& auxInfo = indexEntry->getAuxInfo();
            indexDefinition = auxInfo.toCypher(*indexEntry, context);
        }
        indexesInfo.emplace_back(std::move(tableName), std::move(indexName), std::move(indexType),
            std::move(properties), dependencyLoaded, std::move(indexDefinition));
    }
    return std::make_unique<ShowIndexesBindData>(indexesInfo, bindColumns(*input->binder),
        indexesInfo.size());
}

function_set ShowIndexesFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<common::LogicalTypeID>{});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initSharedState;
    function->initLocalStateFunc = initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
