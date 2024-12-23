#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "function/table/simple_table_functions.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

struct SequenceInfo {
    std::string name;
    std::string databaseName;
    int64_t startValue;
    int64_t increment;
    int64_t minValue;
    int64_t maxValue;
    bool cycle;

    SequenceInfo(std::string name, std::string databaseName, int64_t startValue, int64_t increment,
        int64_t minValue, int64_t maxValue, bool cycle)
        : name{std::move(name)}, databaseName{std::move(databaseName)}, startValue{startValue},
          increment{increment}, minValue{minValue}, maxValue{maxValue}, cycle{cycle} {}
};

struct ShowSequencesBindData : public SimpleTableFuncBindData {
    std::vector<SequenceInfo> sequences;

    ShowSequencesBindData(std::vector<SequenceInfo> sequences, binder::expression_vector columns,
        offset_t maxOffset)
        : SimpleTableFuncBindData{std::move(columns), maxOffset}, sequences{std::move(sequences)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowSequencesBindData>(sequences, columns, maxOffset);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = input.sharedState->ptrCast<SimpleTableFuncSharedState>();
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto sequences = input.bindData->constPtrCast<ShowSequencesBindData>()->sequences;
    auto numSequencesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numSequencesToOutput; i++) {
        auto sequenceInfo = sequences[morsel.startOffset + i];
        dataChunk.getValueVectorMutable(0).setValue(i, sequenceInfo.name);
        dataChunk.getValueVectorMutable(1).setValue(i, sequenceInfo.databaseName);
        dataChunk.getValueVectorMutable(2).setValue(i, sequenceInfo.startValue);
        dataChunk.getValueVectorMutable(3).setValue(i, sequenceInfo.increment);
        dataChunk.getValueVectorMutable(4).setValue(i, sequenceInfo.minValue);
        dataChunk.getValueVectorMutable(5).setValue(i, sequenceInfo.maxValue);
        dataChunk.getValueVectorMutable(6).setValue(i, sequenceInfo.cycle);
    }
    return numSequencesToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    TableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("database name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("start value");
    columnTypes.emplace_back(LogicalType::INT64());
    columnNames.emplace_back("increment");
    columnTypes.emplace_back(LogicalType::INT64());
    columnNames.emplace_back("min value");
    columnTypes.emplace_back(LogicalType::INT64());
    columnNames.emplace_back("max value");
    columnTypes.emplace_back(LogicalType::INT64());
    columnNames.emplace_back("cycle");
    columnTypes.emplace_back(LogicalType::BOOL());
    std::vector<SequenceInfo> sequenceInfos;
    for (auto& entry : context->getCatalog()->getSequenceEntries(context->getTx())) {
        auto sequenceData = entry->getSequenceData();
        auto sequenceInfo = SequenceInfo{entry->getName(), LOCAL_DB_NAME, sequenceData.startValue,
            sequenceData.increment, sequenceData.minValue, sequenceData.maxValue,
            sequenceData.cycle};
        sequenceInfos.push_back(std::move(sequenceInfo));
    }

    // TODO: uncomment this when we can test it
    // auto databaseManager = context->getDatabaseManager();
    // for (auto attachedDatabase : databaseManager->getAttachedDatabases()) {
    //     auto databaseName = attachedDatabase->getDBName();
    //     auto databaseType = attachedDatabase->getDBType();
    //     for (auto& entry : attachedDatabase->getCatalog()->getSequenceEntries(context->getTx()))
    //     {
    //         auto sequenceData = entry->getSequenceData();
    //         auto sequenceInfo =
    //             SequenceInfo{entry->getName(), stringFormat("{}({})", databaseName,
    //             databaseType),
    //                 sequenceData.startValue, sequenceData.increment, sequenceData.minValue,
    //                 sequenceData.maxValue, sequenceData.cycle};
    //         sequenceInfos.push_back(std::move(sequenceInfo));
    //     }
    // }
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ShowSequencesBindData>(std::move(sequenceInfos), columns,
        sequenceInfos.size());
}

function_set ShowSequencesFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initSharedState;
    function->initLocalStateFunc = initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
