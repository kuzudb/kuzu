#include "json_scan.h"

#include "json_utils.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace json_extension {

struct JsonBindData : public TableFuncBindData {

    JsonWrapper json;

    JsonBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, JsonWrapper&& wrapper)
        : TableFuncBindData(columnTypes, columnNames), json(std::move(wrapper)) {}
};

struct JsonScanSharedState : public TableFuncSharedState {

    std::vector<yyjson_arr_iter> iters;
    uint64_t curChunk;
    std::mutex lock;

    JsonScanSharedState(std::vector<yyjson_arr_iter> it) : iters{std::move(it)}, curChunk{0u},
        TableFuncSharedState{} {}
};

struct JsonScanLocalState : public TableFuncLocalState {

    yyjson_arr_iter iterator;
    uint64_t chunkSize;

    JsonScanLocalState(yyjson_arr_iter iter, uint64_t chunkSize) : iterator{iter},
        chunkSize{chunkSize} {}
};


static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* ctx, TableFuncBindInput* input) {
    auto scanInput = ku_dynamic_cast<TableFuncBindInput*, ScanTableFuncBindInput*>(input);
    auto parsedJson = fileToJson(scanInput->inputs[0].strVal);
    std::vector<LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    auto schema = jsonSchema(parsedJson);
    if (schema.getLogicalTypeID() == LogicalTypeID::STRUCT) {
        for (const auto& i : StructType::getFields(schema)) {
            columnTypes.push_back(i.getType().copy());
            columnNames.push_back(i.getName());
        }
    } else if (schema.getLogicalTypeID() == LogicalTypeID::LIST &&
        ListType::getChildType(schema).getLogicalTypeID() == LogicalTypeID::STRUCT) {
        const auto& childType = ListType::getChildType(schema);
        for (const auto& i : StructType::getFields(childType)) {
            columnTypes.push_back(i.getType().copy());
            columnNames.push_back(i.getName());
        }
    } else {
        columnTypes.push_back(schema);
        columnNames.push_back("json");
    }
    return std::make_unique<JsonBindData>(std::move(columnTypes), std::move(columnNames), std::move(parsedJson));
}

static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input) {
    auto jsonBindData = ku_dynamic_cast<TableFuncBindData*, JsonBindData*>(input.bindData);
    std::vector<yyjson_arr_iter> iters;
    auto cnt = 0u;
    auto it = yyjson_arr_iter_with(yyjson_doc_get_root(jsonBindData->json.ptr));
    while (yyjson_arr_iter_next(&it)) {
        if (cnt == 0u) {
            iters.push_back(it);
        }
        cnt = (cnt + 1u) % DEFAULT_VECTOR_CAPACITY;
    }
    return std::make_unique<JsonScanSharedState>(std::move(iters));
}

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput& input, TableFuncSharedState* shared,
    storage::MemoryManager* /*mm*/) {
    auto jsonShared = ku_dynamic_cast<TableFuncSharedState*, JsonScanSharedState*>(shared);
    std::scoped_lock(jsonShared->lock);
    return std::make_unique<JsonScanLocalState>(jsonShared->iters[jsonShared->curChunk++]);
}

static offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, JsonBindData*>(input.bindData);
    auto localState = ku_dynamic_cast<TableFuncLocalState*, JsonScanLocalState*>(input.localState);
    
}

static double progressFunc(TableFuncSharedState* state) {
    auto jsonShared = ku_dynamic_cast<TableFuncSharedState*, JsonScanSharedState*>(state);
    return (double)jsonShared->curChunk / jsonShared->iters.size();
}

TableFunction JsonScan::getFunction() {
    return TableFunction(name, tableFunc, bindFunc, initSharedState, initLocalState, progressFunc,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING});
}

} // namespace json_extension
} // namespace kuzu
