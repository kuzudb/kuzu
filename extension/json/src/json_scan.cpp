#include "json_scan.h"

#include "json_utils.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace json_extension {

struct JsonBindData : public TableFuncBindData {

    std::shared_ptr<JsonWrapper> json;
    bool scanFromList;
    bool scanFromStruct;
    std::map<std::string, uint32_t> nameToIdxMap;

    JsonBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, std::shared_ptr<JsonWrapper> wrapper,
        bool scanFromList, bool scanFromStruct)
        : TableFuncBindData(std::move(columnTypes), std::move(columnNames)),
          json(wrapper),
          scanFromList{scanFromList}, scanFromStruct{scanFromStruct} {
        for (auto i = 0u; i < columnNames.size(); i++) {
            nameToIdxMap[columnNames[i]] = i;
        }
    }

    std::unique_ptr<TableFuncBindData> copy() const {
        return std::make_unique<JsonBindData>(LogicalType::copy(columnTypes), columnNames,
            json, scanFromList, scanFromStruct);
    }
};

struct JsonScanSharedState : public TableFuncSharedState {

    std::vector<yyjson_val*> rows;
    uint64_t curPos = 0u;
    std::mutex lock;

    JsonScanSharedState(std::vector<yyjson_val*> rows) : TableFuncSharedState{},
        rows{std::move(rows)} {}
};

struct JsonScanLocalState : public TableFuncLocalState {

    std::vector<yyjson_val*>::iterator begin, end;
    uint64_t chunkSize;

    JsonScanLocalState(std::vector<yyjson_val*>::iterator begin,
        std::vector<yyjson_val*>::iterator end, uint64_t chunkSize) : begin{begin}, end{end},
        chunkSize{chunkSize} {}
};


static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext*, TableFuncBindInput* input) {
    auto scanInput = ku_dynamic_cast<TableFuncBindInput*, ScanTableFuncBindInput*>(input);
    auto parsedJson = fileToJson(scanInput->inputs[0].strVal);
    std::vector<LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    auto schema = jsonSchema(parsedJson);
    auto scanFromList = false;
    auto scanFromStruct = false;
    if (schema.getLogicalTypeID() == LogicalTypeID::LIST) {
        schema = ListType::getChildType(schema).copy();
        scanFromList = true;
    }
    if (schema.getLogicalTypeID() == LogicalTypeID::STRUCT) {
        for (const auto& i : StructType::getFields(schema)) {
            columnTypes.push_back(i.getType().copy());
            columnNames.push_back(i.getName());
        }
        scanFromStruct = true;
    } else {
        columnTypes.push_back(std::move(schema));
        columnNames.push_back("json");
    }
    return std::make_unique<JsonBindData>(std::move(columnTypes), std::move(columnNames),
        std::make_shared<JsonWrapper>(std::move(parsedJson)), scanFromList, scanFromStruct);
}

static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input) {
    auto jsonBindData = ku_dynamic_cast<TableFuncBindData*, JsonBindData*>(input.bindData);
    std::vector<yyjson_val*> rows;
    if (jsonBindData->scanFromList) {
        auto it = yyjson_arr_iter_with(yyjson_doc_get_root(jsonBindData->json->ptr));
        yyjson_val* val;
        while ((val = yyjson_arr_iter_next(&it))) {
            rows.push_back(val);
        }
    } else {
        rows.push_back(yyjson_doc_get_root(jsonBindData->json->ptr));
    }
    return std::make_unique<JsonScanSharedState>(std::move(rows));
}

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput&, TableFuncSharedState* shared,
    storage::MemoryManager* /*mm*/) {
    auto jsonShared = ku_dynamic_cast<TableFuncSharedState*, JsonScanSharedState*>(shared);
    if (jsonShared->curPos >= jsonShared->rows.size()) {
        return nullptr;
    }
    std::scoped_lock(jsonShared->lock);
    auto chunkSize = std::min(DEFAULT_VECTOR_CAPACITY, jsonShared->rows.size() - jsonShared->curPos);
    auto begin = jsonShared->rows.begin() + jsonShared->curPos;
    jsonShared->curPos += chunkSize;
    auto end = jsonShared->rows.begin() + jsonShared->curPos;
    return std::make_unique<JsonScanLocalState>(begin, end, chunkSize);
}

static offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    const auto bindData = ku_dynamic_cast<TableFuncBindData*, JsonBindData*>(input.bindData);
    const auto localState = ku_dynamic_cast<TableFuncLocalState*, JsonScanLocalState*>(input.localState);
    for (auto& i: output.vectors) {
        i->setAllNull();
    }
    for (auto i = localState->begin; i != localState->end; i++) {
        if (bindData->scanFromStruct) {
            auto objIter = yyjson_obj_iter_with(*i);
            yyjson_val *key, *ele;
            while ((key = yyjson_obj_iter_next(&objIter))) {
                ele = yyjson_obj_iter_get_val(key);
                auto columnIdx = bindData->nameToIdxMap[yyjson_get_str(key)];
                readJsonToValueVector(ele, *output.vectors[columnIdx], i - localState->begin);
            }
        } else {
            readJsonToValueVector(*i, *output.vectors[0], i - localState->begin);
        }
    }
    return localState->begin - localState->end;
}

static double progressFunc(TableFuncSharedState* state) {
    auto jsonShared = ku_dynamic_cast<TableFuncSharedState*, JsonScanSharedState*>(state);
    return (double)jsonShared->curPos / jsonShared->rows.size();
}

std::unique_ptr<TableFunction> JsonScan::getFunction() {
    return std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState, initLocalState,
        progressFunc, std::vector<LogicalTypeID>{LogicalTypeID::STRING});
}

} // namespace json_extension
} // namespace kuzu
