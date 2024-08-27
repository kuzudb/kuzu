#include "json_export.h"

#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_serializer.h"
#include "function/export/export_function.h"
#include "json_utils.h"
#include "main/client_context.h"

using namespace kuzu::function;
using namespace kuzu::common;

namespace kuzu {
namespace json_extension {

struct ExportJSONBindData : public ExportFuncBindData {
    ExportJSONBindData(std::vector<std::string> columnNames, const std::string& fileName)
        : ExportFuncBindData(std::move(columnNames), fileName) {}
    ExportJSONBindData(std::vector<std::string> columnNames, std::vector<LogicalType> columnTypes,
        std::string fileName)
        : ExportFuncBindData(std::move(columnNames), std::move(fileName)) {
        setDataType(std::move(columnTypes));
    }

    std::unique_ptr<ExportFuncBindData> copy() const override {
        return std::make_unique<ExportJSONBindData>(columnNames, LogicalType::copy(types),
            fileName);
    }
};

struct ExportJSONSharedState : public ExportFuncSharedState {
    std::mutex mtx;
    std::unique_ptr<FileInfo> fileInfo;
    std::vector<std::string> jsonValues;

    void init(main::ClientContext& context, const ExportFuncBindData& bindData) override {
        fileInfo = context.getVFSUnsafe()->openFile(bindData.fileName,
            FileFlags::WRITE | FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS, &context);
    }
};

struct ExportJSONLocalState : public ExportFuncLocalState {
    std::vector<std::string> jsonValues;
};

static std::unique_ptr<ExportFuncBindData> bindFunc(ExportFuncBindInput& bindInput) {
    return std::make_unique<ExportJSONBindData>(bindInput.columnNames, bindInput.filePath);
}

static void initSharedState(ExportFuncSharedState& sharedState, main::ClientContext& context,
    const ExportFuncBindData& bindData) {
    sharedState.init(context, bindData);
}

static std::shared_ptr<ExportFuncSharedState> createSharedStateFunc() {
    return std::make_shared<ExportJSONSharedState>();
}

static std::unique_ptr<ExportFuncLocalState> initLocalState(main::ClientContext&,
    const ExportFuncBindData&, std::vector<bool>) {
    return std::make_unique<ExportJSONLocalState>();
}

static void sinkFunc(ExportFuncSharedState&, ExportFuncLocalState& localState,
    const ExportFuncBindData& bindData, std::vector<std::shared_ptr<ValueVector>> inputVectors) {
    auto& jsonLocalState = localState.cast<ExportJSONLocalState>();
    for (const auto& i : jsonifyQueryResult(inputVectors, bindData.columnNames)) {
        jsonLocalState.jsonValues.push_back(jsonToString(i));
    }
}

static void combineFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState) {
    auto& jsonSharedState = sharedState.cast<ExportJSONSharedState>();
    auto& jsonLocalState = localState.cast<ExportJSONLocalState>();
    std::lock_guard<std::mutex> lck{jsonSharedState.mtx};
    for (auto& jsonValue : jsonLocalState.jsonValues) {
        jsonSharedState.jsonValues.push_back(std::move(jsonValue));
    }
}

static void finalizeFunc(ExportFuncSharedState& sharedState) {
    auto& jsonSharedState = sharedState.cast<ExportJSONSharedState>();
    BufferedSerializer serializer;
    serializer.writeBufferData("[\n");
    for (auto& i : jsonSharedState.jsonValues) {
        serializer.writeBufferData(i);
        if (&i == &jsonSharedState.jsonValues.back()) {
            serializer.writeBufferData("\n");
        } else {
            serializer.writeBufferData(",\n");
        }
    }
    serializer.writeBufferData("]\n");
    jsonSharedState.fileInfo->writeFile(serializer.getBlobData(), serializer.getSize(), 0);
    // parameter 0 for 0 offset
}

function_set JsonExportFunction::getFunctionSet() {
    function_set functionSet;
    auto exportFunc = std::make_unique<ExportFunction>(name);
    exportFunc->bind = bindFunc;
    exportFunc->initLocalState = initLocalState;
    exportFunc->createSharedState = createSharedStateFunc;
    exportFunc->initSharedState = initSharedState;
    exportFunc->sink = sinkFunc;
    exportFunc->combine = combineFunc;
    exportFunc->finalize = finalizeFunc;
    functionSet.push_back(std::move(exportFunc));
    return functionSet;
}

} // namespace json_extension
} // namespace kuzu
