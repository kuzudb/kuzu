#include "binder/binder.h"
#include "extension/extension.h"
#include "extension/extension_manager.h"
#include "function/table/simple_table_functions.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct LoadedExtensionInfo {
    std::string name;
    extension::ExtensionSource extensionSource;
    std::string extensionPath;

    LoadedExtensionInfo(std::string name, extension::ExtensionSource extensionSource,
        std::string extensionPath)
        : name{std::move(name)}, extensionSource{extensionSource},
          extensionPath{std::move(extensionPath)} {}
};

struct ShowLoadedExtensionsBindData final : SimpleTableFuncBindData {
    std::vector<LoadedExtensionInfo> loadedExtensionInfo;

    ShowLoadedExtensionsBindData(std::vector<LoadedExtensionInfo> loadedExtensionInfo,
        binder::expression_vector columns, offset_t maxOffset)
        : SimpleTableFuncBindData{std::move(columns), maxOffset},
          loadedExtensionInfo{std::move(loadedExtensionInfo)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowLoadedExtensionsBindData>(*this);
    }
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    const auto sharedState = input.sharedState->ptrCast<SimpleTableFuncSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto& loadedExtensions =
        input.bindData->constPtrCast<ShowLoadedExtensionsBindData>()->loadedExtensionInfo;
    auto numTuplesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numTuplesToOutput; i++) {
        auto loadedExtension = loadedExtensions[morsel.startOffset + i];
        dataChunk.getValueVectorMutable(0).setValue(i, loadedExtension.name);
        dataChunk.getValueVectorMutable(1).setValue(i,
            extension::ExtensionSourceUtils::toString(loadedExtension.extensionSource));
        dataChunk.getValueVectorMutable(2).setValue(i, loadedExtension.extensionPath);
    }
    return numTuplesToOutput;
}

static binder::expression_vector bindColumns(binder::Binder& binder) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("extension name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("extension source");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("extension path");
    columnTypes.emplace_back(LogicalType::STRING());
    return binder.createVariables(columnNames, columnTypes);
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto loadedExtensions = context->getExtensionManager()->getLoadedExtensions();
    std::vector<LoadedExtensionInfo> loadedExtensionInfo;
    for (auto& loadedExtension : loadedExtensions) {
        loadedExtensionInfo.emplace_back(loadedExtension.getExtensionName(),
            loadedExtension.getSource(), loadedExtension.getFullPath());
    }
    return std::make_unique<ShowLoadedExtensionsBindData>(loadedExtensionInfo,
        bindColumns(*input->binder), loadedExtensionInfo.size());
}

function_set ShowLoadedExtensionsFunction::getFunctionSet() {
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
