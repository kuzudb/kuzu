#include <fcntl.h>

#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_serializer.h"
#include "function/cast/vector_cast_functions.h"
#include "function/export/export_function.h"
#include "function/scalar_function.h"
#include "main/client_context.h"

namespace kuzu {
namespace function {

using namespace common;

struct ExportCSVBindData : public ExportFuncBindData {
    CSVOption exportOption;

    ExportCSVBindData(std::vector<std::string> names, std::string fileName, CSVOption exportOption)
        : ExportFuncBindData{std::move(names), std::move(fileName)},
          exportOption{std::move(exportOption)} {}

    std::unique_ptr<ExportFuncBindData> copy() const override {
        auto bindData = std::make_unique<ExportCSVBindData>(names, fileName, exportOption.copy());
        bindData->types = LogicalType::copy(types);
        return bindData;
    }
};

static std::string addEscapes(char toEscape, char escape, const std::string& val) {
    uint64_t i = 0;
    std::string escapedStr = "";
    auto found = val.find(toEscape);

    while (found != std::string::npos) {
        while (i < found) {
            escapedStr += val[i];
            i++;
        }
        escapedStr += escape;
        found = val.find(toEscape, found + sizeof(escape));
    }
    while (i < val.length()) {
        escapedStr += val[i];
        i++;
    }
    return escapedStr;
}

static bool requireQuotes(const ExportCSVBindData& exportCSVBindData, const uint8_t* str,
    uint64_t len) {
    // Check if the string is equal to the null string.
    if (len == strlen(ExportCSVConstants::DEFAULT_NULL_STR) &&
        memcmp(str, ExportCSVConstants::DEFAULT_NULL_STR, len) == 0) {
        return true;
    }
    for (auto i = 0u; i < len; i++) {
        if (str[i] == exportCSVBindData.exportOption.quoteChar ||
            str[i] == ExportCSVConstants::DEFAULT_CSV_NEWLINE[0] ||
            str[i] == exportCSVBindData.exportOption.delimiter) {
            return true;
        }
    }
    return false;
}

static void writeString(BufferedSerializer* serializer, const ExportFuncBindData& bindData,
    const uint8_t* strData, uint64_t strLen, bool forceQuote) {
    auto& exportCSVBindData = bindData.constCast<ExportCSVBindData>();
    if (!forceQuote) {
        forceQuote = requireQuotes(exportCSVBindData, strData, strLen);
    }
    if (forceQuote) {
        bool requiresEscape = false;
        for (auto i = 0u; i < strLen; i++) {
            if (strData[i] == exportCSVBindData.exportOption.quoteChar ||
                strData[i] == exportCSVBindData.exportOption.escapeChar) {
                requiresEscape = true;
                break;
            }
        }

        if (!requiresEscape) {
            serializer->writeBufferData(exportCSVBindData.exportOption.quoteChar);
            serializer->write(strData, strLen);
            serializer->writeBufferData(exportCSVBindData.exportOption.quoteChar);
            return;
        }

        std::string strValToWrite = std::string(reinterpret_cast<const char*>(strData), strLen);
        strValToWrite = addEscapes(exportCSVBindData.exportOption.escapeChar,
            exportCSVBindData.exportOption.escapeChar, strValToWrite);
        if (exportCSVBindData.exportOption.escapeChar != exportCSVBindData.exportOption.quoteChar) {
            strValToWrite = addEscapes(exportCSVBindData.exportOption.quoteChar,
                exportCSVBindData.exportOption.escapeChar, strValToWrite);
        }
        serializer->writeBufferData(exportCSVBindData.exportOption.quoteChar);
        serializer->writeBufferData(strValToWrite);
        serializer->writeBufferData(exportCSVBindData.exportOption.quoteChar);
    } else {
        serializer->write(strData, strLen);
    }
}

struct ExportCSVSharedState : public ExportFuncSharedState {
    std::mutex mtx;
    std::unique_ptr<FileInfo> fileInfo;
    offset_t offset = 0;

    ExportCSVSharedState(main::ClientContext& context, const ExportFuncBindData& bindData) {
        fileInfo = context.getVFSUnsafe()->openFile(bindData.fileName, O_WRONLY | O_CREAT | O_TRUNC,
            &context);
        writeHeader(bindData);
    }

    void writeHeader(const ExportFuncBindData& bindData) {
        auto& exportCSVBindData = bindData.constCast<ExportCSVBindData>();
        BufferedSerializer bufferedSerializer;
        if (exportCSVBindData.exportOption.hasHeader) {
            for (auto i = 0u; i < exportCSVBindData.names.size(); i++) {
                if (i != 0) {
                    bufferedSerializer.writeBufferData(exportCSVBindData.exportOption.delimiter);
                }
                auto& name = exportCSVBindData.names[i];
                writeString(&bufferedSerializer, exportCSVBindData,
                    reinterpret_cast<const uint8_t*>(name.c_str()), name.length(),
                    false /* forceQuote */);
            }
            bufferedSerializer.writeBufferData(ExportCSVConstants::DEFAULT_CSV_NEWLINE);
            writeRows(bufferedSerializer.getBlobData(), bufferedSerializer.getSize());
        }
    }

    void writeRows(const uint8_t* data, uint64_t size) {
        std::lock_guard<std::mutex> lck(mtx);
        fileInfo->writeFile(data, size, offset);
        offset += size;
    }
};

struct ExportCSVLocalState final : public ExportFuncLocalState {
    std::unique_ptr<BufferedSerializer> serializer;
    std::unique_ptr<DataChunk> unflatCastDataChunk;
    std::unique_ptr<DataChunk> flatCastDataChunk;
    std::vector<ValueVector*> castVectors;
    std::vector<function::scalar_func_exec_t> castFuncs;

    ExportCSVLocalState(main::ClientContext& context, const ExportFuncBindData& bindData,
        std::vector<bool> isFlatVec) {
        auto& exportCSVBindData = bindData.constCast<ExportCSVBindData>();
        serializer = std::make_unique<BufferedSerializer>();
        auto numFlatVectors = std::count(isFlatVec.begin(), isFlatVec.end(), true /* isFlat */);
        unflatCastDataChunk = std::make_unique<DataChunk>(isFlatVec.size() - numFlatVectors);
        flatCastDataChunk = std::make_unique<DataChunk>(numFlatVectors,
            DataChunkState::getSingleValueDataChunkState());
        uint64_t numInsertedFlatVector = 0;
        castFuncs.resize(exportCSVBindData.types.size());
        for (auto i = 0u; i < exportCSVBindData.types.size(); i++) {
            castFuncs[i] = function::CastFunction::bindCastFunction("cast",
                exportCSVBindData.types[i], LogicalType::STRING())
                               ->execFunc;
            auto castVector =
                std::make_unique<ValueVector>(LogicalTypeID::STRING, context.getMemoryManager());
            castVectors.push_back(castVector.get());
            if (isFlatVec[i]) {
                flatCastDataChunk->insert(numInsertedFlatVector, std::move(castVector));
                numInsertedFlatVector++;
            } else {
                unflatCastDataChunk->insert(i - numInsertedFlatVector, std::move(castVector));
            }
        }
    }
};

static std::unique_ptr<ExportFuncBindData> bindFunc(ExportFuncBindInput& bindInput) {
    return std::make_unique<ExportCSVBindData>(bindInput.columnNames, bindInput.filePath,
        CSVReaderConfig::construct(std::move(bindInput.parsingOptions)).option.copy());
}

static std::unique_ptr<ExportFuncLocalState> initLocalState(main::ClientContext& context,
    const ExportFuncBindData& bindData, std::vector<bool> isFlatVec) {
    return std::make_unique<ExportCSVLocalState>(context, bindData, isFlatVec);
}

static std::shared_ptr<ExportFuncSharedState> initSharedState(main::ClientContext& context,
    const ExportFuncBindData& bindData) {
    return std::make_shared<ExportCSVSharedState>(context, bindData);
}

static void writeRows(const ExportCSVBindData& exportCSVBindData, ExportCSVLocalState& localState,
    std::vector<std::shared_ptr<ValueVector>> inputVectors) {
    auto& exportCSVLocalState = localState.cast<ExportCSVLocalState>();
    auto& castVectors = localState.castVectors;
    auto& serializer = localState.serializer;
    for (auto i = 0u; i < inputVectors.size(); i++) {
        auto vectorToCast = {inputVectors[i]};
        exportCSVLocalState.castFuncs[i](vectorToCast, *castVectors[i], nullptr /* dataPtr */);
    }

    uint64_t numRowsToWrite = 1;
    for (auto& vectorToCast : inputVectors) {
        if (!vectorToCast->state->isFlat()) {
            numRowsToWrite = vectorToCast->state->getSelVector().getSelSize();
            break;
        }
    }
    for (auto i = 0u; i < numRowsToWrite; i++) {
        for (auto j = 0u; j < castVectors.size(); j++) {
            if (j != 0) {
                serializer->writeBufferData(exportCSVBindData.exportOption.delimiter);
            }
            auto vector = castVectors[j];
            auto pos = vector->state->isFlat() ? vector->state->getSelVector()[0] :
                                                 inputVectors[j]->state->getSelVector()[i];
            if (vector->isNull(pos)) {
                // write null value
                serializer->writeBufferData(ExportCSVConstants::DEFAULT_NULL_STR);
                continue;
            }
            auto strValue = vector->getValue<ku_string_t>(pos);
            // Note: we need blindly add quotes to LIST.
            writeString(serializer.get(), exportCSVBindData, strValue.getData(), strValue.len,
                ExportCSVConstants::DEFAULT_FORCE_QUOTE ||
                    inputVectors[j]->dataType.getLogicalTypeID() == LogicalTypeID::LIST);
        }
        serializer->writeBufferData(ExportCSVConstants::DEFAULT_CSV_NEWLINE);
    }
}

static void sinkFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState,
    const ExportFuncBindData& bindData, std::vector<std::shared_ptr<ValueVector>> inputVectors) {
    auto& exportCSVLocalState = localState.cast<ExportCSVLocalState>();
    auto& exportCSVBindData = bindData.constCast<ExportCSVBindData>();
    writeRows(exportCSVBindData, exportCSVLocalState, std::move(inputVectors));
    auto& serializer = exportCSVLocalState.serializer;
    if (serializer->getSize() > ExportCSVConstants::DEFAULT_CSV_FLUSH_SIZE) {
        auto& exportCSVSharedState = sharedState.cast<ExportCSVSharedState>();
        exportCSVSharedState.writeRows(serializer->getBlobData(), serializer->getSize());
        serializer->reset();
    }
}

static void combineFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState) {
    auto& serializer = localState.cast<ExportCSVLocalState>().serializer;
    auto& exportCSVSharedState = sharedState.cast<ExportCSVSharedState>();
    if (serializer->getSize() > 0) {
        exportCSVSharedState.writeRows(serializer->getBlobData(), serializer->getSize());
        serializer->reset();
    }
}

static void finalizeFunc(ExportFuncSharedState&) {}

function_set ExportCSVFunction::getFunctionSet() {
    function_set functionSet;
    auto exportFunc = std::make_unique<ExportFunction>(name);
    exportFunc->bind = bindFunc;
    exportFunc->initLocal = initLocalState;
    exportFunc->initShared = initSharedState;
    exportFunc->sink = sinkFunc;
    exportFunc->combine = combineFunc;
    exportFunc->finalize = finalizeFunc;
    functionSet.push_back(std::move(exportFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
