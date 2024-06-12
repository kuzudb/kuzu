#include <fcntl.h>

#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_serializer.h"
#include "function/cast/vector_cast_functions.h"
#include "function/copy/copy_function.h"
#include "function/scalar_function.h"
#include "main/client_context.h"

namespace kuzu {
namespace function {

using namespace common;

struct CopyToCSVBindData : public CopyFuncBindData {
    CSVOption copyToOption;

    CopyToCSVBindData(std::vector<std::string> names, std::string fileName, bool canParallel,
        CSVOption copyToOption)
        : CopyFuncBindData{std::move(names), std::move(fileName), canParallel},
          copyToOption{std::move(copyToOption)} {}

    std::unique_ptr<CopyFuncBindData> copy() const override {
        auto bindData =
            std::make_unique<CopyToCSVBindData>(names, fileName, canParallel, copyToOption.copy());
        bindData->types = types;
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

static bool requireQuotes(const CopyToCSVBindData& copyToCSVBindData, const uint8_t* str,
    uint64_t len) {
    // Check if the string is equal to the null string.
    if (len == strlen(CopyToCSVConstants::DEFAULT_NULL_STR) &&
        memcmp(str, CopyToCSVConstants::DEFAULT_NULL_STR, len) == 0) {
        return true;
    }
    for (auto i = 0u; i < len; i++) {
        if (str[i] == copyToCSVBindData.copyToOption.quoteChar ||
            str[i] == CopyToCSVConstants::DEFAULT_CSV_NEWLINE[0] ||
            str[i] == copyToCSVBindData.copyToOption.delimiter) {
            return true;
        }
    }
    return false;
}

static void writeString(BufferedSerializer* serializer, const CopyFuncBindData& bindData,
    const uint8_t* strData, uint64_t strLen, bool forceQuote) {
    auto& copyToCSVBindData = bindData.constCast<CopyToCSVBindData>();
    if (!forceQuote) {
        forceQuote = requireQuotes(copyToCSVBindData, strData, strLen);
    }
    if (forceQuote) {
        bool requiresEscape = false;
        for (auto i = 0u; i < strLen; i++) {
            if (strData[i] == copyToCSVBindData.copyToOption.quoteChar ||
                strData[i] == copyToCSVBindData.copyToOption.escapeChar) {
                requiresEscape = true;
                break;
            }
        }

        if (!requiresEscape) {
            serializer->writeBufferData(copyToCSVBindData.copyToOption.quoteChar);
            serializer->write(strData, strLen);
            serializer->writeBufferData(copyToCSVBindData.copyToOption.quoteChar);
            return;
        }

        std::string strValToWrite = std::string(reinterpret_cast<const char*>(strData), strLen);
        strValToWrite = addEscapes(copyToCSVBindData.copyToOption.escapeChar,
            copyToCSVBindData.copyToOption.escapeChar, strValToWrite);
        if (copyToCSVBindData.copyToOption.escapeChar != copyToCSVBindData.copyToOption.quoteChar) {
            strValToWrite = addEscapes(copyToCSVBindData.copyToOption.quoteChar,
                copyToCSVBindData.copyToOption.escapeChar, strValToWrite);
        }
        serializer->writeBufferData(copyToCSVBindData.copyToOption.quoteChar);
        serializer->writeBufferData(strValToWrite);
        serializer->writeBufferData(copyToCSVBindData.copyToOption.quoteChar);
    } else {
        serializer->write(strData, strLen);
    }
}

struct CopyToCSVSharedState : public CopyFuncSharedState {
    std::mutex mtx;
    std::unique_ptr<FileInfo> fileInfo;
    offset_t offset = 0;

    CopyToCSVSharedState(main::ClientContext& context, const CopyFuncBindData& bindData) {
        fileInfo = context.getVFSUnsafe()->openFile(bindData.fileName, O_WRONLY | O_CREAT | O_TRUNC,
            &context);
        writeHeader(bindData);
    }

    void writeHeader(const CopyFuncBindData& bindData) {
        auto& copyToCSVBindData = bindData.constCast<CopyToCSVBindData>();
        BufferedSerializer bufferedSerializer;
        if (copyToCSVBindData.copyToOption.hasHeader) {
            for (auto i = 0u; i < copyToCSVBindData.names.size(); i++) {
                if (i != 0) {
                    bufferedSerializer.writeBufferData(copyToCSVBindData.copyToOption.delimiter);
                }
                auto& name = copyToCSVBindData.names[i];
                writeString(&bufferedSerializer, copyToCSVBindData,
                    reinterpret_cast<const uint8_t*>(name.c_str()), name.length(),
                    false /* forceQuote */);
            }
            bufferedSerializer.writeBufferData(CopyToCSVConstants::DEFAULT_CSV_NEWLINE);
            writeRows(bufferedSerializer.getBlobData(), bufferedSerializer.getSize());
        }
    }

    void writeRows(const uint8_t* data, uint64_t size) {
        std::lock_guard<std::mutex> lck(mtx);
        fileInfo->writeFile(data, size, offset);
        offset += size;
    }
};

struct CopyToCSVLocalState final : public CopyFuncLocalState {
    std::unique_ptr<BufferedSerializer> serializer;
    std::unique_ptr<DataChunk> unflatCastDataChunk;
    std::unique_ptr<DataChunk> flatCastDataChunk;
    std::vector<ValueVector*> castVectors;
    std::vector<function::scalar_func_exec_t> castFuncs;

    CopyToCSVLocalState(main::ClientContext& context, const CopyFuncBindData& bindData,
        std::vector<bool> isFlatVec) {
        auto& copyToCSVBindData = bindData.constCast<CopyToCSVBindData>();
        serializer = std::make_unique<BufferedSerializer>();
        auto numFlatVectors = std::count(isFlatVec.begin(), isFlatVec.end(), true /* isFlat */);
        unflatCastDataChunk = std::make_unique<DataChunk>(isFlatVec.size() - numFlatVectors);
        flatCastDataChunk = std::make_unique<DataChunk>(numFlatVectors,
            DataChunkState::getSingleValueDataChunkState());
        uint64_t numInsertedFlatVector = 0;
        castFuncs.resize(copyToCSVBindData.types.size());
        for (auto i = 0u; i < copyToCSVBindData.types.size(); i++) {
            castFuncs[i] = function::CastFunction::bindCastFunction("cast",
                copyToCSVBindData.types[i], *LogicalType::STRING())
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

static std::unique_ptr<CopyFuncBindData> bindFunc(CopyFuncBindInput& bindInput) {
    return std::make_unique<CopyToCSVBindData>(bindInput.columnNames, bindInput.filePath,
        bindInput.canParallel,
        CSVReaderConfig::construct(std::move(bindInput.parsingOptions)).option.copy());
}

static std::unique_ptr<CopyFuncLocalState> initLocalState(main::ClientContext& context,
    const CopyFuncBindData& bindData, std::vector<bool> isFlatVec) {
    return std::make_unique<CopyToCSVLocalState>(context, bindData, isFlatVec);
}

static std::shared_ptr<CopyFuncSharedState> initSharedState(main::ClientContext& context,
    const CopyFuncBindData& bindData) {
    return std::make_shared<CopyToCSVSharedState>(context, bindData);
}

static void writeRows(const CopyToCSVBindData& copyToCSVBindData, CopyToCSVLocalState& localState,
    std::vector<std::shared_ptr<ValueVector>> inputVectors) {
    auto& copyToLocalState = localState.cast<CopyToCSVLocalState>();
    auto& castVectors = localState.castVectors;
    auto& serializer = localState.serializer;
    for (auto i = 0u; i < inputVectors.size(); i++) {
        auto vectorToCast = {inputVectors[i]};
        copyToLocalState.castFuncs[i](vectorToCast, *castVectors[i], nullptr /* dataPtr */);
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
                serializer->writeBufferData(copyToCSVBindData.copyToOption.delimiter);
            }
            auto vector = castVectors[j];
            auto pos = vector->state->isFlat() ? vector->state->getSelVector()[0] :
                                                 vector->state->getSelVector()[i];
            if (vector->isNull(pos)) {
                // write null value
                serializer->writeBufferData(CopyToCSVConstants::DEFAULT_NULL_STR);
                continue;
            }
            auto strValue = vector->getValue<ku_string_t>(pos);
            // Note: we need blindly add quotes to LIST.
            writeString(serializer.get(), copyToCSVBindData, strValue.getData(), strValue.len,
                CopyToCSVConstants::DEFAULT_FORCE_QUOTE ||
                    inputVectors[j]->dataType.getLogicalTypeID() == LogicalTypeID::LIST);
        }
        serializer->writeBufferData(CopyToCSVConstants::DEFAULT_CSV_NEWLINE);
    }
}

static void sinkFunc(CopyFuncSharedState& sharedState, CopyFuncLocalState& localState,
    const CopyFuncBindData& bindData, std::vector<std::shared_ptr<ValueVector>> inputVectors) {
    auto& copyToCSVLocalState = localState.cast<CopyToCSVLocalState>();
    auto& copyToCSVBindData = bindData.constCast<CopyToCSVBindData>();
    writeRows(copyToCSVBindData, copyToCSVLocalState, std::move(inputVectors));
    auto& serializer = copyToCSVLocalState.serializer;
    if (serializer->getSize() > CopyToCSVConstants::DEFAULT_CSV_FLUSH_SIZE) {
        auto& copyToSharedState = sharedState.cast<CopyToCSVSharedState>();
        copyToSharedState.writeRows(serializer->getBlobData(), serializer->getSize());
        serializer->reset();
    }
}

static void combineFunc(CopyFuncSharedState& sharedState, CopyFuncLocalState& localState) {
    auto& serializer = localState.cast<CopyToCSVLocalState>().serializer;
    auto& copyToSharedState = sharedState.cast<CopyToCSVSharedState>();
    if (serializer->getSize() > 0) {
        copyToSharedState.writeRows(serializer->getBlobData(), serializer->getSize());
        serializer->reset();
    }
}

static void finalizeFunc(CopyFuncSharedState&) {}

function_set CopyCSVFunction::getFunctionSet() {
    function_set functionSet;
    auto copyFunc = std::make_unique<CopyFunction>(name);
    copyFunc->copyToBind = bindFunc;
    copyFunc->copyToInitLocal = initLocalState;
    copyFunc->copyToInitShared = initSharedState;
    copyFunc->copyToSink = sinkFunc;
    copyFunc->copyToCombine = combineFunc;
    copyFunc->copyToFinalize = finalizeFunc;
    functionSet.push_back(std::move(copyFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
