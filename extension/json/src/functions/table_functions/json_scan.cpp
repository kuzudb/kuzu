#include "json_scan.h"

#include <regex>

#include "common/case_insensitive_map.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/json_common.h"
#include "common/string_utils.h"
#include "function/table/scan_functions.h"
#include "json_utils.h"
#include "processor/execution_context.h"
#include "processor/warning_context.h"
#include "reader/buffered_json_reader.h"

namespace kuzu {
namespace json_extension {

using namespace kuzu::function;
using namespace kuzu::common;

struct JsonScanBindData;

struct JsonScanConfig {
    JsonScanFormat format = JsonConstant::DEFAULT_JSON_FORMAT;
    int64_t depth = JsonConstant::DEFAULT_JSON_DETECT_DEPTH;
    int64_t breadth = JsonConstant::DEFAULT_JSON_DETECT_BREADTH;
    bool autoDetect = JsonConstant::DEFAULT_AUTO_DETECT_VALUE;

    explicit JsonScanConfig(const std::unordered_map<std::string, Value>& options);
};

JsonScanConfig::JsonScanConfig(const std::unordered_map<std::string, Value>& options) {
    for (const auto& i : options) {
        if (i.first == "FORMAT") {
            if (i.second.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
                throw BinderException("Format parameter must be a string.");
            }
            auto formatVal = StringUtils::getUpper(i.second.strVal);
            if (formatVal == "ARRAY") {
                format = JsonScanFormat::ARRAY;
            } else if (formatVal == "UNSTRUCTURED") {
                format = JsonScanFormat::UNSTRUCTURED;
            } else if (formatVal == "AUTO") {
                format = JsonScanFormat::AUTO_DETECT;
            } else {
                throw RuntimeException(
                    "Invalid JSON file format: Must either be 'unstructured' or 'array'");
            }
        } else if (i.first == "MAXIMUM_DEPTH") {
            if (i.second.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
                throw BinderException("Maximum depth parameter must be an int64.");
            }
            depth = i.second.val.int64Val;
        } else if (i.first == CopyConstants::IGNORE_ERRORS_OPTION_NAME) {
            // JSON reader currently doesn't do anything with IGNORE_ERRORS parameter
            if (i.second.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
                throw BinderException("IGNORE_ERRORS parameter must be a bool.");
            }
        } else if (i.first == "SAMPLE_SIZE") {
            if (i.second.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
                throw BinderException("Sample size parameter must be an int64.");
            }
            breadth = i.second.val.int64Val;
        } else if (i.first == "AUTO_DETECT") {
            if (i.second.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
                throw BinderException("Auto detect option must be a bool.");
            }
            autoDetect = i.second.val.booleanVal;
        } else {
            throw BinderException(stringFormat("Unrecognized parameter: {}", i.first));
        }
    }
}

struct JSONScanSharedState : public BaseScanSharedState {
    std::mutex lock;
    std::unique_ptr<BufferedJsonReader> jsonReader;
    uint64_t numRows;

    JSONScanSharedState(main::ClientContext& context, std::string fileName, JsonScanFormat format,
        uint64_t numRows)
        : BaseScanSharedState{}, jsonReader{std::make_unique<BufferedJsonReader>(context,
                                     std::move(fileName), BufferedJSONReaderOptions{format})},
          numRows{numRows} {}

    uint64_t getNumRows() const override { return numRows; }
};

struct JSONScanLocalState : public TableFuncLocalState {
    yyjson_doc* docs[DEFAULT_VECTOR_CAPACITY];
    BufferedJsonReader* currentReader = nullptr;
    JsonScanBufferHandle* currentBufferHandle = nullptr;
    bool isLast = false;
    uint64_t prevBufferRemainder = 0;
    std::unique_ptr<storage::MemoryBuffer> reconstructBuffer;
    uint8_t* bufferPtr = nullptr;
    idx_t bufferOffset = 0;
    uint64_t bufferSize = 0;
    uint64_t numValuesToOutput = 0;
    storage::MemoryManager& mm;

    JSONScanLocalState(storage::MemoryManager& mm, BufferedJsonReader* reader)
        : docs{}, currentReader{reader},
          reconstructBuffer{
              mm.allocateBuffer(false /* initializeToZero */, JsonConstant::SCAN_BUFFER_CAPACITY)},
          mm{mm} {}

    uint64_t readNext();
    bool readNextBuffer();
    bool readNextBufferInternal(uint64_t& bufferIdx, bool& fileDone);
    bool readNextBufferSeek(uint64_t& bufferIdx, bool& fileDone);
    void skipOverArrayStart();
    void parseNextChunk();
    void parseJson(uint8_t* jsonStart, uint64_t jsonSize, uint64_t remaining);
    bool reconstructFirstObject();
};

bool JSONScanLocalState::readNextBufferInternal(uint64_t& bufferIdx, bool& fileDone) {
    if (!readNextBufferSeek(bufferIdx, fileDone)) {
        return false;
    }
    bufferOffset = 0;
    return true;
}

bool JSONScanLocalState::readNextBufferSeek(uint64_t& bufferIdx, bool& fileDone) {
    auto fileHandle = currentReader->getFileHandle();
    auto requestSize =
        JsonConstant::SCAN_BUFFER_CAPACITY - prevBufferRemainder - YYJSON_PADDING_SIZE;
    uint64_t readPosition = 0;
    uint64_t readSize = 0;
    {
        std::lock_guard<std::mutex> mtx(currentReader->lock);
        if (fileHandle->isLastReadRequested()) {
            return false;
        }
        if (!fileHandle->getPositionAndSize(readPosition, readSize, requestSize)) {
            return false;
        }
        bufferIdx = currentReader->getBufferIdx();
        isLast = readSize == 0;
    }
    bufferSize = prevBufferRemainder + readSize;
    fileHandle->readAtPosition(bufferPtr + prevBufferRemainder, readSize, readPosition, fileDone);
    return true;
}

static void skipWhitespace(uint8_t* bufferPtr, idx_t& bufferOffset, const uint64_t& bufferSize) {
    for (; bufferOffset != bufferSize; bufferOffset++) {
        if (!common::StringUtils::isSpace(bufferPtr[bufferOffset])) {
            break;
        }
    }
}

void JSONScanLocalState::skipOverArrayStart() {
    // First read of this buffer, check if it's actually an array and skip over the bytes
    skipWhitespace(bufferPtr, bufferOffset, bufferSize);
    if (bufferOffset == bufferSize) {
        return; // Empty file
    }
    if (bufferPtr[bufferOffset] != '[') {
        throw Exception(common::stringFormat(
            "Expected top-level JSON array with format='array', but first character is '{}' in "
            "file \"{}\"."
            "\nTry setting format='auto' or format='newline_delimited'.",
            (reinterpret_cast<char*>(bufferPtr))[bufferOffset], currentReader->getFileName()));
    }
    skipWhitespace(bufferPtr, ++bufferOffset, bufferSize);
    if (bufferOffset >= bufferSize) {
        throw Exception(common::stringFormat(
            "Missing closing brace ']' in JSON array with format='array' in file \"{}\"",
            currentReader->getFileName()));
    }
    if (bufferPtr[bufferOffset] == ']') {
        skipWhitespace(bufferPtr, ++bufferOffset, bufferSize);
        if (bufferOffset != bufferSize) {
            throw Exception(common::stringFormat("Empty array with trailing data when parsing JSON "
                                                 "array with format='array' in file \"{}\"",
                currentReader->getFileName()));
        }
        return;
    }
}

static uint8_t* nextJsonDefault(uint8_t* ptr, const uint8_t* end) {
    uint64_t parents = 0;
    while (ptr != end) {
        switch (*ptr++) {
        case '{':
        case '[':
            parents++;
            continue;
        case '}':
        case ']':
            parents--;
            break;
        case '"':
            while (ptr != end) {
                auto strChar = *ptr++;
                if (strChar == '"') {
                    break;
                } else if (strChar == '\\') {
                    if (ptr != end) {
                        ptr++; // Skip the escaped char
                    }
                }
            }
            break;
        default:
            continue;
        }

        if (parents == 0) {
            break;
        }
    }

    return ptr;
}

static uint8_t* nextJson(uint8_t* ptr, uint64_t size) {
    auto end = ptr + size;
    switch (*ptr) {
    case '{':
    case '[':
    case '"':
        ptr = nextJsonDefault(ptr, end);
        break;
    default:
        // Special case: JSON array containing JSON without clear "parents", i.e., not obj/arr/str
        while (ptr != end) {
            switch (*ptr++) {
            case ',':
            case ']':
                ptr--;
                break;
            default:
                continue;
            }
            break;
        }
    }
    return ptr == end ? nullptr : ptr;
}

void JSONScanLocalState::parseJson(uint8_t* jsonStart, uint64_t size, uint64_t remaining) {
    yyjson_doc* doc = nullptr;
    yyjson_read_err err;
    doc = JSONCommon::readDocumentUnsafe(jsonStart, remaining, JSONCommon::READ_INSITU_FLAG, &err);
    if (err.code != YYJSON_READ_SUCCESS) {
        currentReader->throwParseError(err);
    }

    // We parse with YYJSON_STOP_WHEN_DONE, so we need to check this by hand
    idx_t numBytesRead = yyjson_doc_get_read_size(doc);
    if (numBytesRead > size) {
        // Can't go past the boundary, even with ignore_errors
        err.code = YYJSON_READ_ERROR_UNEXPECTED_END;
        err.msg = "unexpected end of data";
        err.pos = size;
        currentReader->throwParseError(err, "Try auto-detecting the JSON format");
    } else if (numBytesRead < size) {
        auto off = numBytesRead;
        auto rem = size;
        skipWhitespace(jsonStart, off, rem);
        if (off != rem) { // Between end of document and boundary should be whitespace only
            err.code = YYJSON_READ_ERROR_UNEXPECTED_CONTENT;
            err.msg = "unexpected content after document";
            err.pos = numBytesRead;
            currentReader->throwParseError(err, "Try auto-detecting the JSON format");
        }
    }
    if (!doc) {
        docs[numValuesToOutput] = nullptr;
        return;
    }
    docs[numValuesToOutput] = doc;
}

static uint8_t* nextNewLine(uint8_t* ptr, idx_t size) {
    return reinterpret_cast<uint8_t*>(memchr(ptr, '\n', size));
}

void JSONScanLocalState::parseNextChunk() {
    auto format = currentReader->getFormat();
    for (; numValuesToOutput < DEFAULT_VECTOR_CAPACITY; numValuesToOutput++) {
        skipWhitespace(bufferPtr, bufferOffset, bufferSize);
        auto jsonStart = bufferPtr + bufferOffset;
        auto remaining = bufferSize - bufferOffset;
        if (remaining == 0) {
            break;
        }
        KU_ASSERT(format != JsonScanFormat::AUTO_DETECT);
        auto jsonEnd = format == JsonScanFormat::NEWLINE_DELIMITED ?
                           nextNewLine(jsonStart, remaining) :
                           nextJson(jsonStart, remaining);
        if (jsonEnd == nullptr) {
            if (!isLast) {
                if (format != JsonScanFormat::NEWLINE_DELIMITED) {
                    memcpy(reconstructBuffer->getData(), jsonStart, remaining);
                    prevBufferRemainder = remaining;
                }
                bufferOffset = bufferSize;
                break;
            }
            jsonEnd = jsonStart + remaining;
        }
        auto jsonSize = jsonEnd - jsonStart;
        parseJson(jsonStart, jsonSize, remaining);
        bufferOffset += jsonSize;

        if (format == JsonScanFormat::ARRAY) {
            skipWhitespace(bufferPtr, bufferOffset, bufferSize);
            if (bufferPtr[bufferOffset] == ',' || bufferPtr[bufferOffset] == ']') {
                bufferOffset++;
            } else {
                yyjson_read_err err;
                err.code = YYJSON_READ_ERROR_UNEXPECTED_CHARACTER;
                err.msg = "unexpected character";
                err.pos = jsonSize;
                currentReader->throwParseError(err);
            }
        }
        skipWhitespace(bufferPtr, bufferOffset, bufferSize);
    }
}

static JsonScanFormat autoDetectFormat(uint8_t* buffer_ptr, uint64_t buffer_size) {
    auto lineEnd = nextNewLine(buffer_ptr, buffer_size);
    if (lineEnd != nullptr) {
        idx_t line_size = lineEnd - buffer_ptr;
        skipWhitespace(buffer_ptr, line_size, buffer_size);
        yyjson_read_err error;
        auto doc = yyjson_read_opts(reinterpret_cast<char*>(buffer_ptr), line_size,
            JSONCommon::READ_FLAG, nullptr /* alc */, &error);
        if (error.code == YYJSON_READ_SUCCESS) {
            if (yyjson_is_arr(doc->root) && line_size == buffer_size) {
                return JsonScanFormat::ARRAY;
            } else {
                return JsonScanFormat::NEWLINE_DELIMITED;
            }
        }
    }

    idx_t buffer_offset = 0;
    skipWhitespace(buffer_ptr, buffer_offset, buffer_size);
    auto remaining = buffer_size - buffer_offset;

    if (remaining == 0 || buffer_ptr[buffer_offset] == '{') {
        return JsonScanFormat::UNSTRUCTURED;
    }

    // We know it's not top-level records, if it's not '[', it's not ARRAY either
    if (buffer_ptr[buffer_offset] != '[') {
        return JsonScanFormat::UNSTRUCTURED;
    }

    // It's definitely an ARRAY, but now we have to figure out if there's more than one top-level
    // array
    yyjson_read_err error;
    auto doc = yyjson_read_opts(reinterpret_cast<char*>(buffer_ptr + buffer_offset), remaining,
        JSONCommon::READ_STOP_FLAG, nullptr /* alc */, &error);
    if (error.code == YYJSON_READ_SUCCESS) {
        KU_ASSERT(yyjson_is_arr(doc->root));
        buffer_offset += yyjson_doc_get_read_size(doc);
        skipWhitespace(buffer_ptr, buffer_offset, buffer_size);
        remaining = buffer_size - buffer_offset;
        if (remaining != 0) {
            return JsonScanFormat::UNSTRUCTURED;
        }
        return JsonScanFormat::ARRAY;
    }
    return JsonScanFormat::ARRAY;
}

bool JSONScanLocalState::readNextBuffer() {
    std::unique_ptr<storage::MemoryBuffer> buffer = nullptr;

    if (currentReader && currentBufferHandle) {
        if (--currentBufferHandle->numReaders == 0) {
            // If the buffer used before is no longer used, we can reuse that buffer instead of
            // doing a new memory allocation.
            buffer = currentReader->removeBuffer(*currentBufferHandle);
        }
    }

    if (buffer == nullptr) {
        buffer =
            mm.allocateBuffer(false /* initializeToZero */, JsonConstant::SCAN_BUFFER_CAPACITY);
        bufferPtr = buffer->getData();
    }

    if (currentReader && currentReader->getFormat() != JsonScanFormat::NEWLINE_DELIMITED &&
        !isLast) {
        memcpy(bufferPtr, reconstructBuffer->getData(), prevBufferRemainder);
    }

    uint64_t bufferIdx = 0;
    while (true) {
        if (currentReader) {
            bool doneRead = false;
            bool canRead = readNextBufferInternal(bufferIdx, doneRead);
            if (!isLast && canRead) {
                {
                    std::lock_guard<std::mutex> mtx(currentReader->lock);
                    if (currentReader->getFormat() == JsonScanFormat::AUTO_DETECT) {
                        currentReader->setFormat(autoDetectFormat(bufferPtr, bufferSize));
                    }
                }

                if (bufferIdx == 0 && currentReader->getFormat() == JsonScanFormat::ARRAY) {
                    skipOverArrayStart();
                }
            }
            if (canRead) {
                break;
            }
            currentReader = nullptr;
            currentBufferHandle = nullptr;
            isLast = false;
        }
        KU_ASSERT(!currentBufferHandle);
        return false;
    }

    auto numReaders =
        currentReader->getFormat() == JsonScanFormat::NEWLINE_DELIMITED && !isLast ? 2 : 1;
    auto jsonBufferHandle = std::make_unique<JsonScanBufferHandle>(bufferIdx, std::move(buffer),
        bufferSize, numReaders);
    currentBufferHandle = jsonBufferHandle.get();
    currentReader->insertBuffer(bufferIdx, std::move(jsonBufferHandle));
    prevBufferRemainder = 0;
    memset(bufferPtr + bufferSize, 0, YYJSON_PADDING_SIZE);
    return true;
}

static uint8_t* previousNewLine(uint8_t* ptr, uint64_t size) {
    auto end = ptr - size;
    for (ptr--; ptr != end; ptr--) {
        if (*ptr == '\n') {
            break;
        }
    }
    return ptr;
}

bool JSONScanLocalState::reconstructFirstObject() {
    KU_ASSERT(currentBufferHandle->bufferIdx != 0);
    KU_ASSERT(currentReader->getFormat() == JsonScanFormat::NEWLINE_DELIMITED);

    JsonScanBufferHandle* prevBufferHandle = nullptr;
    while (!prevBufferHandle) {
        prevBufferHandle = currentReader->getBuffer(currentBufferHandle->bufferIdx - 1);
    }

    auto prevBufferPtr = prevBufferHandle->buffer->getData() + prevBufferHandle->bufferSize;
    auto firstPart = previousNewLine(prevBufferPtr, prevBufferHandle->bufferSize);
    auto firstPartSize = prevBufferPtr - firstPart;

    auto reconstructBufferPtr = reconstructBuffer->getData();
    memcpy(reconstructBufferPtr, firstPart, firstPartSize);
    if (--prevBufferHandle->numReaders == 0) {
        currentReader->removeBuffer(*prevBufferHandle);
    }

    if (firstPartSize == 1) {
        return false;
    }

    auto lineSize = firstPartSize;
    if (bufferSize != 0) {
        auto lineEnd = nextNewLine(bufferPtr, bufferSize);
        if (lineEnd == nullptr) {
            // TODO(Ziyi): We should make the maximum object size as a configurable option.
            throw common::RuntimeException{
                common::stringFormat("Json object exceeds the maximum object size.")};
        } else {
            lineEnd++;
        }
        idx_t secondPartSize = lineEnd - bufferPtr;

        lineSize += secondPartSize;

        // And copy the remainder of the line to the reconstruct buffer
        memcpy(reconstructBufferPtr + firstPartSize, bufferPtr, secondPartSize);
        memset(reconstructBufferPtr + lineSize, 0, YYJSON_PADDING_SIZE);
        bufferOffset += secondPartSize;
    }

    parseJson(reconstructBufferPtr, lineSize, lineSize);
    return true;
}

uint64_t JSONScanLocalState::readNext() {
    numValuesToOutput = 0;
    while (numValuesToOutput == 0) {
        if (bufferOffset == bufferSize) {
            if (!readNextBuffer()) {
                break;
            }
            if (currentBufferHandle->bufferIdx != 0 &&
                currentReader->getFormat() == JsonScanFormat::NEWLINE_DELIMITED) {
                if (reconstructFirstObject()) {
                    numValuesToOutput++;
                }
            }
        }
        parseNextChunk();
    }
    return numValuesToOutput;
}

struct JsonScanBindData : public ScanBindData {
    case_insensitive_map_t<idx_t> colNameToIdx;
    JsonScanFormat format;

    JsonScanBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, ReaderConfig config, main::ClientContext* ctx,
        case_insensitive_map_t<idx_t> colNameToIdx, JsonScanFormat format)
        : ScanBindData(std::move(columnTypes), std::move(columnNames), 0 /* numWarningColumns */,
              std::move(config), ctx),
          colNameToIdx{std::move(colNameToIdx)}, format{format} {}

    uint64_t getFieldIdx(const std::string& fieldName) const;

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::unique_ptr<JsonScanBindData>(new JsonScanBindData(*this));
    }

private:
    JsonScanBindData(const JsonScanBindData& other)
        : ScanBindData{other}, colNameToIdx{other.colNameToIdx}, format{other.format} {}
};

uint64_t JsonScanBindData::getFieldIdx(const std::string& fieldName) const {
    auto normalizedName = fieldName;
    // TODO(Ziyi): this is a temporary fix for the json testing framework, since copy-to-json
    // always outputs the property name, so we have to remove the property name prefix for
    // matching.
    if (!colNameToIdx.contains(fieldName)) {
        std::regex pattern(R"(^[^.]+\.(.*))");
        std::smatch match;
        if (std::regex_match(fieldName, match, pattern) && match.size() > 1) {
            normalizedName = match[1]; // Return the part of the string after the "xxx."
        }
    }
    return colNameToIdx.contains(normalizedName) ? colNameToIdx.at(normalizedName) : UINT64_MAX;
}

static JsonScanFormat autoDetect(main::ClientContext* context, const std::string& filePath,
    JsonScanConfig& config, std::vector<common::LogicalType>& types,
    std::vector<std::string>& names, common::case_insensitive_map_t<idx_t>& colNameToIdx) {
    auto numRowsToDetect = config.breadth;
    JSONScanSharedState sharedState(*context, filePath, config.format, 0);
    JSONScanLocalState localState(*context->getMemoryManager(), sharedState.jsonReader.get());
    while (numRowsToDetect != 0) {
        auto numTuplesRead = localState.readNext();
        if (numTuplesRead == 0) {
            break;
        }
        auto next = std::min<uint64_t>(numTuplesRead, numRowsToDetect);
        yyjson_val *key = nullptr, *ele = nullptr;
        for (auto i = 0u; i < next; i++) {
            auto* doc = localState.docs[i];
            KU_ASSERT(nullptr != doc);
            auto objIter = yyjson_obj_iter_with(doc->root);
            while ((key = yyjson_obj_iter_next(&objIter))) {
                ele = yyjson_obj_iter_get_val(key);
                KU_ASSERT(yyjson_get_type(doc->root) == YYJSON_TYPE_OBJ);
                std::string fieldName = yyjson_get_str(key);
                if (!colNameToIdx.contains(fieldName)) {
                    std::regex pattern(R"(^[^.]+\.(.*))");
                    std::smatch match;
                    if (std::regex_match(fieldName, match, pattern) && match.size() > 1) {
                        fieldName = match[1];
                    }
                }
                idx_t colIdx = 0;
                if (colNameToIdx.contains(fieldName)) {
                    colIdx = colNameToIdx.at(fieldName);
                    types[colIdx] = LogicalTypeUtils::combineTypes(types[colIdx],
                        jsonSchema(ele, config.depth, config.breadth));
                } else {
                    colIdx = names.size();
                    colNameToIdx.emplace(fieldName, colIdx);
                    names.push_back(fieldName);
                    types.push_back(jsonSchema(ele, config.depth, config.breadth));
                }
            }
            yyjson_doc_free(doc);
            localState.docs[i] = nullptr;
        }
        numRowsToDetect -= next;
    }

    for (auto& type : types) {
        LogicalTypeUtils::purgeAny(type, LogicalType::STRING());
    }
    return sharedState.jsonReader->getFormat();
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput* scanInput) {
    std::vector<LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    JsonScanConfig scanConfig(scanInput->config.options);
    case_insensitive_map_t<idx_t> colNameToIdx;
    if (!scanInput->expectedColumnNames.empty() || !scanConfig.autoDetect) {
        if (scanInput->expectedColumnNames.empty()) {
            throw common::BinderException{
                "When auto-detect is set to false, Kuzu requires the "
                "user to provide column names and types in the LOAD FROM clause."};
        }
        columnTypes = copyVector(scanInput->expectedColumnTypes);
        columnNames = scanInput->expectedColumnNames;
        idx_t colIdx = 0;
        for (auto& columnName : columnNames) {
            colNameToIdx.emplace(columnName, colIdx++);
        }

        if (scanConfig.format == JsonScanFormat::AUTO_DETECT) {
            JSONScanSharedState sharedState(*context, scanInput->config.getFilePath(0),
                scanConfig.format, 0);
            JSONScanLocalState localState(*context->getMemoryManager(),
                sharedState.jsonReader.get());
            localState.readNext();
            scanConfig.format = sharedState.jsonReader->getFormat();
        }
    } else {
        scanConfig.format = autoDetect(context, scanInput->config.getFilePath(0), scanConfig,
            columnTypes, columnNames, colNameToIdx);
    }
    scanInput->tableFunction->canParallelFunc = [scanConfig]() {
        return scanConfig.format == JsonScanFormat::NEWLINE_DELIMITED;
    };
    return std::make_unique<JsonScanBindData>(std::move(columnTypes), std::move(columnNames),
        scanInput->config.copy(), context, std::move(colNameToIdx), scanConfig.format);
}

static offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto localState = input.localState->ptrCast<JSONScanLocalState>();
    auto bindData = input.bindData->constPtrCast<JsonScanBindData>();
    auto projectionSkips = bindData->getColumnSkips();
    for (auto& valueVector : output.dataChunk.valueVectors) {
        valueVector->setAllNull();
        valueVector->resetAuxiliaryBuffer();
    }
    auto count = localState->readNext();
    yyjson_doc** docs = localState->docs;
    yyjson_val *key = nullptr, *ele = nullptr;
    for (auto i = 0u; i < count; i++) {
        KU_ASSERT(nullptr != docs[i]);
        auto objIter = yyjson_obj_iter_with(docs[i]->root);
        while ((key = yyjson_obj_iter_next(&objIter))) {
            ele = yyjson_obj_iter_get_val(key);
            auto columnIdx = bindData->getFieldIdx(yyjson_get_str(key));
            if (columnIdx == UINT64_MAX || projectionSkips[columnIdx]) {
                continue;
            }
            readJsonToValueVector(ele, *output.dataChunk.valueVectors[columnIdx], i);
        }
        yyjson_doc_free(docs[i]);
        docs[i] = nullptr;
    }
    output.dataChunk.state->getSelVectorUnsafe().setSelSize(count);
    return count;
}

static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input) {
    auto jsonBindData = input.bindData->constPtrCast<JsonScanBindData>();
    return std::make_unique<JSONScanSharedState>(*jsonBindData->context,
        jsonBindData->config.filePaths[0], jsonBindData->format, 0);
}

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput& /*input*/,
    TableFuncSharedState* state, storage::MemoryManager* mm) {
    auto sharedState = state->ptrCast<JSONScanSharedState>();
    return std::make_unique<JSONScanLocalState>(*mm, sharedState->jsonReader.get());
}

static double progressFunc(TableFuncSharedState* /*state*/) {
    // TODO(Ziyi): fix this.
    return 0;
}

static void finalizeFunc(processor::ExecutionContext* ctx, TableFuncSharedState*,
    TableFuncLocalState*) {
    ctx->clientContext->getWarningContextUnsafe().defaultPopulateAllWarnings(ctx->queryID);
}

std::unique_ptr<TableFunction> JsonScan::getFunction() {
    auto func =
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState, initLocalState,
            progressFunc, std::vector<LogicalTypeID>{LogicalTypeID::STRING}, finalizeFunc);
    return func;
}

} // namespace json_extension
} // namespace kuzu
