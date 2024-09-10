#include "json_scan.h"

#include <regex>

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/json_common.h"
#include "common/string_utils.h"
#include "function/table/scan_functions.h"
#include "json_utils.h"
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

    explicit JsonScanConfig(const std::unordered_map<std::string, Value>& options);
};

JsonScanConfig::JsonScanConfig(const std::unordered_map<std::string, Value>& options) {
    for (const auto& i : options) {
        if (i.first == "FORMAT") {
            if (i.second.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
                throw BinderException("Format parameter must be a string.");
            }
            auto tmp = StringUtils::getUpper(i.second.strVal);
            if (tmp == "ARRAY") {
                format = JsonScanFormat::ARRAY;
            } else if (tmp == "UNSTRUCTURED") {
                format = JsonScanFormat::UNSTRUCTURED;
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
    yyjson_val* values[DEFAULT_VECTOR_CAPACITY];
    BufferedJsonReader* currentReader = nullptr;
    JsonScanBufferHandle* currentBufferHandle = nullptr;
    bool isLast = false;
    uint64_t prevBufferRemainder = 0;
    std::unique_ptr<storage::MemoryBuffer> reconstructBuffer;
    uint8_t* bufferPtr = nullptr;
    uint64_t bufferOffset = 0;
    uint64_t bufferSize = 0;
    uint64_t numValuesToOutput = 0;
    storage::MemoryManager& mm;

    JSONScanLocalState(storage::MemoryManager& mm, BufferedJsonReader* reader)
        : currentReader{reader}, reconstructBuffer{mm.allocateBuffer(false /* initializeToZero */,
                                     JsonConstant::SCAN_BUFFER_CAPACITY)},
          mm{mm} {}

    uint64_t readNext();
    bool readNextBuffer();
    bool readNextBufferInternal(uint64_t& bufferIdx, bool& fileDone);
    bool readNextBufferSeek(uint64_t& bufferIdx, bool& fileDone);
    static void skipWhitespace(uint8_t* bufferPtr, uint64_t& bufferOffset,
        const uint64_t& bufferSize);
    void skipOverArrayStart();
    void parseNextChunk();
    void parseJson(uint8_t* jsonStart, uint64_t jsonSize, uint64_t remaining);
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
    uint64_t readPosition;
    uint64_t readSize;
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

void JSONScanLocalState::skipWhitespace(uint8_t* bufferPtr, uint64_t& bufferOffset,
    const uint64_t& bufferSize) {
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
            "\n Try setting format='auto' or format='newline_delimited'.",
            bufferPtr[bufferOffset], currentReader->getFileName()));
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
    yyjson_doc* doc;
    yyjson_read_err err;
    doc = JSONCommon::readDocumentUnsafe(jsonStart, remaining, JSONCommon::READ_INSITU_FLAG, &err);
    if (err.code != YYJSON_READ_SUCCESS) {
        currentReader->throwParseError(err);
    }

    // We parse with YYJSON_STOP_WHEN_DONE, so we need to check this by hand
    auto numBytesRead = yyjson_doc_get_read_size(doc);
    if (numBytesRead > size) {
        // Can't go past the boundary, even with ignore_errors
        err.code = YYJSON_READ_ERROR_UNEXPECTED_END;
        err.msg = "unexpected end of data";
        err.pos = size;
        currentReader->throwParseError(err, "Try auto-detecting the JSON format");
    } else if (numBytesRead < size) {
        auto off = numBytesRead;
        auto rem = size;
        // skipWhiteSpaces(json_start, off, rem);
        if (off != rem) { // Between end of document and boundary should be whitespace only
            err.code = YYJSON_READ_ERROR_UNEXPECTED_CONTENT;
            err.msg = "unexpected content after document";
            err.pos = numBytesRead;
            currentReader->throwParseError(err, "Try auto-detecting the JSON format");
        }
    }
    if (!doc) {
        values[numValuesToOutput] = nullptr;
        return;
    }
    values[numValuesToOutput] = doc->root;
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
        auto jsonEnd = nextJson(jsonStart, remaining);
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

bool JSONScanLocalState::readNextBuffer() {
    auto buffer =
        mm.allocateBuffer(false /* initializeToZero */, JsonConstant::SCAN_BUFFER_CAPACITY);
    bufferPtr = buffer->getData();
    if (currentReader && currentReader->getFormat() != JsonScanFormat::NEWLINE_DELIMITED &&
        !isLast) {
        memcpy(bufferPtr, reconstructBuffer->getData(), prevBufferRemainder);
    }

    uint64_t bufferIdx;
    while (true) {
        if (currentReader) {
            bool doneRead = false;
            bool canRead = readNextBufferInternal(bufferIdx, doneRead);
            if (!isLast && canRead) {
                // We read something
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
    auto jsonBufferHandle =
        std::make_unique<JsonScanBufferHandle>(bufferIdx, std::move(buffer), bufferSize);
    currentBufferHandle = jsonBufferHandle.get();
    currentReader->insertBuffer(bufferIdx, std::move(jsonBufferHandle));
    prevBufferRemainder = 0;
    memset(bufferPtr + bufferSize, 0, YYJSON_PADDING_SIZE);
    return true;
}

uint64_t JSONScanLocalState::readNext() {
    numValuesToOutput = 0;
    while (numValuesToOutput == 0) {
        if (bufferOffset == bufferSize) {
            if (!readNextBuffer()) {
                break;
            }
        }
        parseNextChunk();
    }
    return numValuesToOutput;
}

struct JsonScanBindData : public ScanBindData {
    case_insensitive_map_t<uint64_t> colNameToIdx;
    bool scanFromList;
    bool scanFromStruct;
    JsonScanFormat format;
    uint64_t numRows;

    JsonScanBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, ReaderConfig config, main::ClientContext* ctx,
        case_insensitive_map_t<uint64_t> colNameToIdx, bool scanFromList, bool scanFromStruct,
        JsonScanFormat format, uint64_t numRows)
        : ScanBindData(std::move(columnTypes), std::move(columnNames), std::move(config), ctx),
          colNameToIdx{std::move(colNameToIdx)}, scanFromList{scanFromList},
          scanFromStruct{scanFromStruct}, format{format}, numRows{numRows} {}

    uint64_t getFieldIdx(const std::string& fieldName) const;

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::unique_ptr<JsonScanBindData>(new JsonScanBindData(*this));
    }

private:
    JsonScanBindData(const JsonScanBindData& other)
        : ScanBindData{other}, colNameToIdx{other.colNameToIdx}, scanFromList{other.scanFromList},
          scanFromStruct{other.scanFromStruct}, format{other.format}, numRows{other.numRows} {}
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

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput* scanInput) {
    std::vector<LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    JsonScanConfig scanConfig(scanInput->config.options);
    auto parsedJson = fileToJson(context, scanInput->inputs[0].strVal, scanConfig.format);
    auto scanFromList = false;
    auto scanFromStruct = false;
    if (scanInput->expectedColumnNames.size() > 0) {
        columnTypes = copyVector(scanInput->expectedColumnTypes);
        columnNames = scanInput->expectedColumnNames;
        // still need determine scanning mode.
        if (scanConfig.depth == -1 || scanConfig.depth > 3) {
            scanConfig.depth = 3;
        }
        auto schema = LogicalTypeUtils::purgeAny(
            jsonSchema(parsedJson, scanConfig.depth, scanConfig.breadth), LogicalType::STRING());
        if (schema.getLogicalTypeID() == LogicalTypeID::LIST) {
            schema = ListType::getChildType(schema).copy();
            scanFromList = true;
        }
        if (schema.getLogicalTypeID() == LogicalTypeID::STRUCT) {
            scanFromStruct = true;
        }
    } else {
        auto schema = LogicalTypeUtils::purgeAny(
            jsonSchema(parsedJson, scanConfig.depth, scanConfig.breadth), LogicalType::STRING());
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
    }
    case_insensitive_map_t<uint64_t> colNameToIdx;
    for (auto i = 0u; i < columnNames.size(); i++) {
        colNameToIdx.emplace(columnNames[i], i);
    }
    return std::make_unique<JsonScanBindData>(std::move(columnTypes), std::move(columnNames),
        scanInput->config.copy(), context, std::move(colNameToIdx), scanFromList, scanFromStruct,
        scanConfig.format, 0);
}

static offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto localState = input.localState->ptrCast<JSONScanLocalState>();
    auto bindData = input.bindData->constPtrCast<JsonScanBindData>();
    auto projectionSkips = bindData->getColumnSkips();
    for (auto& valueVector : output.dataChunk.valueVectors) {
        valueVector->setAllNull();
    }
    auto count = localState->readNext();
    yyjson_val** values = localState->values;
    yyjson_val *key, *ele;
    for (auto i = 0u; i < count; i++) {
        auto objIter = yyjson_obj_iter_with(values[i]);
        while ((key = yyjson_obj_iter_next(&objIter))) {
            ele = yyjson_obj_iter_get_val(key);
            auto columnIdx = bindData->getFieldIdx(yyjson_get_str(key));
            if (columnIdx == UINT64_MAX || projectionSkips[columnIdx]) {
                continue;
            }
            readJsonToValueVector(ele, *output.dataChunk.valueVectors[columnIdx], i);
        }
    }
    output.dataChunk.state->getSelVectorUnsafe().setSelSize(count);
    return count;
}

static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input) {
    auto jsonBindData = input.bindData->constPtrCast<JsonScanBindData>();
    return std::make_unique<JSONScanSharedState>(*jsonBindData->context,
        jsonBindData->config.filePaths[0], jsonBindData->format, jsonBindData->numRows);
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

std::unique_ptr<TableFunction> JsonScan::getFunction() {
    auto func = std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState,
        initLocalState, progressFunc, std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    func->canParallelFunc = []() { return false; };
    return func;
}

} // namespace json_extension
} // namespace kuzu
