#include "json_scan.h"

#include <climits>
#include <cstdint>
#include <string_view>

#include "binder/binder.h"
#include "common/case_insensitive_map.h"
#include "common/copy_constructors.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/fast_mem.h"
#include "common/json_common.h"
#include "common/string_utils.h"
#include "function/table/bind_data.h"
#include "function/table/scan_file_function.h"
#include "json_utils.h"
#include "main/json_extension.h"
#include "processor/execution_context.h"
#include "processor/operator/persistent/reader/file_error_handler.h"
#include "processor/warning_context.h"
#include "reader/buffered_json_reader.h"

namespace kuzu {
namespace json_extension {

using namespace kuzu::function;
using namespace kuzu::common;

struct JsonScanBindData;

struct JSONWarningSourceData {
    JSONWarningSourceData() = default;
    static JSONWarningSourceData constructFrom(const processor::WarningSourceData& warningData);

    uint64_t startByteOffset;
    uint64_t endByteOffset;
    uint64_t blockIdx;
    uint32_t offsetInBlock;
};

JSONWarningSourceData JSONWarningSourceData::constructFrom(
    const processor::WarningSourceData& warningData) {
    KU_ASSERT(warningData.numValues == JsonConstant::JSON_WARNING_DATA_NUM_COLUMNS);

    JSONWarningSourceData ret{};
    warningData.dumpTo(ret.blockIdx, ret.offsetInBlock, ret.startByteOffset, ret.endByteOffset);
    return ret;
}

struct JsonScanConfig {
    JsonScanFormat format = JsonConstant::DEFAULT_JSON_FORMAT;
    int64_t depth = JsonConstant::DEFAULT_JSON_DETECT_DEPTH;
    int64_t breadth = JsonConstant::DEFAULT_JSON_DETECT_BREADTH;
    bool autoDetect = JsonConstant::DEFAULT_AUTO_DETECT_VALUE;

    explicit JsonScanConfig(const case_insensitive_map_t<Value>& options);
};

JsonScanConfig::JsonScanConfig(const case_insensitive_map_t<Value>& options) {
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

struct JSONScanSharedState : public TableFuncSharedState {
    std::mutex lock;
    std::unique_ptr<BufferedJsonReader> jsonReader;
    processor::populate_func_t populateErrorFunc;
    processor::SharedFileErrorHandler sharedErrorHandler;

    JSONScanSharedState(main::ClientContext& context, std::string fileName, JsonScanFormat format)
        : TableFuncSharedState{0 /* numRows */},
          jsonReader{std::make_unique<BufferedJsonReader>(context, std::move(fileName),
              BufferedJSONReaderOptions{format})},
          populateErrorFunc(constructPopulateFunc()),
          sharedErrorHandler(JsonExtension::JSON_SCAN_FILE_IDX, &lock, populateErrorFunc) {}

    processor::populate_func_t constructPopulateFunc() const {
        return [this](const processor::CopyFromFileError& error,
                   [[maybe_unused]] idx_t fileIdx) -> processor::PopulatedCopyFromError {
            KU_ASSERT(fileIdx == JsonExtension::JSON_SCAN_FILE_IDX);
            const auto warningData = JSONWarningSourceData::constructFrom(error.warningData);
            const auto lineNumber =
                sharedErrorHandler.getLineNumber(warningData.blockIdx, warningData.offsetInBlock);
            const char* incompleteLineSuffix = error.completedLine ? "" : "...";
            return processor::PopulatedCopyFromError{
                .message = StringUtils::rtrim(error.message),
                .filePath = jsonReader->getFileName(),
                .skippedLineOrRecord = jsonReader->reconstructLine(warningData.startByteOffset,
                                           warningData.endByteOffset) +
                                       incompleteLineSuffix,
                .lineNumber = lineNumber,
            };
        };
    }
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
    uint64_t bufferStartByteOffsetInFile = 0;
    uint64_t numValuesToOutput = 0;
    storage::MemoryManager& mm;
    idx_t lineCountInBuffer;
    std::unique_ptr<processor::LocalFileErrorHandler> errorHandler;
    JSONAllocator allocator;

    JSONScanLocalState(storage::MemoryManager& mm, JSONScanSharedState& sharedState,
        main::ClientContext* context)
        : docs{}, currentReader{sharedState.jsonReader.get()},
          reconstructBuffer{
              mm.allocateBuffer(false /* initializeToZero */, JsonConstant::SCAN_BUFFER_CAPACITY)},
          mm{mm}, lineCountInBuffer(0),
          errorHandler(std::make_unique<processor::LocalFileErrorHandler>(
              &sharedState.sharedErrorHandler, false, context)),
          allocator{mm} {}

    uint64_t readNext(const std::optional<std::vector<ValueVector*>>& warningDataVectors = {});
    bool readNextBuffer();
    bool readNextBufferInternal(uint64_t& bufferIdx, bool& fileDone);
    bool readNextBufferSeek(uint64_t& bufferIdx, bool& fileDone);
    void skipOverArrayStart();
    bool parseNextChunk(const std::optional<std::vector<ValueVector*>>& warningDataVectors);

    // Parses a single json record
    // Returns number of records parsed if parsing succeeds (this will be 0 or 1).
    // If parsing fails returns std::nullopt
    std::optional<uint64_t> parseJson(uint8_t* jsonStart, uint64_t jsonSize, uint64_t remaining,
        idx_t numLinesInJson,
        const std::optional<std::vector<ValueVector*>>& warningDataVectors = {});

    bool reconstructFirstObject();

    void addValuesToWarningDataVectors(processor::WarningSourceData warningData,
        uint64_t recordNumber, const std::optional<std::vector<ValueVector*>>& warningDataVectors);
    uint64_t getFileOffset() const;

    idx_t getNewlineCount(uint64_t startByteOffset, uint64_t endByteOffset) const;
    processor::WarningSourceData getWarningData(uint64_t startByteOffset, uint64_t endByteOffset,
        uint64_t extraLineCount = 0) const;
    void handleParseError(yyjson_read_err& err, bool completedParsingObject = true,
        const std::string& extra = "") const;
};

uint64_t JSONScanLocalState::getFileOffset() const {
    return bufferStartByteOffsetInFile + bufferOffset;
}

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
    bufferStartByteOffsetInFile = readPosition - prevBufferRemainder;
    return true;
}

static void skipWhitespace(uint8_t* bufferPtr, idx_t& bufferOffset, const uint64_t& bufferSize,
    idx_t* lineCount = nullptr) {
    for (; bufferOffset != bufferSize; bufferOffset++) {
        if (lineCount && bufferPtr[bufferOffset] == '\n') {
            ++(*lineCount);
        }
        if (!StringUtils::isSpace(bufferPtr[bufferOffset])) {
            break;
        }
    }
}

void JSONScanLocalState::skipOverArrayStart() {
    // First read of this buffer, check if it's actually an array and skip over the bytes
    skipWhitespace(bufferPtr, bufferOffset, bufferSize, &lineCountInBuffer);
    if (bufferOffset == bufferSize) {
        return; // Empty file
    }
    if (bufferPtr[bufferOffset] != '[') {
        throw Exception(stringFormat(
            "Expected top-level JSON array with format='array', but first character is '{}' in "
            "file \"{}\"."
            "\nTry setting format='auto' or format='newline_delimited'.",
            (reinterpret_cast<char*>(bufferPtr))[bufferOffset], currentReader->getFileName()));
    }
    skipWhitespace(bufferPtr, ++bufferOffset, bufferSize, &lineCountInBuffer);
    if (bufferOffset >= bufferSize) {
        throw Exception(stringFormat(
            "Missing closing brace ']' in JSON array with format='array' in file \"{}\"",
            currentReader->getFileName()));
    }
    if (bufferPtr[bufferOffset] == ']') {
        skipWhitespace(bufferPtr, ++bufferOffset, bufferSize, &lineCountInBuffer);
        if (bufferOffset != bufferSize) {
            throw Exception(stringFormat("Empty array with trailing data when parsing JSON "
                                         "array with format='array' in file \"{}\"",
                currentReader->getFileName()));
        }
        return;
    }
}

static uint8_t* nextJsonDefault(uint8_t* ptr, const uint8_t* end, idx_t& lineCountInBuffer) {
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
                } else if (strChar == '\n') {
                    ++lineCountInBuffer;
                }
            }
            break;
            // on windows each '\r' should come with a '\n' so counting '\n' should be enough to get
            // the line number
        case '\n':
            ++lineCountInBuffer;
            // fall through to continue
        default:
            continue;
        }

        if (parents == 0) {
            break;
        }
    }

    return ptr;
}

static uint8_t* nextJson(uint8_t* ptr, uint64_t size, idx_t& lineCountInJson) {
    lineCountInJson = 0;
    auto end = ptr + size;
    switch (*ptr) {
    case '{':
    case '[':
    case '"':
        ptr = nextJsonDefault(ptr, end, lineCountInJson);
        break;
    default:
        // Special case: JSON array containing JSON without clear "parents", i.e., not obj/arr/str
        while (ptr != end) {
            switch (*ptr++) {
            case ',':
            case ']':
                ptr--;
                break;
            case '\n':
                ++lineCountInJson;
                // fall through to continue
            default:
                continue;
            }
            break;
        }
    }

    return ptr == end ? nullptr : ptr;
}

idx_t JSONScanLocalState::getNewlineCount(uint64_t startByteOffset, uint64_t endByteOffset) const {
    std::string jsonString;
    jsonString.resize(endByteOffset - startByteOffset);
    bool finishedFile = false;
    currentReader->getFileHandle()->readAtPosition(reinterpret_cast<uint8_t*>(jsonString.data()),
        jsonString.size(), startByteOffset, finishedFile);
    return std::count(jsonString.begin(), jsonString.end(), '\n');
}

processor::WarningSourceData JSONScanLocalState::getWarningData(uint64_t startByteOffset,
    uint64_t endByteOffset, uint64_t extraLineCount) const {
    KU_ASSERT(currentBufferHandle);
    return processor::WarningSourceData::constructFrom(currentBufferHandle->bufferIdx,
        lineCountInBuffer + extraLineCount, startByteOffset, endByteOffset);
}

void JSONScanLocalState::handleParseError(yyjson_read_err& err, bool completedParsingObject,
    const std::string& extra) const {
    const uint64_t startByteOffset = getFileOffset();
    const uint64_t endByteOffset = startByteOffset + err.pos + 1;
    currentReader->throwParseError(err, completedParsingObject,
        getWarningData(startByteOffset, endByteOffset,
            getNewlineCount(startByteOffset, endByteOffset)),
        errorHandler.get(), extra);
}

std::optional<uint64_t> JSONScanLocalState::parseJson(uint8_t* jsonStart, uint64_t size,
    uint64_t remaining, idx_t numLinesInJson,
    const std::optional<std::vector<ValueVector*>>& warningDataVectors) {
    yyjson_doc* doc = nullptr;
    yyjson_read_err err;
    doc = JSONCommon::readDocumentUnsafe(jsonStart, remaining, JSONCommon::READ_INSITU_FLAG,
        allocator.getYYJsonAlc(), &err);
    if (err.code != YYJSON_READ_SUCCESS) {
        handleParseError(err, false);
        return std::nullopt;
    }

    idx_t numBytesRead = yyjson_doc_get_read_size(doc);
    if (warningDataVectors && !warningDataVectors->empty()) {
        const auto recordStartOffset = getFileOffset();
        const auto recordEndOffset = getFileOffset() + numBytesRead;
        addValuesToWarningDataVectors(
            getWarningData(recordStartOffset, recordEndOffset, numLinesInJson), numValuesToOutput,
            warningDataVectors);
    }

    // We parse with YYJSON_STOP_WHEN_DONE, so we need to check this by hand
    if (numBytesRead > size) {
        // Can't go past the boundary, even with ignore_errors
        err.code = YYJSON_READ_ERROR_UNEXPECTED_END;
        err.msg = "unexpected end of data";
        err.pos = size;
        handleParseError(err, "Try auto-detecting the JSON format");
        return std::nullopt;
    } else if (numBytesRead < size) {
        auto off = numBytesRead;
        auto rem = size;
        skipWhitespace(jsonStart, off, rem);
        if (off != rem) { // Between end of document and boundary should be whitespace only
            err.code = YYJSON_READ_ERROR_UNEXPECTED_CONTENT;
            err.msg = "unexpected content after document";
            err.pos = numBytesRead;
            handleParseError(err, "Try auto-detecting the JSON format");
            return std::nullopt;
        }
    }

    if (!doc) {
        docs[numValuesToOutput] = nullptr;
        return 0;
    }
    docs[numValuesToOutput] = doc;
    return 1;
}

void JSONScanLocalState::addValuesToWarningDataVectors(processor::WarningSourceData warningData,
    uint64_t recordNumber, const std::optional<std::vector<ValueVector*>>& warningDataVectors) {
    KU_ASSERT(warningDataVectors);
    KU_ASSERT(warningDataVectors->size() == warningData.numValues);
    for (column_id_t i = 0; i < warningData.numValues; ++i) {
        auto* vectorToSet = (*warningDataVectors)[i];
        std::visit(
            [vectorToSet, recordNumber](
                auto warningDataField) { vectorToSet->setValue(recordNumber, warningDataField); },
            warningData.values[i]);
    }
}

static uint8_t* nextNewLine(uint8_t* ptr, idx_t size) {
    return reinterpret_cast<uint8_t*>(memchr(ptr, '\n', size));
}

bool JSONScanLocalState::parseNextChunk(
    const std::optional<std::vector<ValueVector*>>& warningDataVectors) {
    auto format = currentReader->getFormat();
    while (numValuesToOutput < DEFAULT_VECTOR_CAPACITY) {
        skipWhitespace(bufferPtr, bufferOffset, bufferSize, &lineCountInBuffer);
        auto jsonStart = bufferPtr + bufferOffset;
        auto remaining = bufferSize - bufferOffset;
        if (remaining == 0) {
            break;
        }
        KU_ASSERT(format != JsonScanFormat::AUTO_DETECT);
        idx_t lineCountInJson = 0;
        auto jsonEnd = format == JsonScanFormat::NEWLINE_DELIMITED ?
                           nextNewLine(jsonStart, remaining) :
                           nextJson(jsonStart, remaining, lineCountInJson);
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
        auto parseResult =
            parseJson(jsonStart, jsonSize, remaining, lineCountInJson, warningDataVectors);

        bufferOffset += jsonSize;
        lineCountInBuffer += lineCountInJson;

        if (format == JsonScanFormat::ARRAY) {
            skipWhitespace(bufferPtr, bufferOffset, bufferSize, &lineCountInBuffer);
            if (bufferPtr[bufferOffset] == ',' || bufferPtr[bufferOffset] == ']') {
                bufferOffset++;
            } else {
                yyjson_read_err err;
                err.code = YYJSON_READ_ERROR_UNEXPECTED_CHARACTER;
                err.msg = "unexpected character";
                err.pos = jsonSize;
                handleParseError(err);
                parseResult = std::nullopt;
            }
        }
        skipWhitespace(bufferPtr, bufferOffset, bufferSize, &lineCountInBuffer);

        numValuesToOutput += parseResult.has_value() ? *parseResult : 0;

        // if we hit an error, stop the parsing
        // the actual error will be thrown during finalize
        if (!parseResult.has_value() && !errorHandler->getIgnoreErrorsOption()) {
            return false;
        }
    }
    return true;
}

static JsonScanFormat autoDetectFormat(uint8_t* buffer_ptr, uint64_t buffer_size, yyjson_alc* alc) {
    auto lineEnd = nextNewLine(buffer_ptr, buffer_size);
    if (lineEnd != nullptr) {
        idx_t line_size = lineEnd - buffer_ptr;
        skipWhitespace(buffer_ptr, line_size, buffer_size);
        yyjson_read_err error;
        auto doc = yyjson_read_opts(reinterpret_cast<char*>(buffer_ptr), line_size,
            JSONCommon::READ_FLAG, alc, &error);
        if (error.code == YYJSON_READ_SUCCESS) {
            const bool isArr = yyjson_is_arr(doc->root);
            yyjson_doc_free(doc);
            if (isArr && line_size == buffer_size) {
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
        JSONCommon::READ_STOP_FLAG, alc, &error);
    if (error.code == YYJSON_READ_SUCCESS) {
        KU_ASSERT(yyjson_is_arr(doc->root));
        buffer_offset += yyjson_doc_get_read_size(doc);
        yyjson_doc_free(doc);
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
        errorHandler->reportFinishedBlock(currentBufferHandle->bufferIdx, lineCountInBuffer);
        if (--currentBufferHandle->numReaders == 0) {
            // If the buffer used before is no longer used, we can reuse that buffer instead of
            // doing a new memory allocation.
            buffer = currentReader->removeBuffer(*currentBufferHandle);
        }
    }
    lineCountInBuffer = 0;

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
                        currentReader->setFormat(
                            autoDetectFormat(bufferPtr, bufferSize, allocator.getYYJsonAlc()));
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
        errorHandler->finalize();
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
    static constexpr idx_t linesInJson = 1;
    if (bufferSize != 0) {
        auto lineEnd = nextNewLine(bufferPtr, bufferSize);
        if (lineEnd == nullptr) {
            // TODO(Ziyi): We should make the maximum object size as a configurable option.
            throw RuntimeException{stringFormat("Json object exceeds the maximum object size.")};
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

    auto parseResult = parseJson(reconstructBufferPtr, lineSize, lineSize, linesInJson);
    lineCountInBuffer += linesInJson;
    return parseResult.has_value() && *parseResult > 0;
}

uint64_t JSONScanLocalState::readNext(
    const std::optional<std::vector<ValueVector*>>& warningDataVectors) {
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
        bool parseSuccess = parseNextChunk(warningDataVectors);
        if (!parseSuccess) {
            return numValuesToOutput;
        }
    };
    return numValuesToOutput;
}

struct JSONKeyHash {
    inline std::size_t operator()(std::string_view k) const {
        size_t result = 0;
        if (k.size() >= sizeof(size_t)) {
            memcpy(&result, k.data() + k.size() - sizeof(size_t), sizeof(size_t));
        } else {
            result = 0;
            fastMemcpy(&result, k.data(), k.size());
        }
        return result;
    }
};

struct JSONKeyEquality {
    inline bool operator()(std::string_view a, std::string_view b) const {
        if (a.size() != b.size()) {
            return false;
        }
        return FastMemcmp(a.data(), b.data(), a.size()) == 0;
    }
};

template<typename T>
using json_key_map_t = std::unordered_map<std::string_view, T, JSONKeyHash, JSONKeyEquality>;

class JsonColumnInfo {
public:
    explicit JsonColumnInfo(std::vector<std::string> columnNames);
    DELETE_COPY_DEFAULT_MOVE(JsonColumnInfo);

    uint64_t getFieldIdx(yyjson_val* fieldName) const;

private:
    // Note: JSON keys are case-sensitive.
    json_key_map_t<idx_t> colNameToIdx;
    std::vector<std::string> colNames;
};

JsonColumnInfo::JsonColumnInfo(std::vector<std::string> columnNames) {
    this->colNames = std::move(columnNames);
    idx_t colIdx = 0;
    for (auto& columnName : this->colNames) {
        colNameToIdx.insert({std::string_view(columnName), colIdx++});
    }
}

uint64_t JsonColumnInfo::getFieldIdx(yyjson_val* key) const {
    auto fieldName = std::string_view(unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
    // For a small number of keys, probing a vector is faster than lookups in an unordered_map
    if (colNames.size() < 24) {
        auto iter = std::find(colNames.begin(), colNames.end(), fieldName);
        if (iter != colNames.end()) {
            return iter - colNames.begin();
        }
    } else {
        auto itr = colNameToIdx.find(fieldName);
        if (itr != colNameToIdx.end()) {
            return itr->second;
        }
    }
    // From and to are case-insensitive for backward compatibility.
    if (StringUtils::caseInsensitiveEquals(fieldName, "from")) {
        return colNameToIdx.at("from");
    } else if (StringUtils::caseInsensitiveEquals(fieldName, "to")) {
        return colNameToIdx.at("to");
    }
    return UINT64_MAX;
}

struct JsonScanBindData : public ScanFileBindData {
    std::shared_ptr<JsonColumnInfo> columnInfo;
    JsonScanFormat format;

    JsonScanBindData(binder::expression_vector columns, column_id_t numWarningDataColumns,
        FileScanInfo fileScanInfo, main::ClientContext* ctx, JsonColumnInfo columnInfo,
        JsonScanFormat format)
        : ScanFileBindData(columns, 0 /* numRows */, std::move(fileScanInfo), ctx,
              numWarningDataColumns),
          columnInfo{std::make_shared<JsonColumnInfo>(std::move(columnInfo))}, format{format} {}

    uint64_t getFieldIdx(yyjson_val* key) const { return columnInfo->getFieldIdx(key); }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<JsonScanBindData>(*this);
    }

    JsonScanBindData(const JsonScanBindData& other)
        : ScanFileBindData{other}, columnInfo{other.columnInfo}, format{other.format} {}
};

static JsonScanFormat autoDetect(main::ClientContext* context, const std::string& filePath,
    JsonScanConfig& config, std::vector<LogicalType>& types, std::vector<std::string>& names) {
    auto numRowsToDetect = config.breadth;
    JSONScanSharedState sharedState(*context, filePath, config.format);
    JSONScanLocalState localState(*context->getMemoryManager(), sharedState, context);
    std::unordered_map<std::string, idx_t> colNameToIdx;
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
                auto keyPtr = unsafe_yyjson_get_str(key);
                auto keyLen = unsafe_yyjson_get_len(key);
                auto itr = colNameToIdx.find({keyPtr, keyLen});
                if (itr != colNameToIdx.end()) {
                    auto colIdx = itr->second;
                    types[colIdx] = LogicalTypeUtils::combineTypes(types[colIdx],
                        jsonSchema(ele, config.depth, config.breadth));
                } else {
                    colNameToIdx.insert({{keyPtr, keyLen}, (idx_t)names.size()});
                    names.push_back({keyPtr, keyLen});
                    types.push_back(jsonSchema(ele, config.depth, config.breadth));
                }
            }
        }
        numRowsToDetect -= next;
    }

    for (auto& type : types) {
        type = LogicalTypeUtils::purgeAny(type, LogicalType::STRING());
    }
    return sharedState.jsonReader->getFormat();
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto scanInput = ku_dynamic_cast<ExtraScanTableFuncBindInput*>(input->extraInput.get());
    std::vector<LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    JsonScanConfig scanConfig(scanInput->fileScanInfo.options);
    json_key_map_t<idx_t> colNameToIdx;
    if (!scanInput->expectedColumnNames.empty() || !scanConfig.autoDetect) {
        if (scanInput->expectedColumnNames.empty()) {
            throw BinderException{
                "When auto-detect is set to false, Kuzu requires the "
                "user to provide column names and types in the LOAD FROM clause."};
        }
        columnTypes = copyVector(scanInput->expectedColumnTypes);
        columnNames = scanInput->expectedColumnNames;
        if (scanConfig.format == JsonScanFormat::AUTO_DETECT) {
            JSONScanSharedState sharedState(*context,
                scanInput->fileScanInfo.getFilePath(JsonExtension::JSON_SCAN_FILE_IDX),
                scanConfig.format);
            JSONScanLocalState localState(*context->getMemoryManager(), sharedState, context);
            localState.readNext();
            scanConfig.format = sharedState.jsonReader->getFormat();
        }
    } else {
        scanConfig.format = autoDetect(context,
            scanInput->fileScanInfo.getFilePath(JsonExtension::JSON_SCAN_FILE_IDX), scanConfig,
            columnTypes, columnNames);
    }
    scanInput->tableFunction->canParallelFunc = [scanConfig]() {
        return scanConfig.format == JsonScanFormat::NEWLINE_DELIMITED;
    };

    auto variableNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(variableNames, columnTypes);

    bool ignoreErrors = scanInput->fileScanInfo.getOption(CopyConstants::IGNORE_ERRORS_OPTION_NAME,
        CopyConstants::DEFAULT_IGNORE_ERRORS);

    std::vector<std::string> warningColumnNames;
    std::vector<LogicalType> warningColumnTypes;
    column_id_t numWarningDataColumns = 0;
    if (ignoreErrors) {
        numWarningDataColumns = JsonConstant::JSON_WARNING_DATA_NUM_COLUMNS;
        for (idx_t i = 0; i < JsonConstant::JSON_WARNING_DATA_NUM_COLUMNS; ++i) {
            warningColumnNames.emplace_back(JsonConstant::JSON_WARNING_DATA_COLUMN_NAMES[i]);
            warningColumnTypes.emplace_back(JsonConstant::JSON_WARNING_DATA_COLUMN_TYPES[i]);
        }
    }
    auto warningColumns =
        input->binder->createInvisibleVariables(warningColumnNames, warningColumnTypes);
    for (auto& column : warningColumns) {
        columns.push_back(column);
    }
    return std::make_unique<JsonScanBindData>(columns, numWarningDataColumns,
        scanInput->fileScanInfo.copy(), context, JsonColumnInfo{std::move(columnNames)},
        scanConfig.format);
}

static decltype(auto) getWarningDataVectors(const DataChunk& chunk, column_id_t numWarningColumns) {
    KU_ASSERT(numWarningColumns <= chunk.getNumValueVectors());

    std::vector<ValueVector*> ret;
    for (column_id_t i = chunk.getNumValueVectors() - numWarningColumns;
         i < chunk.getNumValueVectors(); ++i) {
        ret.push_back(&chunk.getValueVectorMutable(i));
    }
    return ret;
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto localState = input.localState->ptrCast<JSONScanLocalState>();
    auto bindData = input.bindData->constPtrCast<JsonScanBindData>();
    auto sharedState = input.sharedState->ptrCast<JSONScanSharedState>();
    auto projectionSkips = bindData->getColumnSkips();
    for (auto& valueVector : output.dataChunk.valueVectors) {
        valueVector->setAllNull();
        valueVector->resetAuxiliaryBuffer();
    }
    const auto warningDataVectors =
        getWarningDataVectors(output.dataChunk, bindData->numWarningDataColumns);
    auto count = localState->readNext(warningDataVectors);

    // if we hit an error, stop the parsing for this thread
    if (!localState->errorHandler->getIgnoreErrorsOption() &&
        sharedState->sharedErrorHandler.getNumCachedErrors() > 0) {
        count = 0;
    }

    yyjson_doc** docs = localState->docs;
    yyjson_val *key = nullptr, *ele = nullptr;
    for (auto i = 0u; i < count; i++) {
        KU_ASSERT(nullptr != docs[i]);
        auto objIter = yyjson_obj_iter_with(docs[i]->root);
        while ((key = yyjson_obj_iter_next(&objIter))) {
            ele = yyjson_obj_iter_get_val(key);
            auto columnIdx = bindData->getFieldIdx(key);
            if (columnIdx == UINT64_MAX || projectionSkips[columnIdx]) {
                continue;
            }
            readJsonToValueVector(ele, *output.dataChunk.valueVectors[columnIdx], i);
        }
    }
    localState->allocator.reset();
    output.dataChunk.state->getSelVectorUnsafe().setSelSize(count);
    return count;
}

static std::unique_ptr<TableFuncSharedState> initSharedState(
    const TableFuncInitSharedStateInput& input) {
    auto jsonBindData = input.bindData->constPtrCast<JsonScanBindData>();
    return std::make_unique<JSONScanSharedState>(*jsonBindData->context,
        jsonBindData->fileScanInfo.filePaths[0], jsonBindData->format);
}

static std::unique_ptr<TableFuncLocalState> initLocalState(
    const TableFuncInitLocalStateInput& input) {
    auto jsonBindData = input.bindData.constPtrCast<JsonScanBindData>();
    auto sharedState = input.sharedState.ptrCast<JSONScanSharedState>();
    auto localState = std::make_unique<JSONScanLocalState>(*input.clientContext->getMemoryManager(),
        *sharedState, jsonBindData->context);
    return localState;
}

static double progressFunc(TableFuncSharedState* /*state*/) {
    // TODO(Ziyi): fix this.
    return 0;
}

static void finalizeFunc(const processor::ExecutionContext* ctx,
    TableFuncSharedState* sharedState) {
    auto* jsonSharedState = sharedState->ptrCast<JSONScanSharedState>();

    jsonSharedState->sharedErrorHandler.throwCachedErrorsIfNeeded();
    ctx->clientContext->getWarningContextUnsafe().populateWarnings(ctx->queryID,
        jsonSharedState->populateErrorFunc);
}

function_set JsonScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    func->tableFunc = tableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = initSharedState;
    func->initLocalStateFunc = initLocalState;
    func->progressFunc = progressFunc;
    func->finalizeFunc = finalizeFunc;
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace json_extension
} // namespace kuzu
