#include "function/cast/functions/cast_string_to_functions.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/exception/parser.h"
#include "common/string_format.h"
#include "common/types/blob.h"
#include "storage/store/table_copy_utils.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

// ---------------------- cast String to Blob ------------------------------ //
template<>
void CastStringToTypes::operation(
    common::ku_string_t& input, common::blob_t& result, common::ValueVector& resultVector) {
    result.value.len = common::Blob::getBlobSize(input);
    if (!common::ku_string_t::isShortString(result.value.len)) {
        auto overflowBuffer = common::StringVector::getInMemOverflowBuffer(&resultVector);
        auto overflowPtr = overflowBuffer->allocateSpace(result.value.len);
        result.value.overflowPtr = reinterpret_cast<int64_t>(overflowPtr);
        common::Blob::fromString(
            reinterpret_cast<const char*>(input.getData()), input.len, overflowPtr);
        memcpy(result.value.prefix, overflowPtr, common::ku_string_t::PREFIX_LENGTH);
    } else {
        common::Blob::fromString(
            reinterpret_cast<const char*>(input.getData()), input.len, result.value.prefix);
    }
}

template<>
void CastStringToTypes::operation(const char* input, uint64_t len, common::blob_t& /*result*/,
    ValueVector* vector, uint64_t rowToAdd, const CSVReaderConfig* /*CSVReaderConfig*/) {
    // base case: blob
    storage::TableCopyUtils::validateStrLen(len);
    auto blobBuffer = std::make_unique<uint8_t[]>(len);
    auto blobLen = Blob::fromString(input, len, blobBuffer.get());
    StringVector::addString(vector, rowToAdd, reinterpret_cast<char*>(blobBuffer.get()), blobLen);
}

// ---------------------- cast String to nested types ------------------------------ //
static void skipWhitespace(const char*& input, const char* end) {
    while (input < end && isspace(*input)) {
        input++;
    }
}

static void trimRightWhitespace(const char* input, const char*& end) {
    while (input < end && isspace(*(end - 1))) {
        end--;
    }
}

static bool skipToCloseQuotes(const char*& input, const char* end) {
    auto ch = *input;
    input++; // skip the first " '
    // TODO: escape char
    while (input != end) {
        if (*input == ch) {
            return true;
        }
        input++;
    }
    return false;
}

static bool skipToClose(const char*& input, const char* end, uint64_t& lvl, char target,
    const CSVReaderConfig* csvReaderConfig) {
    input++;
    while (input != end) {
        if (*input == '\'') {
            if (!skipToCloseQuotes(input, end)) {
                return false;
            }
        } else if (*input == '{') { // must have closing brackets fro {, ] if they are not quoted
            if (!skipToClose(input, end, lvl, '}', csvReaderConfig)) {
                return false;
            }
        } else if (*input == csvReaderConfig->listBeginChar) {
            if (!skipToClose(input, end, lvl, csvReaderConfig->listEndChar, csvReaderConfig)) {
                return false;
            }
            lvl++; // nested one more level
        } else if (*input == target) {
            if (target == csvReaderConfig->listEndChar) {
                lvl--;
            }
            return true;
        }
        input++;
    }
    return false; // no corresponding closing bracket
}

static bool isNull(std::string_view& str) {
    auto start = str.data();
    auto end = start + str.length();
    skipWhitespace(start, end);
    if (start == end) {
        return true;
    }
    if (end - start >= 4 && (*start == 'N' || *start == 'n') &&
        (*(start + 1) == 'U' || *(start + 1) == 'u') &&
        (*(start + 2) == 'L' || *(start + 2) == 'l') &&
        (*(start + 3) == 'L' || *(start + 3) == 'l')) {
        start += 4;
        skipWhitespace(start, end);
        if (start == end) {
            return true;
        }
    }
    return false;
}

// ---------------------- cast String to Varlist ------------------------------ //
struct CountPartOperation {
    uint64_t count = 0;

    static inline bool handleKey(
        const char* /*start*/, const char* /*end*/, const CSVReaderConfig* /*config*/) {
        return true;
    }
    inline void handleValue(
        const char* /*start*/, const char* /*end*/, const CSVReaderConfig* /*config*/) {
        count++;
    }
};

struct SplitStringListOperation {
    SplitStringListOperation(uint64_t& offset, ValueVector* resultVector)
        : offset(offset), resultVector(resultVector) {}

    uint64_t& offset;
    ValueVector* resultVector;

    void handleValue(const char* start, const char* end, const CSVReaderConfig* csvReaderConfig) {
        CastStringToTypes::copyStringToVector(resultVector, offset,
            std::string_view{start, (uint32_t)(end - start)}, csvReaderConfig);
        offset++;
    }
};

template<typename T>
static bool splitCStringList(
    const char* input, uint64_t len, T& state, const CSVReaderConfig* csvReaderConfig) {
    auto end = input + len;
    uint64_t lvl = 1;
    bool seen_value = false;

    // locate [
    skipWhitespace(input, end);
    if (input == end || *input != csvReaderConfig->listBeginChar) {
        return false;
    }
    input++;

    auto start_ptr = input;
    while (input < end) {
        auto ch = *input;
        if (ch == csvReaderConfig->listBeginChar) {
            if (!skipToClose(input, end, ++lvl, csvReaderConfig->listEndChar, csvReaderConfig)) {
                return false;
            }
        } else if (ch == '\'' || ch == '"') {
            if (!skipToCloseQuotes(input, end)) {
                return false;
            }
        } else if (ch == '{') {
            uint64_t struct_lvl = 0;
            skipToClose(input, end, struct_lvl, '}', csvReaderConfig);
        } else if (ch == csvReaderConfig->delimiter ||
                   ch == csvReaderConfig->listEndChar) { // split
            if (ch != csvReaderConfig->listEndChar || start_ptr < input || seen_value) {
                state.handleValue(start_ptr, input, csvReaderConfig);
                seen_value = true;
            }
            if (ch == csvReaderConfig->listEndChar) { // last ]
                lvl--;
                break;
            }
            start_ptr = ++input;
            continue;
        }
        input++;
    }
    skipWhitespace(++input, end);
    return (input == end && lvl == 0);
}

template<typename T>
static inline void startListCast(const char* input, uint64_t len, T split,
    const CSVReaderConfig* csvReaderConfig, ValueVector* vector) {
    if (!splitCStringList(input, len, split, csvReaderConfig)) {
        throw ConversionException("Cast failed. " + std::string{input, len} + " is not in " +
                                  LogicalTypeUtils::dataTypeToString(vector->dataType) + " range.");
    }
}

template<>
void CastStringToTypes::operation(const char* input, uint64_t len, list_entry_t& /*result*/,
    ValueVector* vector, uint64_t rowToAdd, const CSVReaderConfig* csvReaderConfig) {
    // calculate the number of elements in array
    CountPartOperation state;
    splitCStringList(input, len, state, csvReaderConfig);

    auto list_entry = ListVector::addList(vector, state.count);
    vector->setValue<list_entry_t>(rowToAdd, list_entry);
    auto listDataVector = ListVector::getDataVector(vector);

    SplitStringListOperation split{list_entry.offset, listDataVector};
    startListCast(input, len, split, csvReaderConfig, vector);
}

// ---------------------- cast String to FixedList ------------------------------ //
template<typename T>
struct SplitStringFixedListOperation {
    SplitStringFixedListOperation(uint64_t& offset, ValueVector* resultVector)
        : offset(offset), resultVector(resultVector) {}

    uint64_t& offset;
    ValueVector* resultVector;

    void handleValue(
        const char* start, const char* end, const CSVReaderConfig* /*CSVReaderConfig*/) {
        T value;
        auto str = std::string_view{start, (uint32_t)(end - start)};
        if (str.empty() || isNull(str)) {
            throw ConversionException("Cast failed. NULL is not allowed for FIXEDLIST.");
        }
        auto type = FixedListType::getChildType(&resultVector->dataType);
        function::CastStringToTypes::operation(start, str.length(), value);
        resultVector->setValue(offset, value);
        offset++;
    }
};

static void validateNumElementsInList(uint64_t numElementsRead, const LogicalType& type) {
    auto numElementsInList = FixedListType::getNumElementsInList(&type);
    if (numElementsRead != numElementsInList) {
        throw CopyException(stringFormat(
            "Each fixed list should have fixed number of elements. Expected: {}, Actual: {}.",
            numElementsInList, numElementsRead));
    }
}

void castStringToFixedList(const char* input, uint64_t len, ValueVector* vector, uint64_t rowToAdd,
    const CSVReaderConfig* csvReaderConfig) {
    assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::FIXED_LIST);
    auto childDataType = FixedListType::getChildType(&vector->dataType);

    // calculate the number of elements in array
    CountPartOperation state;
    splitCStringList(input, len, state, csvReaderConfig);
    validateNumElementsInList(state.count, vector->dataType);

    auto startOffset = state.count * rowToAdd;
    switch (childDataType->getLogicalTypeID()) {
    // TODO: currently only allow these type
    case LogicalTypeID::INT64: {
        SplitStringFixedListOperation<int64_t> split{startOffset, vector};
        startListCast(input, len, split, csvReaderConfig, vector);
    } break;
    case LogicalTypeID::INT32: {
        SplitStringFixedListOperation<int32_t> split{startOffset, vector};
        startListCast(input, len, split, csvReaderConfig, vector);
    } break;
    case LogicalTypeID::INT16: {
        SplitStringFixedListOperation<int16_t> split{startOffset, vector};
        startListCast(input, len, split, csvReaderConfig, vector);
    } break;
    case LogicalTypeID::FLOAT: {
        SplitStringFixedListOperation<float_t> split{startOffset, vector};
        startListCast(input, len, split, csvReaderConfig, vector);
    } break;
    case LogicalTypeID::DOUBLE: {
        SplitStringFixedListOperation<double_t> split{startOffset, vector};
        startListCast(input, len, split, csvReaderConfig, vector);
    } break;
    default: {
        throw NotImplementedException("Unsupported data type: Driver::castStringToFixedList");
    }
    }
}

// ---------------------- cast String to Map ------------------------------ //
struct SplitStringMapOperation {
    SplitStringMapOperation(uint64_t& offset, ValueVector* resultVector)
        : offset(offset), resultVector(resultVector) {}

    uint64_t& offset;
    ValueVector* resultVector;

    inline bool handleKey(
        const char* start, const char* end, const CSVReaderConfig* csvReaderConfig) {
        trimRightWhitespace(start, end);
        CastStringToTypes::copyStringToVector(StructVector::getFieldVector(resultVector, 0).get(),
            offset, std::string_view{start, (uint32_t)(end - start)}, csvReaderConfig);
        return true;
    }

    inline void handleValue(
        const char* start, const char* end, const CSVReaderConfig* csvReaderConfig) {
        trimRightWhitespace(start, end);
        CastStringToTypes::copyStringToVector(StructVector::getFieldVector(resultVector, 1).get(),
            offset++, std::string_view{start, (uint32_t)(end - start)}, csvReaderConfig);
    }
};

template<typename T>
static bool parseKeyOrValue(const char*& input, const char* end, T& state, bool isKey,
    bool& closeBracket, const CSVReaderConfig* csvReaderConfig) {
    auto start = input;
    uint64_t lvl = 0;

    while (input < end) {
        if (*input == '"' || *input == '\'') {
            if (!skipToCloseQuotes(input, end)) {
                return false;
            };
        } else if (*input == '{') {
            if (!skipToClose(input, end, lvl, '}', csvReaderConfig)) {
                return false;
            }
        } else if (*input == csvReaderConfig->listBeginChar) {
            if (!skipToClose(input, end, lvl, csvReaderConfig->listEndChar, csvReaderConfig)) {
                return false;
            };
        } else if (isKey && *input == '=') {
            return state.handleKey(start, input, csvReaderConfig);
        } else if (!isKey && (*input == csvReaderConfig->delimiter || *input == '}')) {
            state.handleValue(start, input, csvReaderConfig);
            if (*input == '}') {
                closeBracket = true;
            }
            return true;
        }
        input++;
    }
    return false;
}

// Split map of format: {a=12,b=13}
template<typename T>
static bool splitCStringMap(
    const char* input, uint64_t len, T& state, const CSVReaderConfig* csvReaderConfig) {
    auto end = input + len;
    bool closeBracket = false;

    skipWhitespace(input, end);
    if (input == end || *input != '{') { // start with {
        return false;
    }
    skipWhitespace(++input, end);
    if (input == end) {
        return false;
    }
    if (*input == '}') {
        skipWhitespace(++input, end); // empty
        return input == end;
    }

    while (input < end) {
        if (!parseKeyOrValue(input, end, state, true, closeBracket, csvReaderConfig)) {
            return false;
        }
        skipWhitespace(++input, end);
        if (!parseKeyOrValue(input, end, state, false, closeBracket, csvReaderConfig)) {
            return false;
        }
        skipWhitespace(++input, end);
        if (closeBracket) {
            return (input == end);
        }
    }
    return false;
}

template<>
void CastStringToTypes::operation(const char* input, uint64_t len, common::map_entry_t& /*result*/,
    ValueVector* vector, uint64_t rowToAdd, const CSVReaderConfig* csvReaderConfig) {
    // count the number of maps in map
    CountPartOperation state;
    splitCStringMap(input, len, state, csvReaderConfig);

    auto list_entry = ListVector::addList(vector, state.count);
    vector->setValue<list_entry_t>(rowToAdd, list_entry);
    auto structVector = ListVector::getDataVector(vector);

    SplitStringMapOperation split{list_entry.offset, structVector};
    if (!splitCStringMap(input, len, split, csvReaderConfig)) {
        throw ConversionException("Cast failed. " + std::string{input, len} + " is not in " +
                                  LogicalTypeUtils::dataTypeToString(vector->dataType) + " range.");
    }
}

// ---------------------- cast String to Struct ------------------------------ //
static bool parseStructFieldName(const char*& input, const char* end) {
    while (input < end) {
        if (*input == ':') {
            return true;
        }
        input++;
    }
    return false;
}

static bool parseStructFieldValue(
    const char*& input, const char* end, const CSVReaderConfig* csvReaderConfig, bool& closeBrack) {
    uint64_t lvl = 0;
    while (input < end) {
        if (*input == '"' || *input == '\'') {
            if (!skipToCloseQuotes(input, end)) {
                return false;
            }
        } else if (*input == '{') {
            if (!skipToClose(input, end, lvl, '}', csvReaderConfig)) {
                return false;
            }
        } else if (*input == csvReaderConfig->listBeginChar) {
            if (!skipToClose(input, end, ++lvl, csvReaderConfig->listEndChar, csvReaderConfig)) {
                return false;
            }
        } else if (*input == csvReaderConfig->delimiter || *input == '}') {
            if (*input == '}') {
                closeBrack = true;
            }
            return (lvl == 0);
        }
        input++;
    }
    return false;
}

static bool tryCastStringToStruct(const char* input, uint64_t len, ValueVector* vector,
    uint64_t rowToAdd, const CSVReaderConfig* csvReaderConfig) {
    // check if start with {
    auto end = input + len;
    auto type = vector->dataType;
    skipWhitespace(input, end);
    if (input == end || *input != '{') {
        return false;
    }
    skipWhitespace(++input, end);

    if (input == end) { // no closing bracket
        return false;
    }
    if (*input == '}') {
        skipWhitespace(++input, end);
        return input == end;
    }

    bool closeBracket = false;
    while (input < end) {
        auto keyStart = input;
        if (!parseStructFieldName(input, end)) { // find key
            return false;
        }
        auto keyEnd = input;
        trimRightWhitespace(keyStart, keyEnd);
        auto fieldIdx = StructType::getFieldIdx(&type, std::string{keyStart, keyEnd});
        if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw ParserException{"Invalid struct field name: " + std::string{keyStart, keyEnd}};
        }

        skipWhitespace(++input, end);
        auto valStart = input;
        if (!parseStructFieldValue(input, end, csvReaderConfig, closeBracket)) { // find value
            return false;
        }
        auto valEnd = input;
        trimRightWhitespace(valStart, valEnd);
        skipWhitespace(++input, end);

        CastStringToTypes::copyStringToVector(StructVector::getFieldVector(vector, fieldIdx).get(),
            rowToAdd, std::string_view{valStart, (uint32_t)(valEnd - valStart)}, csvReaderConfig);

        if (closeBracket) {
            return (input == end);
        }
    }
    return false;
}

template<>
void CastStringToTypes::operation(const char* input, uint64_t len, struct_entry_t& /*result*/,
    ValueVector* vector, uint64_t rowToAdd, const CSVReaderConfig* csvReaderConfig) {
    if (!tryCastStringToStruct(input, len, vector, rowToAdd, csvReaderConfig)) {
        throw ConversionException("Cast failed. " + std::string{input, len} + " is not in " +
                                  LogicalTypeUtils::dataTypeToString(vector->dataType) + " range.");
    }
}

// ---------------------- cast String to Union ------------------------------ //
template<typename T>
static inline void testAndSetValue(ValueVector* vector, uint64_t rowToAdd, T result, bool success) {
    if (success) {
        vector->setValue(rowToAdd, result);
    }
}

static bool tryCastUnionField(
    ValueVector* vector, uint64_t rowToAdd, const char* input, uint64_t len) {
    auto& targetType = vector->dataType;
    bool success = false;
    switch (targetType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        bool result;
        success = function::tryCastToBool(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::INT64: {
        int64_t result;
        success = function::trySimpleIntegerCast(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::INT32: {
        int32_t result;
        success = function::trySimpleIntegerCast(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::INT16: {
        int16_t result;
        success = function::trySimpleIntegerCast(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::INT8: {
        int8_t result;
        success = function::trySimpleIntegerCast(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::UINT64: {
        uint64_t result;
        success = function::trySimpleIntegerCast<uint64_t, false>(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::UINT32: {
        uint32_t result;
        success = function::trySimpleIntegerCast<uint32_t, false>(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::UINT16: {
        uint16_t result;
        success = function::trySimpleIntegerCast<uint16_t, false>(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::UINT8: {
        uint8_t result;
        success = function::trySimpleIntegerCast<uint8_t, false>(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::DOUBLE: {
        double_t result;
        success = function::tryDoubleCast(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::FLOAT: {
        float_t result;
        success = function::tryDoubleCast(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::DATE: {
        date_t result;
        uint64_t pos;
        success = Date::tryConvertDate(input, len, pos, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        timestamp_t result;
        success = Timestamp::tryConvertTimestamp(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::STRING: {
        storage::TableCopyUtils::validateStrLen(len);
        if (!utf8proc::Utf8Proc::isValid(input, len)) {
            throw CopyException{"Invalid UTF8-encoded string."};
        }
        StringVector::addString(vector, rowToAdd, input, len);
        return true;
    } break;
    default: {
        return false;
    }
    }
    return success;
}

template<>
void CastStringToTypes::operation(const char* input, uint64_t len, union_entry_t& /*result*/,
    ValueVector* vector, uint64_t rowToAdd, const CSVReaderConfig* /*CSVReaderConfig**/) {
    auto& type = vector->dataType;
    union_field_idx_t selectedFieldIdx = INVALID_STRUCT_FIELD_IDX;

    for (auto i = 0u; i < UnionType::getNumFields(&type); i++) {
        auto internalFieldIdx = UnionType::getInternalFieldIdx(i);
        auto fieldVector = StructVector::getFieldVector(vector, internalFieldIdx).get();
        if (tryCastUnionField(fieldVector, rowToAdd, input, len)) {
            fieldVector->setNull(rowToAdd, false /* isNull */);
            selectedFieldIdx = i;
            break;
        } else {
            fieldVector->setNull(rowToAdd, true /* isNull */);
        }
    }

    if (selectedFieldIdx == INVALID_STRUCT_FIELD_IDX) {
        throw ConversionException{stringFormat("Could not convert to union type {}: {}.",
            LogicalTypeUtils::dataTypeToString(type), std::string{input, len})};
    }
    StructVector::getFieldVector(vector, UnionType::TAG_FIELD_IDX)
        ->setValue(rowToAdd, selectedFieldIdx);
    StructVector::getFieldVector(vector, UnionType::TAG_FIELD_IDX)
        ->setNull(rowToAdd, false /* isNull */);
}

void CastStringToTypes::copyStringToVector(ValueVector* vector, uint64_t rowToAdd,
    std::string_view strVal, const CSVReaderConfig* csvReaderConfig) {
    auto& type = vector->dataType;

    if (strVal.empty() || isNull(strVal)) {
        vector->setNull(rowToAdd, true /* isNull */);
        return;
    } else {
        vector->setNull(rowToAdd, false /* isNull */);
    }
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::INT128: {
        int128_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::INT64: {
        int64_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::INT32: {
        int32_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::INT16: {
        int16_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::INT8: {
        int8_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::UINT64: {
        uint64_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::UINT32: {
        uint32_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::UINT16: {
        uint16_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::UINT8: {
        uint8_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::FLOAT: {
        float_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::DOUBLE: {
        double_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::BOOL: {
        bool val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::BLOB: {
        blob_t val;
        operation(strVal.data(), strVal.length(), val, vector, rowToAdd, csvReaderConfig);
    } break;
    case LogicalTypeID::STRING: {
        storage::TableCopyUtils::validateStrLen(strVal.length());
        if (!utf8proc::Utf8Proc::isValid(strVal.data(), strVal.length())) {
            throw CopyException{"Invalid UTF8-encoded string."};
        }
        StringVector::addString(vector, rowToAdd, strVal.data(), strVal.length());
    } break;
    case LogicalTypeID::DATE: {
        date_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        timestamp_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::INTERVAL: {
        interval_t val;
        operation(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::MAP: {
        map_entry_t val;
        operation(strVal.data(), strVal.length(), val, vector, rowToAdd, csvReaderConfig);
    } break;
    case LogicalTypeID::VAR_LIST: {
        list_entry_t val;
        operation(strVal.data(), strVal.length(), val, vector, rowToAdd, csvReaderConfig);
    } break;
    case LogicalTypeID::FIXED_LIST: {
        // TODO: add fix list function wrapper
        castStringToFixedList(strVal.data(), strVal.length(), vector, rowToAdd, csvReaderConfig);
    } break;
    case LogicalTypeID::STRUCT: {
        struct_entry_t val;
        operation(strVal.data(), strVal.length(), val, vector, rowToAdd, csvReaderConfig);
    } break;
    case LogicalTypeID::UNION: {
        union_entry_t val;
        operation(strVal.data(), strVal.length(), val, vector, rowToAdd, csvReaderConfig);
    } break;
    default: { // LCOV_EXCL_START
        throw NotImplementedException("CastStringToTypes::operation");
    } // LCOV_EXCL_STOP
    }
}

} // namespace function
} // namespace kuzu
