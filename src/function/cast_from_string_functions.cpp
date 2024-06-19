#include "function/cast/functions/cast_from_string_functions.h"

#include "common/exception/copy.h"
#include "common/exception/parser.h"
#include "common/string_format.h"
#include "common/types/blob.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

// ---------------------- cast String Helper ------------------------------ //
struct CastStringHelper {
    template<typename T>
    static void cast(const char* input, uint64_t len, T& result, ValueVector* /*vector*/ = nullptr,
        uint64_t /*rowToAdd*/ = 0, const CSVOption* /*option*/ = nullptr) {
        simpleIntegerCast<int64_t>(input, len, result, LogicalTypeID::INT64);
    }
};

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, int128_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleInt128Cast(input, len, result);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, int32_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<int32_t>(input, len, result, LogicalTypeID::INT32);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, int16_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<int16_t>(input, len, result, LogicalTypeID::INT16);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, int8_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<int8_t>(input, len, result, LogicalTypeID::INT8);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, uint64_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<uint64_t, false>(input, len, result, LogicalTypeID::UINT64);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, uint32_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<uint32_t, false>(input, len, result, LogicalTypeID::UINT32);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, uint16_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<uint16_t, false>(input, len, result, LogicalTypeID::UINT16);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, uint8_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<uint8_t, false>(input, len, result, LogicalTypeID::UINT8);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, float& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    doubleCast<float>(input, len, result, LogicalTypeID::FLOAT);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, double& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    doubleCast<double>(input, len, result, LogicalTypeID::DOUBLE);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, bool& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    castStringToBool(input, len, result);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, date_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    result = Date::fromCString(input, len);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, timestamp_ms_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    TryCastStringToTimestamp::cast<timestamp_ms_t>(input, len, result, LogicalTypeID::TIMESTAMP_MS);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, timestamp_ns_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    TryCastStringToTimestamp::cast<timestamp_ns_t>(input, len, result, LogicalTypeID::TIMESTAMP_NS);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, timestamp_sec_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    TryCastStringToTimestamp::cast<timestamp_sec_t>(input, len, result,
        LogicalTypeID::TIMESTAMP_SEC);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, timestamp_tz_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    TryCastStringToTimestamp::cast<timestamp_tz_t>(input, len, result, LogicalTypeID::TIMESTAMP_TZ);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, timestamp_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    result = Timestamp::fromCString(input, len);
}

template<>
inline void CastStringHelper::cast(const char* input, uint64_t len, interval_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    result = Interval::fromCString(input, len);
}

// ---------------------- cast String to Blob ------------------------------ //
template<>
void CastString::operation(const ku_string_t& input, blob_t& result, ValueVector* resultVector,
    uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    result.value.len = Blob::getBlobSize(input);
    if (!ku_string_t::isShortString(result.value.len)) {
        auto overflowBuffer = StringVector::getInMemOverflowBuffer(resultVector);
        auto overflowPtr = overflowBuffer->allocateSpace(result.value.len);
        result.value.overflowPtr = reinterpret_cast<int64_t>(overflowPtr);
        Blob::fromString(reinterpret_cast<const char*>(input.getData()), input.len, overflowPtr);
        memcpy(result.value.prefix, overflowPtr, ku_string_t::PREFIX_LENGTH);
    } else {
        Blob::fromString(reinterpret_cast<const char*>(input.getData()), input.len,
            result.value.prefix);
    }
}

template<>
void CastStringHelper::cast(const char* input, uint64_t len, blob_t& /*result*/,
    ValueVector* vector, uint64_t rowToAdd, const CSVOption* /*option*/) {
    // base case: blob
    auto blobBuffer = std::make_unique<uint8_t[]>(len);
    auto blobLen = Blob::fromString(input, len, blobBuffer.get());
    BlobVector::addBlob(vector, rowToAdd, blobBuffer.get(), blobLen);
}

//---------------------- cast String to UUID ------------------------------ //
template<>
void CastString::operation(const ku_string_t& input, ku_uuid_t& result,
    ValueVector* /*result_vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    result.value = UUID::fromString(input.getAsString());
}

// LCOV_EXCL_START
template<>
void CastStringHelper::cast(const char* input, uint64_t len, ku_uuid_t& result,
    ValueVector* /*vector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    result.value = UUID::fromCString(input, len);
}
// LCOV_EXCL_STOP

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

static void trimQuotes(const char*& keyStart, const char*& keyEnd) {
    // Skip quotations on struct keys.
    if (keyStart[0] == '\'' && (keyEnd - 1)[0] == '\'') {
        keyStart++;
        keyEnd--;
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
    const CSVOption* option) {
    input++;
    while (input != end) {
        if (*input == '\'') {
            if (!skipToCloseQuotes(input, end)) {
                return false;
            }
        } else if (*input == '{') { // must have closing brackets {, ] if they are not quoted
            if (!skipToClose(input, end, lvl, '}', option)) {
                return false;
            }
        } else if (*input == CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR) {
            if (!skipToClose(input, end, lvl, CopyConstants::DEFAULT_CSV_LIST_END_CHAR, option)) {
                return false;
            }
            lvl++; // nested one more level
        } else if (*input == target) {
            if (target == CopyConstants::DEFAULT_CSV_LIST_END_CHAR) {
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

// ---------------------- cast String to List Helper ------------------------------ //
struct CountPartOperation {
    uint64_t count = 0;

    static inline bool handleKey(const char* /*start*/, const char* /*end*/,
        const CSVOption* /*config*/) {
        return true;
    }
    inline void handleValue(const char* /*start*/, const char* /*end*/,
        const CSVOption* /*config*/) {
        count++;
    }
};

struct SplitStringListOperation {
    SplitStringListOperation(uint64_t& offset, ValueVector* resultVector)
        : offset(offset), resultVector(resultVector) {}

    uint64_t& offset;
    ValueVector* resultVector;

    void handleValue(const char* start, const char* end, const CSVOption* option) {
        skipWhitespace(start, end);
        trimRightWhitespace(start, end);
        CastString::copyStringToVector(resultVector, offset,
            std::string_view{start, (uint32_t)(end - start)}, option);
        offset++;
    }
};

template<typename T>
static bool splitCStringList(const char* input, uint64_t len, T& state, const CSVOption* option) {
    auto end = input + len;
    uint64_t lvl = 1;
    bool seen_value = false;

    // locate [
    skipWhitespace(input, end);
    if (input == end || *input != CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR) {
        return false;
    }
    skipWhitespace(++input, end);

    auto start_ptr = input;
    while (input < end) {
        auto ch = *input;
        if (ch == CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR) {
            if (!skipToClose(input, end, ++lvl, CopyConstants::DEFAULT_CSV_LIST_END_CHAR, option)) {
                return false;
            }
        } else if (ch == '\'' || ch == '"') {
            if (!skipToCloseQuotes(input, end)) {
                return false;
            }
        } else if (ch == '{') {
            uint64_t struct_lvl = 0;
            skipToClose(input, end, struct_lvl, '}', option);
        } else if (ch == option->delimiter ||
                   ch == CopyConstants::DEFAULT_CSV_LIST_END_CHAR) { // split
            if (ch != CopyConstants::DEFAULT_CSV_LIST_END_CHAR || start_ptr < input || seen_value) {
                state.handleValue(start_ptr, input, option);
                seen_value = true;
            }
            if (ch == CopyConstants::DEFAULT_CSV_LIST_END_CHAR) { // last ]
                lvl--;
                break;
            }
            skipWhitespace(++input, end);
            start_ptr = input;
            continue;
        }
        input++;
    }
    skipWhitespace(++input, end);
    return (input == end && lvl == 0);
}

template<typename T>
static inline void startListCast(const char* input, uint64_t len, T split, const CSVOption* option,
    ValueVector* vector) {
    if (!splitCStringList(input, len, split, option)) {
        throw ConversionException("Cast failed. " + std::string{input, len} + " is not in " +
                                  vector->dataType.toString() + " range.");
    }
}

// ---------------------- cast String to Array Helper ------------------------------ //
static void validateNumElementsInArray(uint64_t numElementsRead, const LogicalType& type) {
    auto numElementsInArray = ArrayType::getNumElements(type);
    if (numElementsRead != numElementsInArray) {
        throw CopyException(stringFormat(
            "Each array should have fixed number of elements. Expected: {}, Actual: {}.",
            numElementsInArray, numElementsRead));
    }
}

// ---------------------- cast String to List/Array ------------------------------ //
template<>
void CastStringHelper::cast(const char* input, uint64_t len, list_entry_t& /*result*/,
    ValueVector* vector, uint64_t rowToAdd, const CSVOption* option) {
    auto logicalTypeID = vector->dataType.getLogicalTypeID();

    // calculate the number of elements in array
    CountPartOperation state;
    splitCStringList(input, len, state, option);
    if (logicalTypeID == LogicalTypeID::ARRAY) {
        validateNumElementsInArray(state.count, vector->dataType);
    }

    auto list_entry = ListVector::addList(vector, state.count);
    vector->setValue<list_entry_t>(rowToAdd, list_entry);
    auto listDataVector = ListVector::getDataVector(vector);

    SplitStringListOperation split{list_entry.offset, listDataVector};
    startListCast(input, len, split, option, vector);
}

template<>
void CastString::operation(const ku_string_t& input, list_entry_t& result,
    ValueVector* resultVector, uint64_t rowToAdd, const CSVOption* option) {
    CastStringHelper::cast(reinterpret_cast<const char*>(input.getData()), input.len, result,
        resultVector, rowToAdd, option);
}

// ---------------------- cast String to Map ------------------------------ //
struct SplitStringMapOperation {
    SplitStringMapOperation(uint64_t& offset, ValueVector* resultVector)
        : offset(offset), resultVector(resultVector) {}

    uint64_t& offset;
    ValueVector* resultVector;

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    inline bool handleKey(const char* start, const char* end, const CSVOption* option) {
        trimRightWhitespace(start, end);
        CastString::copyStringToVector(StructVector::getFieldVector(resultVector, 0).get(), offset,
            std::string_view{start, (uint32_t)(end - start)}, option);
        return true;
    }

    inline void handleValue(const char* start, const char* end, const CSVOption* option) {
        trimRightWhitespace(start, end);
        CastString::copyStringToVector(StructVector::getFieldVector(resultVector, 1).get(),
            offset++, std::string_view{start, (uint32_t)(end - start)}, option);
    }
};

template<typename T>
static bool parseKeyOrValue(const char*& input, const char* end, T& state, bool isKey,
    bool& closeBracket, const CSVOption* option) {
    auto start = input;
    uint64_t lvl = 0;

    while (input < end) {
        if (*input == '"' || *input == '\'') {
            if (!skipToCloseQuotes(input, end)) {
                return false;
            }
        } else if (*input == '{') {
            if (!skipToClose(input, end, lvl, '}', option)) {
                return false;
            }
        } else if (*input == CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR) {
            if (!skipToClose(input, end, lvl, CopyConstants::DEFAULT_CSV_LIST_END_CHAR, option)) {
                return false;
            }
        } else if (isKey && *input == '=') {
            return state.handleKey(start, input, option);
        } else if (!isKey && (*input == option->delimiter || *input == '}')) {
            state.handleValue(start, input, option);
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
static bool splitCStringMap(const char* input, uint64_t len, T& state, const CSVOption* option) {
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
        if (!parseKeyOrValue(input, end, state, true, closeBracket, option)) {
            return false;
        }
        skipWhitespace(++input, end);
        if (!parseKeyOrValue(input, end, state, false, closeBracket, option)) {
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
void CastStringHelper::cast(const char* input, uint64_t len, map_entry_t& /*result*/,
    ValueVector* vector, uint64_t rowToAdd, const CSVOption* option) {
    // count the number of maps in map
    CountPartOperation state;
    splitCStringMap(input, len, state, option);

    auto list_entry = ListVector::addList(vector, state.count);
    vector->setValue<list_entry_t>(rowToAdd, list_entry);
    auto structVector = ListVector::getDataVector(vector);

    SplitStringMapOperation split{list_entry.offset, structVector};
    if (!splitCStringMap(input, len, split, option)) {
        throw ConversionException("Cast failed. " + std::string{input, len} + " is not in " +
                                  vector->dataType.toString() + " range.");
    }
}

template<>
void CastString::operation(const ku_string_t& input, map_entry_t& result, ValueVector* resultVector,
    uint64_t rowToAdd, const CSVOption* option) {
    CastStringHelper::cast(reinterpret_cast<const char*>(input.getData()), input.len, result,
        resultVector, rowToAdd, option);
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

static bool parseStructFieldValue(const char*& input, const char* end, const CSVOption* option,
    bool& closeBrack) {
    uint64_t lvl = 0;
    while (input < end) {
        if (*input == '"' || *input == '\'') {
            if (!skipToCloseQuotes(input, end)) {
                return false;
            }
        } else if (*input == '{') {
            if (!skipToClose(input, end, lvl, '}', option)) {
                return false;
            }
        } else if (*input == CopyConstants::DEFAULT_CSV_LIST_BEGIN_CHAR) {
            if (!skipToClose(input, end, ++lvl, CopyConstants::DEFAULT_CSV_LIST_END_CHAR, option)) {
                return false;
            }
        } else if (*input == option->delimiter || *input == '}') {
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
    uint64_t rowToAdd, const CSVOption* option) {
    // default values to NULL
    auto fieldVectors = StructVector::getFieldVectors(vector);
    for (auto& fieldVector : fieldVectors) {
        fieldVector->setNull(rowToAdd, true);
    }

    // check if start with {
    auto end = input + len;
    const auto& type = vector->dataType;
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
        trimQuotes(keyStart, keyEnd);
        auto fieldIdx = StructType::getFieldIdx(type, std::string{keyStart, keyEnd});
        if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw ParserException{"Invalid struct field name: " + std::string{keyStart, keyEnd}};
        }

        skipWhitespace(++input, end);
        auto valStart = input;
        if (!parseStructFieldValue(input, end, option, closeBracket)) { // find value
            return false;
        }
        auto valEnd = input;
        trimRightWhitespace(valStart, valEnd);
        skipWhitespace(++input, end);

        auto fieldVector = StructVector::getFieldVector(vector, fieldIdx).get();
        fieldVector->setNull(rowToAdd, false);
        CastString::copyStringToVector(fieldVector, rowToAdd,
            std::string_view{valStart, (uint32_t)(valEnd - valStart)}, option);

        if (closeBracket) {
            return (input == end);
        }
    }
    return false;
}

template<>
void CastStringHelper::cast(const char* input, uint64_t len, struct_entry_t& /*result*/,
    ValueVector* vector, uint64_t rowToAdd, const CSVOption* option) {
    if (!tryCastStringToStruct(input, len, vector, rowToAdd, option)) {
        throw ConversionException("Cast failed. " + std::string{input, len} + " is not in " +
                                  vector->dataType.toString() + " range.");
    }
}

template<>
void CastString::operation(const ku_string_t& input, struct_entry_t& result,
    ValueVector* resultVector, uint64_t rowToAdd, const CSVOption* option) {
    CastStringHelper::cast(reinterpret_cast<const char*>(input.getData()), input.len, result,
        resultVector, rowToAdd, option);
}

// ---------------------- cast String to Union ------------------------------ //
template<typename T>
static inline void testAndSetValue(ValueVector* vector, uint64_t rowToAdd, T result, bool success) {
    if (success) {
        vector->setValue(rowToAdd, result);
    }
}

static bool tryCastUnionField(ValueVector* vector, uint64_t rowToAdd, const char* input,
    uint64_t len) {
    auto& targetType = vector->dataType;
    bool success = false;
    switch (targetType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        bool result;
        success = function::tryCastToBool(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::INT128: {
        int128_t result;
        success = function::trySimpleInt128Cast(input, len, result);
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
        double result;
        success = function::tryDoubleCast(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::FLOAT: {
        float result;
        success = function::tryDoubleCast(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::DECIMAL: {
        switch (targetType.getPhysicalType()) {
        case PhysicalTypeID::INT16: {
            int16_t result;
            tryDecimalCast(input, len, result, DecimalType::getPrecision(targetType),
                DecimalType::getScale(targetType));
            testAndSetValue(vector, rowToAdd, result, success);
        } break;
        case PhysicalTypeID::INT32: {
            int32_t result;
            tryDecimalCast(input, len, result, DecimalType::getPrecision(targetType),
                DecimalType::getScale(targetType));
            testAndSetValue(vector, rowToAdd, result, success);
        } break;
        case PhysicalTypeID::INT64: {
            int64_t result;
            tryDecimalCast(input, len, result, DecimalType::getPrecision(targetType),
                DecimalType::getScale(targetType));
            testAndSetValue(vector, rowToAdd, result, success);
        } break;
        case PhysicalTypeID::INT128: {
            int128_t result;
            tryDecimalCast(input, len, result, DecimalType::getPrecision(targetType),
                DecimalType::getScale(targetType));
            testAndSetValue(vector, rowToAdd, result, success);
        } break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    case LogicalTypeID::DATE: {
        date_t result;
        uint64_t pos;
        success = Date::tryConvertDate(input, len, pos, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        timestamp_ns_t result;
        success = TryCastStringToTimestamp::tryCast<timestamp_ns_t>(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        timestamp_ms_t result;
        success = TryCastStringToTimestamp::tryCast<timestamp_ms_t>(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        timestamp_sec_t result;
        success = TryCastStringToTimestamp::tryCast<timestamp_sec_t>(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        timestamp_tz_t result;
        success = TryCastStringToTimestamp::tryCast<timestamp_tz_t>(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        timestamp_t result;
        success = Timestamp::tryConvertTimestamp(input, len, result);
        testAndSetValue(vector, rowToAdd, result, success);
    } break;
    case LogicalTypeID::STRING: {
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
void CastStringHelper::cast(const char* input, uint64_t len, union_entry_t& /*result*/,
    ValueVector* vector, uint64_t rowToAdd, const CSVOption* /*option*/) {
    auto& type = vector->dataType;
    union_field_idx_t selectedFieldIdx = INVALID_STRUCT_FIELD_IDX;

    for (auto i = 0u; i < UnionType::getNumFields(type); i++) {
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
            type.toString(), std::string{input, len})};
    }
    StructVector::getFieldVector(vector, UnionType::TAG_FIELD_IDX)
        ->setValue(rowToAdd, selectedFieldIdx);
    StructVector::getFieldVector(vector, UnionType::TAG_FIELD_IDX)
        ->setNull(rowToAdd, false /* isNull */);
}

template<>
void CastString::operation(const ku_string_t& input, union_entry_t& result,
    ValueVector* resultVector, uint64_t rowToAdd, const CSVOption* CSVOption) {
    CastStringHelper::cast(reinterpret_cast<const char*>(input.getData()), input.len, result,
        resultVector, rowToAdd, CSVOption);
}

void CastString::copyStringToVector(ValueVector* vector, uint64_t vectorPos,
    std::string_view strVal, const CSVOption* option) {
    auto& type = vector->dataType;
    if (strVal.empty() || isNull(strVal)) {
        vector->setNull(vectorPos, true /* isNull */);
        return;
    }
    vector->setNull(vectorPos, false /* isNull */);
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::INT128: {
        int128_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        int64_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::INT32: {
        int32_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::INT16: {
        int16_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::INT8: {
        int8_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::UINT64: {
        uint64_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::UINT32: {
        uint32_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::UINT16: {
        uint16_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::UINT8: {
        uint8_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::FLOAT: {
        float val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::DECIMAL: {
        switch (type.getPhysicalType()) {
        case PhysicalTypeID::INT16: {
            int16_t val;
            decimalCast(strVal.data(), strVal.length(), val, type);
            vector->setValue(vectorPos, val);
        } break;
        case PhysicalTypeID::INT32: {
            int32_t val;
            decimalCast(strVal.data(), strVal.length(), val, type);
            vector->setValue(vectorPos, val);
        } break;
        case PhysicalTypeID::INT64: {
            int64_t val;
            decimalCast(strVal.data(), strVal.length(), val, type);
            vector->setValue(vectorPos, val);
        } break;
        case PhysicalTypeID::INT128: {
            int128_t val;
            decimalCast(strVal.data(), strVal.length(), val, type);
            vector->setValue(vectorPos, val);
        } break;
        default:
            KU_UNREACHABLE;
        }
    } break;
    case LogicalTypeID::DOUBLE: {
        double val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::BOOL: {
        bool val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::BLOB: {
        blob_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val, vector, vectorPos, option);
    } break;
    case LogicalTypeID::UUID: {
        ku_uuid_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val.value);
    } break;
    case LogicalTypeID::STRING: {
        if (!utf8proc::Utf8Proc::isValid(strVal.data(), strVal.length())) {
            throw CopyException{"Invalid UTF8-encoded string."};
        }
        StringVector::addString(vector, vectorPos, strVal.data(), strVal.length());
    } break;
    case LogicalTypeID::DATE: {
        date_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        timestamp_ns_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        timestamp_ms_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        timestamp_sec_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        timestamp_tz_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        timestamp_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::INTERVAL: {
        interval_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val);
        vector->setValue(vectorPos, val);
    } break;
    case LogicalTypeID::MAP: {
        map_entry_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val, vector, vectorPos, option);
    } break;
    case LogicalTypeID::ARRAY:
    case LogicalTypeID::LIST: {
        list_entry_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val, vector, vectorPos, option);
    } break;
    case LogicalTypeID::STRUCT: {
        struct_entry_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val, vector, vectorPos, option);
    } break;
    case LogicalTypeID::UNION: {
        union_entry_t val;
        CastStringHelper::cast(strVal.data(), strVal.length(), val, vector, vectorPos, option);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace function
} // namespace kuzu
