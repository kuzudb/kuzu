#include "json_utils.h"

#include <cstdlib>

#include "common/exception/not_implemented.h"
#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/types/value/value.h"
#include "function/cast/functions/cast_decimal.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "function/cast/functions/cast_string_non_nested_functions.h"
#include "function/cast/functions/numeric_limits.h"
#include "json_type.h"

using namespace kuzu::common;

namespace kuzu {
namespace json_extension {

JsonWrapper::~JsonWrapper() {
    if (ptr) {
        yyjson_doc_free(ptr);
        ptr = nullptr;
    }
}

JsonMutWrapper::~JsonMutWrapper() {
    if (ptr) {
        yyjson_mut_doc_free(ptr);
        ptr = nullptr;
    }
}

yyjson_mut_val* jsonifyAsString(JsonMutWrapper& wrapper, const common::ValueVector& vec,
    uint64_t pos) {
    auto strVal = vec.getAsValue(pos)->toString();
    return yyjson_mut_strcpy(wrapper.ptr, strVal.c_str());
}

yyjson_mut_val* jsonify(JsonMutWrapper& wrapper, const common::ValueVector& vec, uint64_t pos) {
    // creates a mutable value linked to wrapper.ptr, but the responsibility to integrate it into
    // wrapper is placed upon the caller
    yyjson_mut_val* result = nullptr;
    if (vec.isNull(pos)) {
        result = yyjson_mut_null(wrapper.ptr);
    } else if (JsonType::isJson(vec.dataType)) {
        auto strVal = vec.getValue<ku_string_t>(pos);
        auto strContent = strVal.getAsStringView();

        // Convert JSON string to an immutable JSON document using stringToJson
        JsonWrapper immutable_json_wrapper = stringToJson(std::string(strContent));
        yyjson_doc* immutable_doc = immutable_json_wrapper.ptr;
        if (!immutable_doc) {
            // Handle error if parsing fails
            return yyjson_mut_null(wrapper.ptr);
        }

        // Create a new mutable document in the wrapper and copy the root from the immutable
        // document
        yyjson_mut_val* mut_root =
            yyjson_val_mut_copy(wrapper.ptr, yyjson_doc_get_root(immutable_doc));

        return mut_root;
    } else {
        switch (vec.dataType.getLogicalTypeID()) {
        case LogicalTypeID::BOOL:
            result = yyjson_mut_bool(wrapper.ptr, vec.getValue<bool>(pos));
            break;
        case LogicalTypeID::SERIAL:
        case LogicalTypeID::INT64:
            result = yyjson_mut_sint(wrapper.ptr, vec.getValue<int64_t>(pos));
            break;
        case LogicalTypeID::INT32:
            result = yyjson_mut_sint(wrapper.ptr, vec.getValue<int32_t>(pos));
            break;
        case LogicalTypeID::INT16:
            result = yyjson_mut_sint(wrapper.ptr, vec.getValue<int16_t>(pos));
            break;
        case LogicalTypeID::INT8:
            result = yyjson_mut_sint(wrapper.ptr, vec.getValue<int8_t>(pos));
            break;
        case LogicalTypeID::UINT64:
            result = yyjson_mut_uint(wrapper.ptr, vec.getValue<uint64_t>(pos));
            break;
        case LogicalTypeID::UINT32:
            result = yyjson_mut_uint(wrapper.ptr, vec.getValue<uint32_t>(pos));
            break;
        case LogicalTypeID::UINT16:
            result = yyjson_mut_uint(wrapper.ptr, vec.getValue<uint16_t>(pos));
            break;
        case LogicalTypeID::UINT8:
            result = yyjson_mut_uint(wrapper.ptr, vec.getValue<uint8_t>(pos));
            break;
        case LogicalTypeID::DOUBLE:
            result = yyjson_mut_real(wrapper.ptr, vec.getValue<double>(pos));
            break;
        case LogicalTypeID::FLOAT:
            result = yyjson_mut_real(wrapper.ptr, vec.getValue<float>(pos));
            break;
        case LogicalTypeID::STRING: {
            auto strVal = vec.getValue<ku_string_t>(pos);
            result = yyjson_mut_strncpy(wrapper.ptr, (const char*)strVal.getData(), strVal.len);
        } break;
        case LogicalTypeID::LIST:
        case LogicalTypeID::ARRAY: {
            result = yyjson_mut_arr(wrapper.ptr);
            const auto dataVector = ListVector::getDataVector(&vec);
            auto listEntry = vec.getValue<list_entry_t>(pos);
            for (auto i = 0u; i < listEntry.size; ++i) {
                yyjson_mut_arr_append(result, jsonify(wrapper, *dataVector, listEntry.offset + i));
            }
        } break;
        case LogicalTypeID::MAP: {
            result = yyjson_mut_obj(wrapper.ptr);
            auto listEntry = vec.getValue<list_entry_t>(pos);
            const auto keyDataVector = MapVector::getKeyVector(&vec);
            const auto valDataVector = MapVector::getValueVector(&vec);
            for (auto i = 0u; i < listEntry.size; ++i) {
                yyjson_mut_obj_add(result,
                    jsonifyAsString(wrapper, *keyDataVector, listEntry.offset + i),
                    jsonify(wrapper, *valDataVector, listEntry.offset + i));
            }
        } break;
        case LogicalTypeID::RECURSIVE_REL:
        case LogicalTypeID::NODE:
        case LogicalTypeID::REL:
        case LogicalTypeID::STRUCT: {
            result = yyjson_mut_obj(wrapper.ptr);
            const auto& fieldVectors = StructVector::getFieldVectors(&vec);
            const auto& fieldNames = StructType::getFieldNames(vec.dataType);
            for (auto i = 0u; i < fieldVectors.size(); i++) {
                auto key = yyjson_mut_strcpy(wrapper.ptr, fieldNames[i].c_str());
                auto val = jsonify(wrapper, *fieldVectors[i], pos);
                yyjson_mut_obj_add(result, key, val);
            }
        } break;
        case LogicalTypeID::UNION: {
            const auto& tagVector = UnionVector::getTagVector(&vec);
            const auto& fieldVector =
                UnionVector::getValVector(&vec, tagVector->getValue<union_field_idx_t>(pos));
            result = jsonify(wrapper, *fieldVector, pos);
        } break;
        default: {
            return jsonifyAsString(wrapper, vec, pos);
        }
        }
    }
    return result;
}

JsonWrapper jsonify(const common::ValueVector& vec, uint64_t pos) {
    JsonMutWrapper result; // mutability necessary for manual construction
    if (!vec.isNull(pos) && vec.dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        auto strVal = vec.getValue<ku_string_t>(pos);
        // check one more time in case this string a struct
        auto strContent = strVal.getAsStringView();
        LogicalType detectedType = function::inferMinimalTypeFromString(strContent);
        if (detectedType.getLogicalTypeID() != LogicalTypeID::STRING) {
            return stringToJson(std::string(strContent));
        }
    }
    yyjson_mut_val* root = jsonify(result, vec, pos);
    yyjson_mut_doc_set_root(result.ptr, root);
    return JsonWrapper(yyjson_mut_doc_imut_copy(result.ptr, nullptr));
}

std::vector<JsonWrapper> jsonifyQueryResult(
    const std::vector<std::shared_ptr<common::ValueVector>>& columns,
    const std::vector<std::string>& names) {
    auto numRows = 1u; // 1 if all vectors are flat
    for (auto i = 0u; i < columns.size(); i++) {
        if (!columns[i]->state->isFlat()) {
            numRows = columns[i]->state->getSelVector().getSelSize();
            break;
        }
    }
    std::vector<JsonWrapper> result;
    std::vector<yyjson_val*> firstResultValues;
    // save pointer to first result values so that flat tuples can just copy over
    for (auto i = 0u; i < numRows; i++) {
        JsonMutWrapper curResult;
        auto root = yyjson_mut_obj(curResult.ptr);
        yyjson_mut_doc_set_root(curResult.ptr, root);
        for (auto j = 0u; j < columns.size(); j++) {
            auto key = yyjson_mut_strncpy(curResult.ptr, names[j].c_str(), names[j].size());
            if (columns[j]->state->isFlat() && i > 0) {
                // copy from first result
                auto val = yyjson_val_mut_copy(curResult.ptr, firstResultValues[j]);
                yyjson_mut_obj_add(root, key, val);
            } else {
                // create new yyjson value
                auto val = jsonify(curResult, *columns[j],
                    columns[j]->state->getSelVector().getSelectedPositions()[i]);
                yyjson_mut_obj_add(root, key, val);
            }
        }
        result.push_back(JsonWrapper(yyjson_mut_doc_imut_copy(curResult.ptr, nullptr)));
        if (i == 0) {
            yyjson_val *key = nullptr, *val = nullptr;
            auto iter = yyjson_obj_iter_with(yyjson_doc_get_root(result[i].ptr));
            while ((key = yyjson_obj_iter_next(&iter))) {
                val = yyjson_obj_iter_get_val(key);
                firstResultValues.push_back(val);
            }
        }
    }
    return result;
}

common::LogicalType jsonSchema(yyjson_val* val, int64_t depth, int64_t breadth) {
    if (depth == 0) {
        return LogicalType::STRING();
    }
    depth = depth == -1 ? depth : depth - 1;
    switch (yyjson_get_type(val)) {
    case YYJSON_TYPE_ARR: {
        common::LogicalType childType(LogicalTypeID::ANY); // todo: make a null dt and replace this
        yyjson_val* ele = nullptr;
        auto iter = yyjson_arr_iter_with(val);
        auto sampled = 0u;
        while ((ele = yyjson_arr_iter_next(&iter))) {
            childType = LogicalTypeUtils::combineTypes(childType, jsonSchema(ele, depth, -1));
            if (breadth != -1 && ++sampled >= breadth) {
                break;
            }
        }
        return LogicalType::LIST(std::move(childType));
    }
    case YYJSON_TYPE_OBJ: {
        std::vector<StructField> fields;
        yyjson_val *key = nullptr, *ele = nullptr;
        auto iter = yyjson_obj_iter_with(val);
        auto sampled = 0u;
        while ((key = yyjson_obj_iter_next(&iter))) {
            ele = yyjson_obj_iter_get_val(key);
            fields.emplace_back(std::string(yyjson_get_str(key)), jsonSchema(ele, depth, -1));
            if (breadth != -1 && ++sampled >= breadth) {
                break;
            }
        }
        return LogicalType::STRUCT(std::move(fields));
    }
    case YYJSON_TYPE_NULL:
        return LogicalType::ANY();
    case YYJSON_TYPE_BOOL:
        return LogicalType::BOOL();
    case YYJSON_TYPE_NUM:
        switch (yyjson_get_subtype(val)) {
        case YYJSON_SUBTYPE_UINT: {
            auto value = yyjson_get_uint(val);
            if (function::NumericLimits<uint8_t>::isInBounds(value)) {
                return LogicalType::UINT8();
            } else if (function::NumericLimits<uint16_t>::isInBounds(value)) {
                return LogicalType::UINT16();
            } else if (function::NumericLimits<uint32_t>::isInBounds(value)) {
                return LogicalType::UINT32();
            } else {
                return LogicalType::UINT64();
            }
        }
        case YYJSON_SUBTYPE_SINT: {
            auto value = yyjson_get_sint(val);
            if (function::NumericLimits<int8_t>::isInBounds(value)) {
                return LogicalType::INT8();
            } else if (function::NumericLimits<int16_t>::isInBounds(value)) {
                return LogicalType::INT16();
            } else if (function::NumericLimits<int32_t>::isInBounds(value)) {
                return LogicalType::INT32();
            } else {
                return LogicalType::INT64();
            }
        }
        case YYJSON_SUBTYPE_REAL:
            return LogicalType::DOUBLE();
        default:
            KU_UNREACHABLE;
        }
    case YYJSON_TYPE_STR: {
        auto value = yyjson_get_str(val);
        LogicalType detectedType = function::inferMinimalTypeFromString(std::string_view(value));
        // These are written inside quote in JSON standard, so we need to distinguish them from
        // string
        if (detectedType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP ||
            detectedType.getLogicalTypeID() == LogicalTypeID::DATE ||
            detectedType.getLogicalTypeID() == LogicalTypeID::INTERVAL ||
            detectedType.getLogicalTypeID() == LogicalTypeID::UUID) {
            return detectedType;
        }
        // The other quoted variable will be actual string
        return LogicalType::STRING();
    }
    default:
        KU_UNREACHABLE;
    }
}

common::LogicalType jsonSchema(const JsonWrapper& wrapper, int64_t depth, int64_t breadth) {
    return jsonSchema(yyjson_doc_get_root(wrapper.ptr), depth, breadth);
}

static void readFromJsonArr(yyjson_val* val, common::ValueVector& vec, uint64_t pos) {
    const auto& outputType = vec.dataType;
    vec.setNull(pos, false);
    switch (outputType.getLogicalTypeID()) {
    case LogicalTypeID::ARRAY:
    case LogicalTypeID::LIST: {
        if (outputType.getLogicalTypeID() == LogicalTypeID::ARRAY) {
            if (yyjson_arr_size(val) != ArrayType::getNumElements(outputType)) {
                throw RuntimeException(
                    stringFormat("Expected type {} but list type has {} elements",
                        outputType.toString(), yyjson_arr_size(val)));
            }
        }
        auto lst = ListVector::addList(&vec, yyjson_arr_size(val));
        vec.setValue(pos, lst);
        auto childVec = ListVector::getDataVector(&vec);
        auto childPos = lst.offset;

        auto it = yyjson_arr_iter_with(val);
        yyjson_val* ele = nullptr;
        while ((ele = yyjson_arr_iter_next(&it))) {
            readJsonToValueVector(ele, *childVec, childPos);
            childPos++;
        }
    } break;
    case LogicalTypeID::STRING: {
        auto str = jsonToString(val);
        StringVector::addString(&vec, pos, str);
    } break;
    case LogicalTypeID::UNION: {
        // find column that is of type array or list
        bool foundType = false;
        for (auto i = 0u; i < UnionType::getNumFields(outputType); i++) {
            auto& childType = UnionType::getFieldType(outputType, i);
            if (childType.getLogicalTypeID() == LogicalTypeID::LIST ||
                childType.getLogicalTypeID() == LogicalTypeID::ARRAY) {
                UnionVector::getTagVector(&vec)->setValue<union_field_idx_t>(pos, i);
                readFromJsonArr(val, *UnionVector::getValVector(&vec, i), pos);
                foundType = true;
                break;
            }
        }
        // no type was found
        if (!foundType) {
            throw NotImplementedException(
                stringFormat("Cannot read from JSON Array to {}", outputType.toString()));
        }
    } break;
    default:
        throw NotImplementedException(
            stringFormat("Cannot read from JSON Array to {}", outputType.toString()));
    }
}

static void readFromJsonObj(yyjson_val* val, common::ValueVector& vec, uint64_t pos) {
    const auto& outputType = vec.dataType;
    vec.setNull(pos, false);
    switch (outputType.getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        vec.setValue<int64_t>(pos, pos);
        auto names = StructType::getFieldNames(outputType);
        for (auto i = 0u; i < StructType::getNumFields(outputType); i++) {
            auto childVec = StructVector::getFieldVector(&vec, i);
            auto childObj = yyjson_obj_get(val, names[i].c_str());
            if (childObj == nullptr) {
                childVec->setNull(pos, true);
            } else {
                readJsonToValueVector(childObj, *childVec, pos);
            }
        }
    } break;
    case LogicalTypeID::MAP: {
        auto numFields = yyjson_obj_size(val);
        auto keyBuffer = MapVector::getKeyVector(&vec);
        auto valBuffer = MapVector::getValueVector(&vec);
        auto listEntry = ListVector::addList(&vec, numFields);
        vec.setValue<list_entry_t>(pos, listEntry);
        yyjson_val *key = nullptr, *value = nullptr;
        auto it = yyjson_obj_iter_with(val);
        for (auto i = 0u; i < numFields; i++) {
            key = yyjson_obj_iter_next(&it);
            value = yyjson_obj_iter_get_val(key);
            StringVector::addString(keyBuffer, listEntry.offset + i,
                std::string(yyjson_get_str(key)));
            readJsonToValueVector(value, *valBuffer, listEntry.offset + i);
        }
    } break;
    case LogicalTypeID::STRING: {
        auto str = jsonToString(val);
        StringVector::addString(&vec, pos, str);
    } break;
    case LogicalTypeID::UNION: {
        // find column that is of type array or list
        bool foundType = false;
        for (auto i = 0u; i < UnionType::getNumFields(outputType); i++) {
            auto& childType = UnionType::getFieldType(outputType, i);
            if (childType.getLogicalTypeID() == LogicalTypeID::STRUCT ||
                childType.getLogicalTypeID() == LogicalTypeID::MAP) {
                UnionVector::getTagVector(&vec)->setValue<union_field_idx_t>(pos, i);
                readFromJsonObj(val, *UnionVector::getValVector(&vec, i), pos);
                foundType = true;
                break;
            }
        }
        // no type was found
        if (!foundType) {
            throw NotImplementedException(
                stringFormat("Cannot read from JSON Object to {}", outputType.toString()));
        }
    } break;
    default:
        throw NotImplementedException(
            stringFormat("Cannot read from JSON Object to {}", outputType.toString()));
    }
}

static void readFromJsonBool(bool val, common::ValueVector& vec, uint64_t pos) {
    const auto& outputType = vec.dataType;
    vec.setNull(pos, false);
    switch (outputType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        vec.setValue<bool>(pos, val);
        break;
    case LogicalTypeID::STRING:
        StringVector::addString(&vec, pos, val ? "True" : "False");
        break;
    case LogicalTypeID::UNION: {
        // find column that is of type array or list
        bool foundType = false;
        for (auto i = 0u; i < UnionType::getNumFields(outputType); i++) {
            auto& childType = UnionType::getFieldType(outputType, i);
            if (childType.getLogicalTypeID() == LogicalTypeID::BOOL) {
                UnionVector::getTagVector(&vec)->setValue<union_field_idx_t>(pos, i);
                UnionVector::getValVector(&vec, i)->setValue<bool>(pos, val);
                foundType = true;
                break;
            }
        }
        // no type was found
        if (!foundType) {
            throw NotImplementedException(
                stringFormat("Cannot read from JSON Bool to {}", outputType.toString()));
        }
    } break;
    default:
        throw NotImplementedException(
            stringFormat("Cannot read from JSON Bool to {}", outputType.toString()));
    }
}

template<typename NUM_TYPE>
static void readFromJsonNum(NUM_TYPE val, common::ValueVector& vec, uint64_t pos) {
    const auto& outputType = vec.dataType;
    vec.setNull(pos, false);
    switch (outputType.getLogicalTypeID()) {
    case LogicalTypeID::INT8:
        vec.setValue<int8_t>(pos, (int8_t)val);
        break;
    case LogicalTypeID::INT16:
        vec.setValue<int16_t>(pos, (int16_t)val);
        break;
    case LogicalTypeID::INT32:
        vec.setValue<int32_t>(pos, (int32_t)val);
        break;
    case LogicalTypeID::INT64:
        vec.setValue<int64_t>(pos, (int64_t)val);
        break;
    case LogicalTypeID::INT128:
        vec.setValue<int128_t>(pos, int128_t(val));
        break;
    case LogicalTypeID::UINT8:
        vec.setValue<uint8_t>(pos, (uint8_t)val);
        break;
    case LogicalTypeID::UINT16:
        vec.setValue<uint16_t>(pos, (uint16_t)val);
        break;
    case LogicalTypeID::UINT32:
        vec.setValue<uint32_t>(pos, (uint32_t)val);
        break;
    case LogicalTypeID::UINT64:
        vec.setValue<uint64_t>(pos, (uint64_t)val);
        break;
    case LogicalTypeID::FLOAT:
        vec.setValue<float>(pos, (float)val);
        break;
    case LogicalTypeID::DOUBLE:
        vec.setValue<double>(pos, (double)val);
        break;
    case LogicalTypeID::DECIMAL:
        switch (outputType.getPhysicalType()) {
        case PhysicalTypeID::INT16:
            function::CastToDecimal::operation(val, vec.getValue<int16_t>(pos), vec, vec);
            break;
        case PhysicalTypeID::INT32:
            function::CastToDecimal::operation(val, vec.getValue<int32_t>(pos), vec, vec);
            break;
        case PhysicalTypeID::INT64:
            function::CastToDecimal::operation(val, vec.getValue<int64_t>(pos), vec, vec);
            break;
        case PhysicalTypeID::INT128:
            function::CastToDecimal::operation(val, vec.getValue<int128_t>(pos), vec, vec);
            break;
        default:
            KU_UNREACHABLE;
        }
        break;
    case LogicalTypeID::STRING: {
        auto str = std::to_string(val);
        StringVector::addString(&vec, pos, str);
    } break;
    case LogicalTypeID::UNION: {
        // find column that is of type array or list
        bool foundType = false;
        for (auto i = 0u; i < UnionType::getNumFields(outputType); i++) {
            auto& childType = UnionType::getFieldType(outputType, i);
            if (LogicalTypeUtils::isNumerical(childType)) {
                UnionVector::getTagVector(&vec)->setValue<union_field_idx_t>(pos, i);
                readFromJsonNum(val, *UnionVector::getValVector(&vec, i), pos);
                foundType = true;
                break;
            }
        }
        // no type was found
        if (!foundType) {
            throw NotImplementedException(
                stringFormat("Cannot read from JSON Number to {}", outputType.toString()));
        }
    } break;
    default:
        throw NotImplementedException(
            stringFormat("Cannot read from JSON Number to {}", outputType.toString()));
    }
}

static void readFromJsonStr(std::string val, common::ValueVector& vec, uint64_t pos) {
    vec.setNull(pos, false);
    if (vec.dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        // casting produces undesired behaviour when the dest requires no cast
        StringVector::addString(&vec, pos, val);
        return;
    }
    CSVOption opt;
    function::CastString::copyStringToVector(&vec, pos, val, &opt);
}

void readJsonToValueVector(yyjson_val* val, common::ValueVector& vec, uint64_t pos) {
    switch (yyjson_get_type(val)) {
    case YYJSON_TYPE_ARR:
        readFromJsonArr(val, vec, pos);
        break;
    case YYJSON_TYPE_OBJ:
        readFromJsonObj(val, vec, pos);
        break;
    case YYJSON_TYPE_NULL:
        vec.setNull(pos, true);
        break;
    case YYJSON_TYPE_BOOL:
        readFromJsonBool(yyjson_get_bool(val), vec, pos);
        break;
    case YYJSON_TYPE_NUM:
        switch (yyjson_get_subtype(val)) {
        case YYJSON_SUBTYPE_UINT:
            readFromJsonNum(yyjson_get_uint(val), vec, pos);
            break;
        case YYJSON_SUBTYPE_SINT:
            readFromJsonNum(yyjson_get_sint(val), vec, pos);
            break;
        case YYJSON_SUBTYPE_REAL:
            readFromJsonNum(yyjson_get_real(val), vec, pos);
            break;
        default:
            KU_UNREACHABLE;
        }
        break;
    case YYJSON_TYPE_STR:
        readFromJsonStr(yyjson_get_str(val), vec, pos);
        break;
    default:
        KU_UNREACHABLE;
    }
}

std::string jsonToString(const JsonWrapper& wrapper) {
    auto tmp = yyjson_write(wrapper.ptr, 0 /* flg */, nullptr /* len */);
    std::string str(tmp ? tmp : "");
    free(tmp); // call free to avoid alloc-dealloc mismatch
    return str;
}

std::string jsonToString(const yyjson_val* val) {
    auto tmp = yyjson_val_write(val, 0, nullptr);
    std::string str(tmp ? tmp : "");
    free(tmp); // call free to avoid alloc-dealloc mismatch
    return str;
}

void invalidJsonError(const char* data, size_t size, yyjson_read_err* err) {
    size_t line = 0, col = 0, chr = 0;
    if (yyjson_locate_pos(data, size, err->pos, &line, &col, &chr)) {
        throw RuntimeException(stringFormat("Error {} at line {}, column {}, character index {}",
            err->msg, line, col, chr));
    } else {
        throw RuntimeException(stringFormat("Error {} at byte {}", err->msg, err->pos));
    }
}

JsonWrapper stringToJson(const std::string& str) {
    yyjson_read_err err;
    auto json = yyjson_read_opts(const_cast<char*>(str.c_str()), str.size(), 0, nullptr, &err);
    if (json == nullptr) {
        invalidJsonError(str.c_str(), str.size(), &err);
    }
    return JsonWrapper(json);
}

JsonWrapper stringToJsonNoError(const std::string& str) {
    return JsonWrapper(yyjson_read(str.c_str(), str.size(), 0 /* flg */));
}

JsonWrapper mergeJson(const JsonWrapper& A, const JsonWrapper& B) {
    JsonMutWrapper ret;
    auto root = yyjson_merge_patch(ret.ptr, yyjson_doc_get_root(A.ptr), yyjson_doc_get_root(B.ptr));
    yyjson_mut_doc_set_root(ret.ptr, root);
    return JsonWrapper(yyjson_mut_doc_imut_copy(ret.ptr, nullptr /* alc */), nullptr /* buffer */);
}

std::string jsonExtractToString(const JsonWrapper& wrapper, uint64_t pos) {
    return jsonToString(yyjson_arr_get(yyjson_doc_get_root(wrapper.ptr), pos));
}

std::string jsonExtractToString(const JsonWrapper& wrapper, std::string path) {
    std::vector<std::string> actualPath;
    for (auto i = 0u, prvDelim = 0u; i <= path.size(); i++) {
        if (i == path.size() || path[i] == '/' || path[i] == '.') {
            actualPath.push_back(path.substr(prvDelim, i - prvDelim));
            prvDelim = i + 1;
        }
    }
    yyjson_val* ptr = yyjson_doc_get_root(wrapper.ptr);
    for (const auto& item : actualPath) {
        if (&item == &actualPath.front() && item == "$") {
            continue;
        }
        if (yyjson_get_type(ptr) == YYJSON_TYPE_OBJ) {
            ptr = yyjson_obj_get(ptr, item.c_str());
        } else {
            int32_t idx = -1;
            if (!function::trySimpleIntegerCast(item.c_str(), item.length(), idx)) {
                return "";
            }
            ptr = yyjson_arr_get(ptr, idx);
        }
        if (ptr == nullptr) {
            return "";
        }
    }
    return jsonToString(ptr);
}

uint32_t jsonArraySize(const JsonWrapper& wrapper) {
    return yyjson_arr_size(yyjson_doc_get_root(wrapper.ptr));
}

} // namespace json_extension
} // namespace kuzu
