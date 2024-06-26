#include "json_utils.h"

#include "common/exception/binder.h"
#include "common/exception/not_implemented.h"
#include "common/exception/runtime.h"
#include "common/types/value/value.h"

using namespace kuzu::common;

namespace kuzu {
namespace json_extension {

static yyjson_mut_val* jsonifyAsString(JsonMutWrapper& wrapper, const common::ValueVector& vec, uint64_t pos) {
    auto strVal = "\"" + vec.getAsValue(pos)->toString() + "\"";
    return yyjson_mut_strcpy(wrapper.ptr, strVal.c_str());
}

static yyjson_mut_val* jsonify(JsonMutWrapper& wrapper, const common::ValueVector& vec, uint64_t pos)  {
    yyjson_mut_val* result = nullptr;
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
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        auto strVal = "\"" + vec.getValue<ku_string_t>(pos).getAsString() + "\"";
        result = yyjson_mut_strcpy(wrapper.ptr, strVal.c_str());
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
        const auto keyVector = MapVector::getKeyVector(&vec);
        const auto keyDataVector = ListVector::getDataVector(keyVector);
        auto keyListEntry = keyVector->getValue<list_entry_t>(pos);
        const auto valVector = MapVector::getValueVector(&vec);
        const auto valDataVector = ListVector::getDataVector(valVector);
        auto valListEntry = valVector->getValue<list_entry_t>(pos);
        for (auto i = 0u; i < keyListEntry.size; ++i) {
            yyjson_mut_obj_add(result,
                jsonifyAsString(wrapper, *keyDataVector, keyListEntry.offset + i),
                jsonify(wrapper, *valDataVector, valListEntry.offset + i));
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
        const auto& fieldVector = UnionVector::getValVector(&vec, tagVector->getValue<union_field_idx_t>(pos));
        result = jsonify(wrapper, *fieldVector, pos);
    } break;
    case LogicalTypeID::INTERNAL_ID:
    case LogicalTypeID::UUID:
    case LogicalTypeID::DATE: {
        return jsonifyAsString(wrapper, vec, pos);
    } break;
    case LogicalTypeID::ANY:
    default:
        throw NotImplementedException(stringFormat("Type {} to json conversion not supported", 
            vec.dataType.toString()));
    }
    return result;
}

JsonWrapper jsonify(const common::ValueVector& vec, uint64_t pos) {
    JsonMutWrapper result; // mutability necessary for manual construction
    yyjson_mut_val* root = jsonify(result, vec, pos);
    yyjson_mut_doc_set_root(result.ptr, root);
    return JsonWrapper(yyjson_mut_doc_imut_copy(result.ptr, nullptr));
}

static common::LogicalType combineTypeJsonContext(const common::LogicalType& lft,
    const common::LogicalType& rit) { // always succeeds
    if (lft.getLogicalTypeID() == LogicalTypeID::STRING || rit.getLogicalTypeID() == LogicalTypeID::STRING) {
        return LogicalType::STRING();
    }
    if (lft.getLogicalTypeID() == rit.getLogicalTypeID() && lft.getLogicalTypeID() == LogicalTypeID::STRUCT) {
        std::vector<StructField> resultingFields;
        for (const auto& i : StructType::getFields(lft)) {
            auto name = i.getName();
            if (StructType::hasField(rit, name)) {
                resultingFields.emplace_back(name, combineTypeJsonContext(i.getType(),
                    StructType::getFieldType(rit, name)));
            } else {
                resultingFields.push_back(i.copy());
            }
        }
        for (const auto& i: StructType::getFields(rit)) {
            auto name = i.getName();
            if (!StructType::hasField(lft, name)) {
                resultingFields.push_back(i.copy());
            }
        }
        return LogicalType::STRUCT(std::move(resultingFields));
    }
    common::LogicalType result;
    if (!LogicalTypeUtils::tryGetMaxLogicalType(lft, rit, result)) {
        return LogicalType::STRING();
    }
    return result;
}

common::LogicalType jsonSchema(yyjson_val* val) {
    switch(yyjson_get_type(val)) {
    case YYJSON_TYPE_ARR: {
        common::LogicalType childType(LogicalTypeID::ANY);
        yyjson_val* ele;
        auto iter = yyjson_arr_iter_with(val);
        while((ele = yyjson_arr_iter_next(&iter))) {
            childType = combineTypeJsonContext(childType, jsonSchema(ele));
        }
        return LogicalType::LIST(std::move(childType));
    } break;
    case YYJSON_TYPE_OBJ: {
        std::vector<StructField> fields;
        yyjson_val *key, *ele;
        auto iter = yyjson_obj_iter_with(val);
        while ((key = yyjson_obj_iter_next(&iter))) {
            ele = yyjson_obj_iter_get_val(key);
            fields.emplace_back(std::string(yyjson_get_str(key)), jsonSchema(ele));
        }
        return LogicalType::STRUCT(std::move(fields));
    } break;
    case YYJSON_TYPE_NULL:
        return LogicalType::ANY();
    case YYJSON_TYPE_BOOL:
        return LogicalType::BOOL();
    case YYJSON_TYPE_NUM:
        switch (yyjson_get_subtype(val)) {
        case YYJSON_SUBTYPE_UINT:
            return LogicalType::UINT64();
        case YYJSON_SUBTYPE_SINT:
            return LogicalType::INT64();
        case YYJSON_SUBTYPE_REAL:
            return LogicalType::DOUBLE();
        default:
            KU_UNREACHABLE;
        }
    case YYJSON_TYPE_STR:
        return LogicalType::STRING();
    default:
        KU_UNREACHABLE;
    }
}

common::LogicalType jsonSchema(const JsonWrapper& wrapper) {
    return jsonSchema(yyjson_doc_get_root(wrapper.ptr));
}

// Precondition: vec.dataType is not STRING
static void readFromJsonArr(yyjson_val* val, common::ValueVector& vec, uint64_t pos) {
    const auto& outputType = vec.dataType;
    vec.setNull(pos, false);
    switch (outputType.getLogicalTypeID()) {
    case LogicalTypeID::LIST: {
        auto lst = ListVector::addList(&vec, yyjson_arr_size(val));
        vec.setValue(pos, lst);
        auto childVec = ListVector::getDataVector(&vec);
        auto childPos = lst.offset;

        auto it = yyjson_arr_iter_with(val);
        yyjson_val* ele;
        while ((ele = yyjson_arr_iter_next(&it))) {
            readJsonToValueVector(ele, *childVec, childPos);
            childPos++;
        }
    } break;
    case LogicalTypeID::STRING: {
        auto str = jsonToString(val);
        StringVector::addString(&vec, pos, str);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

// Precondition: vec.dataType is not STRING
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
    case LogicalTypeID::STRING: {
        auto str = jsonToString(val);
        StringVector::addString(&vec, pos, str);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

// Precondition: vec.dataType is not STRING
template<typename NUM_TYPE>
static void readFromJsonNum(NUM_TYPE val, common::ValueVector& vec, uint64_t pos) {
    const auto& outputType = vec.dataType;
    vec.setNull(pos, false);
    switch (outputType.getLogicalTypeID()) {
    case LogicalTypeID::INT128:
        vec.setValue<int128_t>(pos, int128_t(val));
        break;
    case LogicalTypeID::INT64:
        vec.setValue<int64_t>(pos, (int64_t)val);
        break;
    case LogicalTypeID::UINT64:
        vec.setValue<uint64_t>(pos, (uint64_t)val);
        break;
    case LogicalTypeID::DOUBLE:
        vec.setValue<double>(pos, (double)val);
        break;
    case LogicalTypeID::STRING: {
        auto str = std::to_string(val);
        StringVector::addString(&vec, pos, str);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void readJsonToValueVector(yyjson_val* val, common::ValueVector& vec, uint64_t pos) {
    switch(yyjson_get_type(val)) {
    case YYJSON_TYPE_ARR:
        readFromJsonArr(val, vec, pos);
        break;
    case YYJSON_TYPE_OBJ:
        readFromJsonObj(val, vec, pos);
        break;
    case YYJSON_TYPE_NULL:
        vec.setNull(pos, true);
        // Technically unnecessary given this function's one use case, but
        // removing this line may cause this function to behave unexpectedly for possible future use cases
        break;
    case YYJSON_TYPE_BOOL:
        vec.setNull(pos, false);
        vec.setValue<bool>(pos, yyjson_get_bool(val));
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
        }
        break;
    case YYJSON_TYPE_STR:
        vec.setNull(pos, false);
        StringVector::addString(&vec, pos, yyjson_get_str(val));
        break;
    default:
        KU_UNREACHABLE;
    }
}

std::string jsonToString(const JsonWrapper& wrapper) {
    std::unique_ptr<char[]> arr(yyjson_write(wrapper.ptr, 0, nullptr));
    if (arr.get()) {
        return std::string(arr.get());
    } else {
        return "";
    }
}

std::string jsonToString(const yyjson_val* val) {
    std::unique_ptr<char[]> arr(yyjson_val_write(val, 0, nullptr));
    if (arr.get()) {
        return std::string(arr.get());
    } else {
        return "";
    }
}

JsonWrapper stringToJson(const std::string& str) {
    auto json = yyjson_read(str.c_str(), str.size(), 0);
    if (json == nullptr) {
        throw RuntimeException("Invalid json input");
    }
    return JsonWrapper(json);
}

JsonWrapper fileToJson(const std::string& str) {
    yyjson_read_err err;
    auto result = yyjson_read_file(str.c_str(), 0, nullptr, &err);
    if (err.msg != nullptr) {
        throw BinderException(stringFormat(
            "Failed to read json file:\n{}\n",
            std::string(err.msg)));
    }
    return JsonWrapper(result);
}

JsonWrapper mergeJson(const JsonWrapper& A, const JsonWrapper& B) {
    JsonMutWrapper ret;
    auto root = yyjson_merge_patch(ret.ptr,
        yyjson_doc_get_root(A.ptr),
        yyjson_doc_get_root(B.ptr));
    yyjson_mut_doc_set_root(ret.ptr, root);
    return JsonWrapper(yyjson_mut_doc_imut_copy(ret.ptr, nullptr));
}

std::string jsonExtractToString(const JsonWrapper& wrapper, uint64_t pos) {
    return jsonToString(yyjson_arr_get(yyjson_doc_get_root(wrapper.ptr), pos));
}

std::string jsonExtractToString(const JsonWrapper& wrapper, std::string path) {
    std::vector<uint32_t> delimiters;
    delimiters.push_back(0);
    for (auto i = 0u; i < path.size(); i++) {
        if (path[i] == '/') {
            delimiters.push_back(i);
        }
    }
    delimiters.push_back(path.size());
    yyjson_val* ptr = yyjson_doc_get_root(wrapper.ptr);
    for (auto i = 0u; i + 1 < delimiters.size() && ptr; i++) {
        ptr = yyjson_obj_getn(ptr, path.c_str() + delimiters[i] + 1, delimiters[i + 1] - delimiters[i] - 1);
    }
    return jsonToString(ptr);
}

size_t jsonArraySize(const JsonWrapper& wrapper) {
    return yyjson_arr_size(yyjson_doc_get_root(wrapper.ptr));
}

static bool jsonContains(yyjson_val* haystack, yyjson_val* needle);

static bool jsonContainsArrArr(yyjson_val* haystack, yyjson_val* needle) {
    size_t needleIdx, needleMax, haystackIdx, haystackMax;
    yyjson_val* needleChild, *haystackChild;
    yyjson_arr_foreach(needle, needleIdx, needleMax, needleChild) {
        bool found = false;
        yyjson_arr_foreach(haystack, haystackIdx, haystackMax, haystackChild) {
            if (jsonContains(haystackChild, needleChild)) {
                found = true;
            }
        }
        if (!found) {
            return false;
        }
    }
    return true;
}

static bool jsonContainsArr(yyjson_val* haystack, yyjson_val* needle) {
    size_t haystackIdx, haystackMax;
    yyjson_val* haystackChild;
    yyjson_arr_foreach(haystack, haystackIdx, haystackMax, haystackChild) {
        if (jsonContains(haystackChild, needle)) {
            return true;
        }
    }
    return false;
}

static bool jsonContainsObjObj(yyjson_val* haystack, yyjson_val* needle) {
    size_t needleIdx, needleMax;
    yyjson_val *key, *needleChild;
    yyjson_obj_foreach(needle, needleIdx, needleMax, key, needleChild) {
        auto haystackChild = yyjson_obj_getn(haystack, yyjson_get_str(key), yyjson_get_len(key));
        if (!haystackChild || !jsonContains(haystackChild, needleChild)) {
            return false;
        }
    }
    return true;
}

// consider fuzzy equivalency checking optimization?
static bool jsonContains(yyjson_val* haystack, yyjson_val* needle) {
    switch(yyjson_get_type(haystack)) {
    case YYJSON_TYPE_ARR:
        if (yyjson_get_type(needle) == YYJSON_TYPE_ARR) {
            return jsonContainsArrArr(haystack, needle);
        } else {
            return jsonContainsArr(haystack, needle);
        }
    case YYJSON_TYPE_OBJ:
        return jsonContainsObjObj(haystack, needle);
    case YYJSON_TYPE_NULL:
    case YYJSON_TYPE_BOOL:
    case YYJSON_TYPE_NUM:
    case YYJSON_TYPE_STR:
        return yyjson_equals(haystack, needle);
    default:
        return false;
    }
}

bool jsonContains(const JsonWrapper& haystack, const JsonWrapper& needle) {
    return jsonContains(yyjson_doc_get_root(haystack.ptr), yyjson_doc_get_root(needle.ptr));
}

std::vector<std::string> jsonGetKeys(const JsonWrapper& wrapper) {
    std::vector<std::string> result;
    yyjson_val* key;
    auto iter = yyjson_obj_iter_with(yyjson_doc_get_root(wrapper.ptr));
    while ((key = yyjson_obj_iter_next(&iter))) {
        result.emplace_back(yyjson_get_str(key));
    }
    return result;
}

} // namespace json_extension
} // namespace kuzu
