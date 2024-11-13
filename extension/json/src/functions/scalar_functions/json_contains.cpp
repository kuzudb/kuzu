#include "common/json_common.h"
#include "function/scalar_function.h"
#include "json_scalar_functions.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static bool jsonFuzzyEquals(yyjson_val* haystack, yyjson_val* needle);

static bool jsonArrayFuzzyEquals(yyjson_val* haystack, yyjson_val* needle) {
    KU_ASSERT(yyjson_get_tag(haystack) == (YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE) &&
              yyjson_get_tag(needle) == (YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE));

    size_t needleIdx = 0, needleMax = 0, haystackIdx = 0, haystackMax = 0;
    yyjson_val *needleChild = nullptr, *haystackChild = nullptr;
    yyjson_arr_foreach(needle, needleIdx, needleMax, needleChild) {
        bool found = false;
        yyjson_arr_foreach(haystack, haystackIdx, haystackMax, haystackChild) {
            if (jsonFuzzyEquals(haystackChild, needleChild)) {
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }
    return true;
}

static bool JSONObjectFuzzyEquals(yyjson_val* haystack, yyjson_val* needle) {
    KU_ASSERT(yyjson_get_tag(haystack) == (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE) &&
              yyjson_get_tag(needle) == (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE));

    size_t idx = 0, max = 0;
    yyjson_val *key = nullptr, *needleChild = nullptr;
    yyjson_obj_foreach(needle, idx, max, key, needleChild) {
        auto haystackChild =
            yyjson_obj_getn(haystack, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
        if (!haystackChild || !jsonFuzzyEquals(haystackChild, needleChild)) {
            return false;
        }
    }
    return true;
}

static bool jsonFuzzyEquals(yyjson_val* haystack, yyjson_val* needle) {
    KU_ASSERT(haystack != nullptr);
    KU_ASSERT(needle != nullptr);

    // Strict equality
    if (unsafe_yyjson_equals(haystack, needle)) {
        return true;
    }

    auto haystackTag = yyjson_get_tag(needle);
    if (haystackTag != yyjson_get_tag(haystack)) {
        return false;
    }

    // Fuzzy equality (contained in)
    switch (haystackTag) {
    case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
        return jsonArrayFuzzyEquals(haystack, needle);
    case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
        return JSONObjectFuzzyEquals(haystack, needle);
    default:
        return false;
    }
}

static bool jsonContains(yyjson_val* haystack, yyjson_val* needle);

static bool JSONArrayContains(yyjson_val* haystackArray, yyjson_val* needle) {
    KU_ASSERT(yyjson_get_tag(haystackArray) == (YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE));

    size_t idx = 0, max = 0;
    yyjson_val* childHaystack = nullptr;
    yyjson_arr_foreach(haystackArray, idx, max, childHaystack) {
        if (jsonContains(childHaystack, needle)) {
            return true;
        }
    }
    return false;
}

static bool jsonObjectContains(yyjson_val* haystackObject, yyjson_val* needle) {
    KU_ASSERT(yyjson_get_tag(haystackObject) == (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE));

    size_t idx = 0, max = 0;
    yyjson_val *key = nullptr, *childHaystack = nullptr;
    yyjson_obj_foreach(haystackObject, idx, max, key, childHaystack) {
        if (jsonContains(childHaystack, needle)) {
            return true;
        }
    }
    return false;
}

static bool jsonContains(yyjson_val* haystack, yyjson_val* needle) {
    if (jsonFuzzyEquals(haystack, needle)) {
        return true;
    }

    switch (yyjson_get_tag(haystack)) {
    case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
        return JSONArrayContains(haystack, needle);
    case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
        return jsonObjectContains(haystack, needle);
    default:
        return false;
    }
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    const auto& param1 = parameters[0];
    const auto& param2 = parameters[1];
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
         ++selectedPos) {
        auto resultPos = result.state->getSelVector()[selectedPos];
        auto param1Pos = param1->state->getSelVector()[param1->state->isFlat() ? 0 : selectedPos];
        auto param2Pos = param2->state->getSelVector()[param2->state->isFlat() ? 0 : selectedPos];
        auto isNull = param1->isNull(param1Pos) || param2->isNull(param2Pos);
        result.setNull(resultPos, isNull);
        if (!isNull) {
            auto haystackStr = param1->getValue<ku_string_t>(param1Pos).getAsString();
            auto needleStr = param2->getValue<ku_string_t>(param2Pos).getAsString();
            auto haystackDoc =
                JsonWrapper{JSONCommon::readDocument(haystackStr, JSONCommon::READ_FLAG)};
            auto needleDoc =
                JsonWrapper{JSONCommon::readDocument(needleStr, JSONCommon::READ_FLAG)};
            result.setValue<bool>(resultPos,
                jsonContains(haystackDoc.ptr->root, needleDoc.ptr->root));
        }
    }
}

function_set JsonContainsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL, execFunc));
    return result;
}

} // namespace json_extension
} // namespace kuzu
