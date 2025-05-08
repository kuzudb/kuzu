#include "pandas/pandas_analyzer.h"

#include "cached_import/py_cached_import.h"
#include "function/built_in_function_utils.h"
#include "py_conversion.h"

namespace kuzu {

// NOTE: MOVES right to left
static bool upgradeType(common::LogicalType& left, common::LogicalType& right) {
    if (right.getLogicalTypeID() == common::LogicalTypeID::ANY) {
        return true;
    }
    if (left.getLogicalTypeID() == common::LogicalTypeID::ANY ||
        ((left.getLogicalTypeID() == common::LogicalTypeID::LIST) &&
            (common::ListType::getChildType(left).getLogicalTypeID() ==
                common::LogicalTypeID::ANY))) {
        left = std::move(right);
        return true;
    }
    if (((right.getLogicalTypeID() == common::LogicalTypeID::LIST) &&
            (common::ListType::getChildType(right).getLogicalTypeID() ==
                common::LogicalTypeID::ANY))) {
        return true;
    }
    auto leftToRightCost = function::BuiltInFunctionsUtils::getCastCost(left.getLogicalTypeID(),
        right.getLogicalTypeID());
    if (leftToRightCost != common::UNDEFINED_CAST_COST) {
        left = std::move(right);
    } else {
        return false;
    }
    return true;
}

common::LogicalType PandasAnalyzer::getListType(py::object& ele, bool& canConvert) {
    uint64_t i = 0;
    common::LogicalType listType;
    for (auto pyVal : ele) {
        auto object = py::reinterpret_borrow<py::object>(pyVal);
        auto itemType = getItemType(object, canConvert);
        if (i == 0) {
            listType = std::move(itemType);
        } else {
            if (!upgradeType(listType, itemType)) {
                canConvert = false;
            }
        }
        if (!canConvert) {
            break;
        }
        i++;
    }
    return listType;
}

static bool isValidMapComponent(const py::handle& component) {
    if (py::none().is(component)) {
        return true;
    }
    if (!py::hasattr(component, "__getitem__")) {
        return false;
    }
    if (!py::hasattr(component, "__len__")) {
        return false;
    }
    return true;
}

bool dictionaryHasMapFormat(const PyDictionary& dict) {
    if (dict.len != 2) {
        return false;
    }

    // { 'key': [ .. keys .. ], 'value': [ .. values .. ]}
    auto keysKey = py::str("key");
    auto valuesKey = py::str("value");
    auto keys = dict[keysKey];
    auto values = dict[valuesKey];
    if (!keys || !values) {
        return false;
    }
    if (!isValidMapComponent(keys)) {
        return false;
    }
    if (!isValidMapComponent(values)) {
        return false;
    }
    if (py::none().is(keys) || py::none().is(values)) {
        return true;
    }
    auto size = py::len(keys);
    if (size != py::len(values)) {
        return false;
    }
    return true;
}

common::LogicalType PandasAnalyzer::dictToMap(const PyDictionary& dict, bool& canConvert) {
    auto keys = dict.values.attr("__getitem__")(0);
    auto values = dict.values.attr("__getitem__")(1);

    if (py::none().is(keys) || py::none().is(values)) {
        return common::LogicalType::MAP(common::LogicalType::ANY(), common::LogicalType::ANY());
    }

    auto keyType = PandasAnalyzer::getListType(keys, canConvert);
    if (!canConvert) {
        return common::LogicalType::MAP(common::LogicalType::ANY(), common::LogicalType::ANY());
    }
    auto valueType = getListType(values, canConvert);
    if (!canConvert) {
        return common::LogicalType::MAP(common::LogicalType::ANY(), common::LogicalType::ANY());
    }

    return common::LogicalType::MAP(std::move(keyType), std::move(valueType));
}

common::LogicalType PandasAnalyzer::dictToStruct(const PyDictionary& dict, bool& canConvert) {
    std::vector<common::StructField> fields;

    for (auto i = 0u; i < dict.len; i++) {
        auto dictKey = dict.keys.attr("__getitem__")(i);
        auto key = std::string(py::str(dictKey));
        auto dictVal = dict.values.attr("__getitem__")(i);
        auto val = getItemType(dictVal, canConvert);
        fields.emplace_back(std::move(key), std::move(val));
    }
    return common::LogicalType::STRUCT(std::move(fields));
}

common::LogicalType PandasAnalyzer::getItemType(py::object ele, bool& canConvert) {
    auto objectType = getPythonObjectType(ele);
    switch (objectType) {
    case PythonObjectType::None:
        return common::LogicalType::ANY();
    case PythonObjectType::Bool:
        return common::LogicalType::BOOL();
    case PythonObjectType::Integer:
        return common::LogicalType::INT64();
    case PythonObjectType::Float:
        return common::LogicalType::DOUBLE();
    case PythonObjectType::Datetime:
        return common::LogicalType::TIMESTAMP();
    case PythonObjectType::Date:
        return common::LogicalType::DATE();
    case PythonObjectType::String:
        return common::LogicalType::STRING();
    case PythonObjectType::List:
        return common::LogicalType::LIST(getListType(ele, canConvert));
    case PythonObjectType::UUID:
        return common::LogicalType::UUID();
    case PythonObjectType::Dict: {
        PyDictionary dict = PyDictionary(py::reinterpret_borrow<py::object>(ele));
        if (dict.len == 0) {
            return common::LogicalType::MAP(common::LogicalType::ANY(), common::LogicalType::ANY());
        }
        if (dictionaryHasMapFormat(dict)) {
            return dictToMap(dict, canConvert);
        }
        return dictToStruct(dict, canConvert);
    }
    default:
        KU_UNREACHABLE;
    }
}

static py::object findFirstNonNull(const py::handle& row, uint64_t numRows) {
    for (auto i = 0u; i < numRows; i++) {
        auto obj = row(i);
        if (!obj.is_none()) {
            return obj;
        }
    }
    return py::none();
}

common::LogicalType PandasAnalyzer::innerAnalyze(py::object column, bool& canConvert) {
    auto numRows = py::len(column);
    auto pandasModule = importCache->pandas;
    auto pandasSeries = pandasModule.core.series.Series();

    if (py::isinstance(column, pandasSeries)) {
        column = column.attr("__array__")();
    }
    auto row = column.attr("__getitem__");
    common::LogicalType itemType = getItemType(findFirstNonNull(row, numRows), canConvert);
    auto sampleSize = numRows > 1000u ? 1000u : numRows;
    for (auto i = 1u; i < sampleSize; i += 1) {
        auto obj = row(i);
        auto curItemType = getItemType(obj, canConvert);
        if (!canConvert || !upgradeType(itemType, curItemType)) {
            canConvert = false;
            return curItemType;
        }
    }
    if (itemType.getPhysicalType() == common::PhysicalTypeID::ANY) {
        canConvert = false;
    }
    return itemType;
}

bool PandasAnalyzer::analyze(py::object column) {
    bool canConvert = true;
    auto type = innerAnalyze(std::move(column), canConvert);
    if (canConvert) {
        analyzedType = std::move(type);
    }
    return canConvert;
}

} // namespace kuzu
