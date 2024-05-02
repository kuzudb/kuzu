#include "pandas/pandas_analyzer.h"

#include "cached_import/py_cached_import.h"
#include "function/built_in_function_utils.h"
#include "py_conversion.h"

namespace kuzu {

static bool upgradeType(common::LogicalType& left, const common::LogicalType& right) {
    if (right.getLogicalTypeID() == common::LogicalTypeID::ANY) {
        return true;
    }
    if (left.getLogicalTypeID() == common::LogicalTypeID::ANY ||
        ((left.getLogicalTypeID() == common::LogicalTypeID::LIST) &&
            (common::ListType::getChildType(left).getLogicalTypeID() ==
                common::LogicalTypeID::ANY))) {
        left = right;
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
        left = right;
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
            listType = itemType;
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

common::LogicalType PandasAnalyzer::getItemType(py::object ele, bool& canConvert) {
    auto objectType = getPythonObjectType(ele);
    switch (objectType) {
    case PythonObjectType::None:
        return *common::LogicalType::ANY();
    case PythonObjectType::Bool:
        return *common::LogicalType::BOOL();
    case PythonObjectType::Integer:
        return *common::LogicalType::INT64();
    case PythonObjectType::Float:
        return *common::LogicalType::DOUBLE();
    case PythonObjectType::Datetime:
        return *common::LogicalType::TIMESTAMP();
    case PythonObjectType::Date:
        return *common::LogicalType::DATE();
    case PythonObjectType::String:
        return *common::LogicalType::STRING();
    case PythonObjectType::List:
        return *common::LogicalType::LIST(getListType(ele, canConvert));
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
    for (auto i = 1u; i < numRows; i += 1) {
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
        analyzedType = type;
    }
    return canConvert;
}

} // namespace kuzu
