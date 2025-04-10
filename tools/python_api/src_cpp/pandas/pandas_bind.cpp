#include "pandas/pandas_bind.h"

#include "common/exception/runtime.h"
#include "pandas/pandas_analyzer.h"

namespace kuzu {

using namespace kuzu::common;

struct PandasBindColumn {
public:
    PandasBindColumn(py::handle name, py::handle type, py::object column)
        : name(name), type(type), handle(std::move(column)) {}

public:
    py::handle name;
    py::handle type;
    py::object handle;
};

struct PandasDataFrameBind {
public:
    explicit PandasDataFrameBind(py::handle& df) {
        names = py::list(df.attr("columns"));
        types = py::list(df.attr("dtypes"));
        getter = df.attr("__getitem__");
    }
    PandasBindColumn operator[](uint64_t index) const {
        KU_ASSERT(index < names.size());
        auto column = py::reinterpret_borrow<py::object>(getter(names[index]));
        auto type = types[index];
        auto name = names[index];
        return PandasBindColumn(name, type, column);
    }

public:
    py::list names;
    py::list types;

private:
    py::object getter;
};

static common::LogicalType bindColumn(PandasBindColumn& bindColumn,
    PandasColumnBindData* bindData) {
    common::LogicalType columnType;
    auto& column = bindColumn.handle;

    bindData->npType = NumpyTypeUtils::convertNumpyType(bindColumn.type);
    bool hasMaskColumn = py::hasattr(column.attr("array"), "_mask");

    if (hasMaskColumn) {
        bindData->mask = std::make_unique<RegisteredArray>(column.attr("array").attr("_mask"));
    }

    if (bindData->npType.type == NumpyNullableType::FLOAT_16) {
        auto pandasArray = column.attr("array");
        bindData->pandasCol =
            std::make_unique<PandasNumpyColumn>(py::array(column.attr("to_numpy")("float32")));
        bindData->npType.type = NumpyNullableType::FLOAT_32;
        columnType = NumpyTypeUtils::numpyToLogicalType(bindData->npType);
    } else {
        auto pandasArray = column.attr("array");
        if (py::hasattr(pandasArray, "_data")) {
            // This means we can access the numpy array directly.
            bindData->pandasCol =
                std::make_unique<PandasNumpyColumn>(column.attr("array").attr("_data"));
        } else if (py::hasattr(pandasArray, "asi8")) {
            // This is a datetime object, has the option to get the array as int64_t's.
            bindData->pandasCol =
                std::make_unique<PandasNumpyColumn>(py::array(pandasArray.attr("asi8")));
        } else {
            // Otherwise we have to get it through 'to_numpy()'.
            bindData->pandasCol =
                std::make_unique<PandasNumpyColumn>(py::array(column.attr("to_numpy")()));
        }
        columnType = NumpyTypeUtils::numpyToLogicalType(bindData->npType);
    }
    if (bindData->npType.type == NumpyNullableType::OBJECT) {
        PandasAnalyzer analyzer;
        if (analyzer.analyze(column)) {
            columnType = analyzer.getAnalyzedType().copy();
        }
    }
    return LogicalTypeUtils::purgeAny(columnType, LogicalType::STRING());
}

void Pandas::bind(py::handle dfToBind,
    std::vector<std::unique_ptr<PandasColumnBindData>>& columnBindData,
    std::vector<common::LogicalType>& returnTypes, std::vector<std::string>& names) {

    PandasDataFrameBind df(dfToBind);
    auto numColumns = py::len(df.names);
    // LCOV_EXCL_START
    if (numColumns == 0 || py::len(df.types) == 0 || numColumns != py::len(df.types)) {
        throw common::RuntimeException{"Need a DataFrame with at least one column."};
    }
    // LCOV_EXCL_STOP

    returnTypes.reserve(numColumns);
    names.reserve(numColumns);
    for (auto i = 0u; i < numColumns; i++) {
        std::unique_ptr<PandasColumnBindData> bindData = std::make_unique<PandasColumnBindData>();

        names.emplace_back(py::str(df.names[i]));
        auto column = df[i];
        auto columnType = bindColumn(column, bindData.get());

        returnTypes.push_back(std::move(columnType));
        columnBindData.push_back(std::move(bindData));
    }
}

} // namespace kuzu
