#pragma once

#include "common/types/value/value.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct ExportFuncLocalState {
    virtual ~ExportFuncLocalState() = default;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<ExportFuncLocalState&, TARGET&>(*this);
    }
};

struct ExportFuncSharedState {
    virtual ~ExportFuncSharedState() = default;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<ExportFuncSharedState&, TARGET&>(*this);
    }
};

struct ExportFuncBindData {
    std::vector<std::string> columnNames;
    std::vector<common::LogicalType> types;
    std::string fileName;

    ExportFuncBindData(std::vector<std::string> columnNames, std::string fileName)
        : columnNames{std::move(columnNames)}, fileName{std::move(fileName)} {}

    virtual ~ExportFuncBindData() = default;

    void setDataType(std::vector<common::LogicalType> types_) { types = std::move(types_); }

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const ExportFuncBindData&, const TARGET&>(*this);
    }

    virtual std::unique_ptr<ExportFuncBindData> copy() const = 0;
};

struct ExportFuncBindInput {
    std::vector<std::string> columnNames;
    std::string filePath;
    std::unordered_map<std::string, common::Value> parsingOptions;
};

using bind_t =
    std::function<std::unique_ptr<ExportFuncBindData>(function::ExportFuncBindInput& bindInput)>;
using init_local_t = std::function<std::unique_ptr<ExportFuncLocalState>(main::ClientContext&,
    const ExportFuncBindData&, std::vector<bool>)>;
using init_shared_t = std::function<std::shared_ptr<ExportFuncSharedState>(main::ClientContext&,
    ExportFuncBindData&)>;
using sink_t = std::function<void(ExportFuncSharedState&, ExportFuncLocalState&,
    const ExportFuncBindData&, std::vector<std::shared_ptr<common::ValueVector>>)>;
using combine_t = std::function<void(ExportFuncSharedState&, ExportFuncLocalState&)>;
using finalize_t = std::function<void(ExportFuncSharedState&)>;

struct ExportFunction : public Function {
    explicit ExportFunction(std::string name) : Function{std::move(name), {}} {}

    ExportFunction(std::string name, init_local_t copyToInitLocal, init_shared_t copyToInitGlobal,
        sink_t copyToSink, combine_t copyToCombine, finalize_t copyToFinalize)
        : Function{std::move(name), {}}, initLocal{std::move(copyToInitLocal)},
          initShared{std::move(copyToInitGlobal)}, sink{std::move(copyToSink)},
          combine{std::move(copyToCombine)}, finalize{std::move(copyToFinalize)} {}

    std::unique_ptr<Function> copy() const override {
        return std::make_unique<ExportFunction>(name, initLocal, initShared, sink, combine,
            finalize);
    }

    bind_t bind;
    init_local_t initLocal;
    init_shared_t initShared;
    sink_t sink;
    combine_t combine;
    finalize_t finalize;
};

struct ExportCSVFunction : public ExportFunction {
    static constexpr const char* name = "COPY_CSV";

    static function_set getFunctionSet();
};

struct ExportParquetFunction : public ExportFunction {
    static constexpr const char* name = "COPY_PARQUET";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
