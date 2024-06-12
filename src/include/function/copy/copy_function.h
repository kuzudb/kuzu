#pragma once

#include "common/types/value/value.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct CopyFuncLocalState {
    virtual ~CopyFuncLocalState() = default;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<CopyFuncLocalState&, TARGET&>(*this);
    }
};

struct CopyFuncSharedState {
    virtual ~CopyFuncSharedState() = default;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<CopyFuncSharedState&, TARGET&>(*this);
    }
};

struct CopyFuncBindData {
    std::vector<std::string> names;
    std::vector<common::LogicalType> types;
    std::string fileName;
    bool canParallel;

    CopyFuncBindData(std::vector<std::string> names, std::string fileName, bool canParallel)
        : names{std::move(names)}, fileName{std::move(fileName)}, canParallel{canParallel} {}

    virtual ~CopyFuncBindData() = default;

    void bindDataType(std::vector<common::LogicalType> types_) { types = std::move(types_); }

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const CopyFuncBindData&, const TARGET&>(*this);
    }

    virtual std::unique_ptr<CopyFuncBindData> copy() const = 0;
};

struct CopyFuncBindInput {
    std::vector<std::string> columnNames;
    std::string filePath;
    std::unordered_map<std::string, common::Value> parsingOptions;
    bool canParallel;
};

using copy_to_bind_t =
    std::function<std::unique_ptr<CopyFuncBindData>(function::CopyFuncBindInput& bindInput)>;
using copy_to_initialize_local_t = std::function<std::unique_ptr<CopyFuncLocalState>(
    main::ClientContext&, const CopyFuncBindData&, std::vector<bool>)>;
using copy_to_initialize_shared_t =
    std::function<std::shared_ptr<CopyFuncSharedState>(main::ClientContext&, CopyFuncBindData&)>;
using copy_to_sink_t = std::function<void(CopyFuncSharedState&, CopyFuncLocalState&,
    const CopyFuncBindData&, std::vector<std::shared_ptr<common::ValueVector>>)>;
using copy_to_combine_t = std::function<void(CopyFuncSharedState&, CopyFuncLocalState&)>;
using copy_to_finalize_t = std::function<void(CopyFuncSharedState&)>;
using copy_to_can_parallel_t = std::function<bool(void)>;

struct CopyFunction : public Function {
    explicit CopyFunction(std::string name) : Function{std::move(name), {}} {}

    CopyFunction(std::string name, copy_to_initialize_local_t copyToInitLocal,
        copy_to_initialize_shared_t copyToInitGlobal, copy_to_sink_t copyToSink,
        copy_to_combine_t copyToCombine, copy_to_finalize_t copyToFinalize)
        : Function{std::move(name), {}}, copyToInitLocal{std::move(copyToInitLocal)},
          copyToInitShared{std::move(copyToInitGlobal)}, copyToSink{std::move(copyToSink)},
          copyToCombine{std::move(copyToCombine)}, copyToFinalize{std::move(copyToFinalize)} {}

    std::unique_ptr<Function> copy() const override {
        return std::make_unique<CopyFunction>(name, copyToInitLocal, copyToInitShared, copyToSink,
            copyToCombine, copyToFinalize);
    }

    copy_to_bind_t copyToBind;
    copy_to_initialize_local_t copyToInitLocal;
    copy_to_initialize_shared_t copyToInitShared;
    copy_to_sink_t copyToSink;
    copy_to_combine_t copyToCombine;
    copy_to_finalize_t copyToFinalize;
    copy_to_can_parallel_t copyToCanParallel;
};

struct CopyCSVFunction : public CopyFunction {
    static constexpr const char* name = "COPY_CSV";

    static function_set getFunctionSet();
};

struct CopyParquetFunction : public CopyFunction {
    static constexpr const char* name = "COPY_PARQUET";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
