#pragma once

#include <mutex>

#include "common/data_chunk/data_chunk.h"
#include "function/function.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace binder {
class BoundReadingClause;
}
namespace parser {
struct YieldVariable;
class ParsedExpression;
} // namespace parser

namespace planner {
class LogicalOperator;
class LogicalPlan;
class Planner;
} // namespace planner

namespace processor {
struct ExecutionContext;
class PlanMapper;
} // namespace processor

namespace transaction {
class Transaction;
} // namespace transaction

namespace function {

struct TableFuncBindInput;
struct TableFuncBindData;

struct TableFuncMorsel {
    common::offset_t startOffset;
    common::offset_t endOffset;

    TableFuncMorsel(common::offset_t startOffset, common::offset_t endOffset)
        : startOffset{startOffset}, endOffset{endOffset} {}

    bool hasMoreToOutput() const { return startOffset != common::INVALID_OFFSET; }

    static TableFuncMorsel createInvalidMorsel() {
        return {common::INVALID_OFFSET, common::INVALID_OFFSET};
    }

    bool isInvalid() const {
        return startOffset == common::INVALID_OFFSET && endOffset == common::INVALID_OFFSET;
    }
};

struct KUZU_API TableFuncSharedState {
    common::offset_t maxOffset;
    common::offset_t curOffset;
    common::offset_t maxMorselSize;
    std::mutex mtx;

    explicit TableFuncSharedState();
    explicit TableFuncSharedState(common::offset_t maxOffset);
    TableFuncSharedState(common::offset_t maxOffset, common::offset_t maxMorselSize);
    virtual ~TableFuncSharedState() = default;

    virtual TableFuncMorsel getMorsel();

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

struct TableFuncLocalState {
    virtual ~TableFuncLocalState() = default;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

struct TableFuncInput {
    TableFuncBindData* bindData;
    TableFuncLocalState* localState;
    TableFuncSharedState* sharedState;
    processor::ExecutionContext* context;

    TableFuncInput() = default;
    TableFuncInput(TableFuncBindData* bindData, TableFuncLocalState* localState,
        TableFuncSharedState* sharedState, processor::ExecutionContext* context)
        : bindData{bindData}, localState{localState}, sharedState{sharedState}, context{context} {}
    DELETE_COPY_DEFAULT_MOVE(TableFuncInput);
};

// We are in the middle of merging different scan operators into table function. But they organize
// output vectors in different ways. E.g.
// - Call functions and scan file functions put all vectors into single data chunk
// - Factorized table scan instead
// We introduce this as a temporary solution to unify the interface. In the long term, we should aim
// to use ResultSet as TableFuncOutput.
struct TableFuncOutput {
    common::DataChunk dataChunk;
    std::vector<common::ValueVector*> vectors;

    TableFuncOutput() = default;
    DELETE_COPY_DEFAULT_MOVE(TableFuncOutput);
};

struct TableFunctionInitInput final {
    TableFuncBindData* bindData;
    uint64_t queryID;
    const main::ClientContext& context;

    explicit TableFunctionInitInput(TableFuncBindData* bindData, uint64_t queryID,
        const main::ClientContext& context)
        : bindData{bindData}, queryID{queryID}, context{context} {}
};

using table_func_bind_t = std::function<std::unique_ptr<TableFuncBindData>(main::ClientContext*,
    const TableFuncBindInput*)>;
using table_func_t = std::function<common::offset_t(const TableFuncInput&, TableFuncOutput&)>;
using table_func_init_shared_t =
    std::function<std::unique_ptr<TableFuncSharedState>(const TableFunctionInitInput&)>;
using table_func_init_local_t = std::function<std::unique_ptr<TableFuncLocalState>(
    const TableFunctionInitInput&, TableFuncSharedState*, storage::MemoryManager*)>;
using table_func_can_parallel_t = std::function<bool()>;
using table_func_progress_t = std::function<double(TableFuncSharedState* sharedState)>;
using table_func_finalize_t =
    std::function<void(const processor::ExecutionContext*, TableFuncSharedState*)>;
using table_func_rewrite_t =
    std::function<std::string(main::ClientContext&, const TableFuncBindData& bindData)>;
using table_func_get_logical_plan_t = std::function<void(const transaction::Transaction*,
    planner::Planner*, const binder::BoundReadingClause&, std::shared_ptr<planner::LogicalOperator>,
    const std::vector<std::unique_ptr<planner::LogicalPlan>>&)>;
using table_func_get_physical_plan_t = std::function<std::unique_ptr<processor::PhysicalOperator>(
    const main::ClientContext*, processor::PlanMapper*, const planner::LogicalOperator*)>;

struct KUZU_API TableFunction final : Function {
    table_func_t tableFunc = nullptr;
    table_func_bind_t bindFunc = nullptr;
    table_func_init_shared_t initSharedStateFunc = nullptr;
    table_func_init_local_t initLocalStateFunc = nullptr;
    table_func_can_parallel_t canParallelFunc = [] { return true; };
    table_func_progress_t progressFunc = [](TableFuncSharedState*) { return 0.0; };
    table_func_finalize_t finalizeFunc = [](auto, auto) {};
    table_func_rewrite_t rewriteFunc = nullptr;
    table_func_get_logical_plan_t getLogicalPlanFunc = getLogicalPlan;
    table_func_get_physical_plan_t getPhysicalPlanFunc = getPhysicalPlan;

    TableFunction() {}
    TableFunction(std::string name, std::vector<common::LogicalTypeID> inputTypes)
        : Function{std::move(name), std::move(inputTypes)} {}
    ~TableFunction() override;
    TableFunction(const TableFunction&) = default;
    TableFunction& operator=(const TableFunction& other) = default;
    DEFAULT_BOTH_MOVE(TableFunction);

    std::string signatureToString() const override {
        return common::LogicalTypeUtils::toString(parameterTypeIDs);
    }

    std::unique_ptr<TableFunction> copy() const { return std::make_unique<TableFunction>(*this); }

    static std::unique_ptr<TableFuncSharedState> initSharedState(
        const TableFunctionInitInput& input);
    static std::unique_ptr<TableFuncLocalState> initEmptyLocalState(
        const TableFunctionInitInput& input, TableFuncSharedState* state,
        storage::MemoryManager* mm);
    static std::vector<std::string> extractYieldVariables(const std::vector<std::string>& names,
        const std::vector<parser::YieldVariable>& yieldVariables);
    static void getLogicalPlan(const transaction::Transaction* transaction,
        planner::Planner* planner, const binder::BoundReadingClause& readingClause,
        std::shared_ptr<planner::LogicalOperator> logicalOp,
        const std::vector<std::unique_ptr<planner::LogicalPlan>>& logicalPlans);
    static std::unique_ptr<processor::PhysicalOperator> getPhysicalPlan(
        const main::ClientContext* clientContext, processor::PlanMapper* planMapper,
        const planner::LogicalOperator* logicalOp);
    static common::offset_t emptyTableFunc(const TableFuncInput& input, TableFuncOutput& output);
};

struct CurrentSettingFunction final {
    static constexpr const char* name = "CURRENT_SETTING";

    static function_set getFunctionSet();
};

struct CatalogVersionFunction final {
    static constexpr const char* name = "CATALOG_VERSION";

    static function_set getFunctionSet();
};

struct DBVersionFunction final {
    static constexpr const char* name = "DB_VERSION";

    static function_set getFunctionSet();
};

struct ShowTablesFunction final {
    static constexpr const char* name = "SHOW_TABLES";

    static function_set getFunctionSet();
};

struct ShowWarningsFunction final {
    static constexpr const char* name = "SHOW_WARNINGS";

    static function_set getFunctionSet();
};

struct ClearWarningsFunction final {
    static constexpr const char* name = "CLEAR_WARNINGS";

    static function_set getFunctionSet();
};

struct TableInfoFunction final {
    static constexpr const char* name = "TABLE_INFO";

    static function_set getFunctionSet();
};

struct ShowSequencesFunction final {
    static constexpr const char* name = "SHOW_SEQUENCES";

    static function_set getFunctionSet();
};

struct ShowConnectionFunction final {
    static constexpr const char* name = "SHOW_CONNECTION";

    static function_set getFunctionSet();
};

struct StorageInfoFunction final {
    static constexpr const char* name = "STORAGE_INFO";

    static function_set getFunctionSet();
};

struct StatsInfoFunction final {
    static constexpr const char* name = "STATS_INFO";

    static function_set getFunctionSet();
};

struct BMInfoFunction final {
    static constexpr const char* name = "BM_INFO";

    static function_set getFunctionSet();
};

struct ShowAttachedDatabasesFunction final {
    static constexpr const char* name = "SHOW_ATTACHED_DATABASES";

    static function_set getFunctionSet();
};

struct ShowFunctionsFunction final {
    static constexpr const char* name = "SHOW_FUNCTIONS";

    static function_set getFunctionSet();
};

struct CreateProjectedGraphFunction final {
    static constexpr const char* name = "CREATE_PROJECTED_GRAPH";

    static function_set getFunctionSet();
};

struct DropProjectedGraphFunction final {
    static constexpr const char* name = "DROP_PROJECTED_GRAPH";

    static function_set getFunctionSet();
};

struct ShowLoadedExtensionsFunction final {
    static constexpr const char* name = "SHOW_LOADED_EXTENSIONS";

    static function_set getFunctionSet();
};

struct ShowOfficialExtensionsFunction final {
    static constexpr const char* name = "SHOW_OFFICIAL_EXTENSIONS";

    static function_set getFunctionSet();
};

struct ShowIndexesFunction final {
    static constexpr const char* name = "SHOW_INDEXES";

    static function_set getFunctionSet();
};

// Cache a table column to the transaction local cache.
// Note this is only used for internal purpose, and only supports node tables for now.
struct LocalCacheArrayColumnFunction final {
    static constexpr const char* name = "_CACHE_ARRAY_COLUMN_LOCALLY";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
