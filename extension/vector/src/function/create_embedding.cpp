#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/hnsw_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/mask.h"
#include "common/types/ku_list.h"
#include "common/types/types.h"
#include "common/types/value/nested.h"
#include "expression_evaluator/expression_evaluator_utils.h"
#include "function/function.h"
#include "function/gds/gds.h"
#include "function/hnsw_index_functions.h"
#include "function/scalar_function.h"
#include "function/table/bind_data.h"
#include "index/hnsw_index.h"
#include "index/hnsw_index_utils.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/operator/sip/logical_semi_masker.h"
#include "planner/planner.h"
#include "processor/execution_context.h"
#include "processor/operator/table_function_call.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;
using namespace kuzu::planner;
using namespace kuzu::catalog;
using namespace kuzu::processor;

namespace kuzu {
namespace vector_extension {



static void execFunc( 
    const std::vector<std::shared_ptr<common::ValueVector>>& /*parameters*/, 
    const std::vector<common::SelectionVector*>& /*parameterSelVectors*/, 
    common::ValueVector& result, 
    common::SelectionVector* resultSelVector, 
    void* /*dataPtr*/)
{
    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {
        auto pos = (*resultSelVector)[selectedPos];
        auto resultEntry = ListVector::addList(&result, 10);
        result.setValue(pos, resultEntry);
        auto resultDataVector = ListVector::getDataVector(&result);
        auto resultPos = resultEntry.offset;
        for (int i = 0; i < 10; i++) {
            resultDataVector->copyFromValue(resultPos++, Value((float)i));
        }
    }
}


static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::vector<LogicalType> types;
    types.push_back(input.arguments[0]->getDataType().copy());
    assert(types.front() == LogicalType::STRING());
    return std::make_unique<FunctionBindData>(std::move(types), LogicalType::LIST(LogicalType(LogicalTypeID::FLOAT)));
}

function_set CreateEmbedding::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::LIST, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace vector_extension
} // namespace kuzu
