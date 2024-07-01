#include "function/sequence/sequence_functions.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/sequence_catalog_entry.h"
#include "function/scalar_function.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct CurrVal {
    static void operation(common::ku_string_t& input, common::ValueVector& result, void* dataPtr) {
        auto ctx = reinterpret_cast<FunctionBindData*>(dataPtr)->clientContext;
        auto catalog = ctx->getCatalog();
        auto sequenceName = input.getAsString();
        auto sequenceID = catalog->getSequenceID(ctx->getTx(), sequenceName);
        auto sequenceEntry = catalog->getSequenceCatalogEntry(ctx->getTx(), sequenceID);
        result.setValue(0, sequenceEntry->currVal());
    }
};

struct NextVal {
    static void operation(common::ku_string_t& input, common::ValueVector& result, void* dataPtr) {
        auto ctx = reinterpret_cast<FunctionBindData*>(dataPtr)->clientContext;
        auto cnt = reinterpret_cast<FunctionBindData*>(dataPtr)->count;
        auto catalog = ctx->getCatalog();
        auto sequenceName = input.getAsString();
        auto sequenceID = catalog->getSequenceID(ctx->getTx(), sequenceName);
        auto sequenceEntry = catalog->getSequenceCatalogEntry(ctx->getTx(), sequenceID);
        sequenceEntry->nextKVal(ctx->getTx(), cnt, result);
        result.state->getSelVectorUnsafe().setSelSize(cnt);
    }
};

function_set CurrValFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::INT64,
        ScalarFunction::UnarySequenceExecFunction<common::ku_string_t, common::ValueVector,
            CurrVal>));
    return functionSet;
}

function_set NextValFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::INT64,
        ScalarFunction::UnarySequenceExecFunction<common::ku_string_t, common::ValueVector,
            NextVal>));
    return functionSet;
}

} // namespace function
} // namespace kuzu
