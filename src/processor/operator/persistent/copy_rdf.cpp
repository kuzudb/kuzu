#include "processor/operator/persistent/copy_rdf.h"

#include "common/string_format.h"
#include "processor/result/factorized_table_util.h"
namespace kuzu {
namespace processor {

std::string CopyRdfPrintInfo::toString() const {
    std::string result = "Table Name: ";
    result += tableName;
    return result;
}

void CopyRdf::finalize(ExecutionContext* context) {
    auto outputMsg = common::stringFormat("Done copy rdf.");
    FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), outputMsg,
        context->clientContext->getMemoryManager());
}

} // namespace processor
} // namespace kuzu
