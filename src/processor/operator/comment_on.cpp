#include "processor/operator/comment_on.h"

#include "catalog/catalog.h"
#include "common/string_format.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool CommentOn::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    if (commentOnInfo->hasExecuted) {
        return false;
    }
    context->clientContext->progressBar->addJobsToPipeline(1);
    commentOnInfo->catalog->setTableComment(commentOnInfo->tableID, commentOnInfo->comment);
    commentOnInfo->hasExecuted = true;
    outputVector->setValue<std::string>(outputVector->state->selVector->selectedPositions[0],
        stringFormat("Table {} comment updated.", commentOnInfo->tableName));
    metrics->numOutputTuple.increase(1);
    context->clientContext->progressBar->finishJobsInPipeline(1);
    return true;
}

} // namespace processor
} // namespace kuzu
