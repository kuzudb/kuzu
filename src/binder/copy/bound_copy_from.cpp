#include "binder/copy/bound_copy_from.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundCopyFromInfo> BoundCopyFromInfo::copy() {
    expression_vector copiedColumnExpressions;
    copiedColumnExpressions.reserve(columnExpressions.size());
    for (const auto& columnExpression : columnExpressions) {
        copiedColumnExpressions.push_back(columnExpression->copy());
    }
    return std::make_unique<BoundCopyFromInfo>(copyDesc->copy(), tableSchema,
        std::move(copiedColumnExpressions), offsetExpression->copy(),
        tableSchema->tableType == common::TableType::REL ? boundOffsetExpression->copy() : nullptr,
        tableSchema->tableType == common::TableType::REL ? nbrOffsetExpression->copy() : nullptr,
        containsSerial);
}

} // namespace binder
} // namespace kuzu
