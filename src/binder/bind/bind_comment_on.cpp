#include "binder/binder.h"
#include "binder/bound_comment_on.h"
#include "parser/comment_on.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCommentOn(const parser::Statement& statement) {
    auto& commentOnStatement = reinterpret_cast<const parser::CommentOn&>(statement);
    auto tableName = commentOnStatement.getTable();
    auto comment = commentOnStatement.getComment();

    validateTableExist(tableName);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableID = catalogContent->getTableID(tableName);

    return std::make_unique<BoundCommentOn>(tableID, tableName, comment);
}

} // namespace binder
} // namespace kuzu
