#include "parser/comment_on.h"
#include "parser/transformer.h"

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformCommentOn(CypherParser::KU_CommentOnContext& ctx) {
    auto table = transformSchemaName(*ctx.oC_SchemaName());
    auto comment = transformStringLiteral(*ctx.StringLiteral());
    return std::make_unique<CommentOn>(std::move(table), std::move(comment));
}

} // namespace parser
} // namespace kuzu
