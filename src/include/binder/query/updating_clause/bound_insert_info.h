#pragma once

#include "binder/expression/expression.h"
#include "common/enums/conflict_action.h"
#include "common/enums/table_type.h"

namespace kuzu {
namespace binder {

struct BoundInsertInfo {
    common::TableType tableType;
    std::shared_ptr<Expression> pattern;
    expression_vector columnExprs;
    expression_vector columnDataExprs;
    // When we insert RDF triples through create statement,
    // e.g. CREATE (s {iri:a})-[p {iri:b}]->(o {iri:c})
    // We will compile 3 iri insertions to resource table. They are s.iri, o.iri
    // and p'.iri (note this is not p.iri because we are inserting iri to rel table)
    // followed by 1 insertion to triple table.
    // When we need to RETURN p.iri after insertion. We need to replace all p'.iri with p.iri.
    // Otherwise, no operator can p.iri into scope.
    std::shared_ptr<Expression> iriReplaceExpr;
    common::ConflictAction conflictAction;

    BoundInsertInfo(common::TableType tableType, std::shared_ptr<Expression> pattern)
        : tableType{tableType}, pattern{std::move(pattern)},
          conflictAction{common::ConflictAction::ON_CONFLICT_THROW} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundInsertInfo);

private:
    BoundInsertInfo(const BoundInsertInfo& other)
        : tableType{other.tableType}, pattern{other.pattern}, columnExprs{other.columnExprs},
          columnDataExprs{other.columnDataExprs}, iriReplaceExpr{other.iriReplaceExpr},
          conflictAction{other.conflictAction} {}
};

} // namespace binder
} // namespace kuzu
