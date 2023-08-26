#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "binder/bound_statement.h"
#include "common/copier_config/copier_config.h"
#include "common/types/internal_id_t.h"

namespace kuzu {
namespace binder {

class BoundCopyRDF : public BoundStatement {
public:
    BoundCopyRDF(common::CopyDescription copyDescription, common::rdf_graph_id_t rdfGraphID,
        std::string rdfGraphName)
        : BoundStatement{common::StatementType::COPY_RDF,
              BoundStatementResult::createSingleStringColumnResult()},
          copyDescription{std::move(copyDescription)}, rdfGraphID{rdfGraphID},
          rdfGraphName{std::move(rdfGraphName)} {}

    inline common::CopyDescription getCopyDescription() const { return copyDescription; }

    inline common::table_id_t getRDFGraphID() const { return rdfGraphID; }

    inline std::string getRDFGraphName() const { return rdfGraphName; }

private:
    common::CopyDescription copyDescription;
    common::rdf_graph_id_t rdfGraphID;
    std::string rdfGraphName;
};

} // namespace binder
} // namespace kuzu
