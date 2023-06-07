#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundCreateRDFGraph : public BoundDDL {
public:
    explicit BoundCreateRDFGraph(std::string rdfGraphName)
        : BoundDDL{common::StatementType::CREATE_RDF_GRAPH, std::move(rdfGraphName)} {}
};

} // namespace binder
} // namespace kuzu
