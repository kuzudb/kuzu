#pragma once

#include "common/types/types.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace graph {

class Graph {
public:
    explicit Graph(main::ClientContext* context) : context{context} {}
    virtual ~Graph() = default;

    virtual common::offset_t getNumNodes() = 0;

    virtual common::offset_t getNumEdges() = 0;

    virtual uint64_t getFwdDegreeOffset(common::offset_t offset) = 0;

    virtual void initializeStateFwdNbrs(common::offset_t offset) = 0;

    virtual bool hasMoreFwdNbrs() = 0;

    virtual common::ValueVector* getFwdNbrs() = 0;

protected:
    main::ClientContext* context;
};

} // namespace graph
} // namespace kuzu
