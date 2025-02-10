#pragma once

#include "common/cast.h"
#include "common/types/types.h"

namespace kuzu {
namespace function {

// Maintain algorithm specific data structures
class GDSAuxiliaryState {
public:
    GDSAuxiliaryState() = default;
    virtual ~GDSAuxiliaryState() = default;

    // Initialize state for source node.
    virtual void initSource(common::nodeID_t) {}
    // Initialize state before extending from `fromTable` to `toTable`.
    // Normally you want to pin data structures on `toTableID`.
    virtual void beginFrontierCompute(common::table_id_t fromTableID,
        common::table_id_t toTableID) = 0;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

class EmptyGDSAuxiliaryState : public GDSAuxiliaryState {
public:
    EmptyGDSAuxiliaryState() = default;

    void beginFrontierCompute(common::table_id_t, common::table_id_t) override {}
};

} // namespace function
} // namespace kuzu
