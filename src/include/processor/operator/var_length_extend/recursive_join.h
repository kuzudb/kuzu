# pragma once

#include "processor/operator/sink.h"
#include "processor/operator/var_length_extend/scan_bfs_level.h"

namespace kuzu {
namespace processor {

class RecursiveJoin : public Sink {
public:
    void initGlobalStateInternal(ExecutionContext *context) override;

    void initLocalStateInternal(ResultSet *resultSet_, ExecutionContext *context) override;

    void executeInternal(ExecutionContext *context) override;

    void finalize(ExecutionContext *context) override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    BFSMorsel* bfsMorsel;

};

}
}