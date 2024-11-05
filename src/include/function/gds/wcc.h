#pragma once

#include "common/enums/extend_direction.h"
#include "common/enums/path_semantic.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "output_writer.h"

namespace kuzu {
namespace function {

struct WCCCompState {
    std::unique_ptr<function::FrontierPair> frontierPair;
    std::unique_ptr<function::EdgeCompute> edgeCompute;

    WCCCompState(std::unique_ptr<function::FrontierPair> frontierPair,
                std::unique_ptr<function::EdgeCompute> edgeCompute) :
                frontierPair{std::move(frontierPair)},
                edgeCompute{std::move(edgeCompute)} {}

    void beginFrontierComputeBetweenTables(common::table_id_t curFrontierTableID, common::table_id_t nextFrontierTableID) const {
        frontierPair->beginFrontierComputeBetweenTables(curFrontierTableID, nextFrontierTableID);
    }
};

}
}