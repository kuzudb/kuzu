#pragma once

#include <cstdint>

namespace kuzu {
namespace planner {

enum class SemiMaskPosition : uint8_t {
    NONE = 0,
    ON_BUILD = 1,
    ON_PROBE = 2,
    PROHIBIT = 3,
};

enum class SIPDependency : uint8_t {
    NONE = 0,
    PROBE_DEPENDS_ON_BUILD = 1,
    BUILD_DEPENDS_ON_PROBE = 2,
};

/*
 * Direction of side way information passing. If direction is probe to build, then probe side
 * must have been materialized, and we need to construct a new pipeline to scan materialized result.
 * */
enum class SIPDirection {
    NONE = 0,
    PROBE_TO_BUILD = 1,
    BUILD_TO_PROBE = 2,
};

/*
 *
 * We perform side way information passing in the following cases
 * 1. Inner hash join
 *
 * If we add semi mask on build side, position is ON_BUILD, direction is BUILD_TO_PROBE and
 * dependency is PROBE_DEPENDS_ON_BUILD (because scan need to be generated before semi masker).
 *
 * If we add semi mask on probe side, position is ON_PROBE, direction is PROBE_TO_BUILD
 * and dependency is BUILD_DEPENDS_ON_PROBE.
 *
 * 2. Unnesting corrected subquery
 *
 * When unnesting a correlated subquery, we first accumulate the probe plan and pass information to
 * the build side. Semi mask position is NONE, direction is PROBE_TO_BUILD and dependency is
 * PROBE_DEPENDS_ON_BUILD.
 *
 * TODO(Xiyang): it worth thinking if we should simply put outer plan always on the build side.
 *
 * We disable semi-mask-based sip in the following cases
 *
 * During join order enumeration, we might disable semi mask if probe side cardinality is large to
 * avoid large materialization probe side intermediate result.
 *
 * During filter push down, we disable semi mask if join condition is not id-based
 * */
struct SIPInfo {
    SemiMaskPosition position = SemiMaskPosition::NONE;
    SIPDependency dependency = SIPDependency::NONE;
    SIPDirection direction = SIPDirection::NONE;
};

} // namespace planner
} // namespace kuzu
