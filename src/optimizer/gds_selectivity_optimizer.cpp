#include "optimizer/gds_selectivity_optimizer.h"

#include "binder/expression/node_expression.h"
#include "binder/expression_visitor.h"
#include "function/gds/gds.h"
#include "planner/operator/logical_filter.h"
#include "planner/operator/logical_gds_call.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_semi_mask_dependency.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/operator/logical_projection.h"

using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
    namespace optimizer {

        void GDSSelectivityOptimizer::rewrite(planner::LogicalPlan *plan) {
            plan->setLastOperator(visitOperator(plan->getLastOperator()));
        }

        std::shared_ptr<planner::LogicalOperator> GDSSelectivityOptimizer::visitOperator(
                const std::shared_ptr<planner::LogicalOperator> &op) {
            // bottom-up traversal
            for (auto i = 0u; i < op->getNumChildren(); ++i) {
                op->setChild(i, visitOperator(op->getChild(i)));
            }
            auto result = visitOperatorReplaceSwitch(op);
            result->computeFlatSchema();
            return result;
        }

        std::shared_ptr<planner::LogicalOperator> GDSSelectivityOptimizer::visitHashJoinReplace(
                std::shared_ptr<planner::LogicalOperator> op) {
            // For now, keep it simple and check on probe side we have ANN_SEARCH GDS call and hash join
            // not returning other than _id
            auto hashJoin = op->ptrCast<LogicalHashJoin>();
            bool gdsHasSelectiveOutput = false;
            const NodeExpression *gdsNodeInput;
            auto probeSide = hashJoin->getChild(0);
            if (probeSide->getOperatorType() == LogicalOperatorType::GDS_CALL) {
                auto gdsBindData = probeSide->constCast<LogicalGDSCall>().getInfo().getBindData();
                gdsHasSelectiveOutput = gdsBindData->hasSelectiveOutput();
                gdsNodeInput = gdsBindData->getNodeInput()->constPtrCast<NodeExpression>();
            }

            // Bail out if we don't have GDS call with selective output
            if (!gdsHasSelectiveOutput) {
                return op;
            }
            // Extremely hacky!!
            bool canRemoveHashJoin = false;
            bool addLookupScan = false;
            expression_vector lookupProperties;

            auto buildSide = hashJoin->getChild(1);
            if (buildSide->getOperatorType() == LogicalOperatorType::SEMI_MASKER) {
                auto outExpressions = buildSide->getSchema()->getExpressionsInScope();
                // If we have only one output and it is the same as the GDS node input, we can remove hash
                // join and replace it with semi mask dependency
                if (outExpressions.size() == 1 &&
                    outExpressions[0]->getUniqueName() == gdsNodeInput->getInternalID()->getUniqueName()) {
                    canRemoveHashJoin = true;
                } else {
                    auto project = buildSide->getChild(0);
                    std::shared_ptr<LogicalOperator> filterNode;
                    bool removeProjection = false;
                    if (project->getOperatorType() == LogicalOperatorType::PROJECTION) {
                        auto projectionNode = project->ptrCast<LogicalProjection>();
                        auto projectExpressions = projectionNode->getExpressionsToProject();
                        // Remove all expressions that are not the same as the GDS node input
                        for (auto &expr: projectExpressions) {
                            if (expr->getUniqueName() == gdsNodeInput->getInternalID()->getUniqueName()) {
                                removeProjection = true;
                            }
                        }
                        filterNode = project->getChild(0);
                    } else {
                        filterNode = project;
                    }
                    if (filterNode->getOperatorType() == LogicalOperatorType::FILTER) {
                        auto filterExpression = filterNode->ptrCast<LogicalFilter>()->getPredicate();

                        // Collect all property expressions in the filter expression
                        expression_set filterProperties;
                        auto collector = PropertyExprCollector();
                        collector.visit(filterExpression);
                        for (auto &expr: collector.getPropertyExprs()) {
                            filterProperties.insert(expr);
                        }

                        auto scanNode = filterNode->getChild(0);
                        bool anotherFilterNode = false;
                        std::shared_ptr<LogicalOperator> anotherFilter;
                        if (scanNode->getOperatorType() == LogicalOperatorType::FILTER) {
                            anotherFilter = scanNode;
                            auto filterExpression2 = anotherFilter->ptrCast<LogicalFilter>()->getPredicate();
                            auto collector = PropertyExprCollector();
                            collector.visit(filterExpression2);
                            for (auto &expr: collector.getPropertyExprs()) {
                                filterProperties.insert(expr);
                            }
                            scanNode = scanNode->getChild(0);
                            anotherFilterNode = true;
                        }
                        if (scanNode->getOperatorType() == LogicalOperatorType::SCAN_NODE_TABLE) {
                            // Create new scan node with only filter properties
                            auto scan = scanNode->ptrCast<LogicalScanNodeTable>();
                            expression_vector newProperties;
                            for (auto &prop: scan->getProperties()) {
                                if (filterProperties.find(prop) != filterProperties.end()) {
                                    newProperties.push_back(prop);
                                }
                                lookupProperties.push_back(prop);
                            }
                            auto newScan = std::make_shared<LogicalScanNodeTable>(scan->getNodeID(),
                                                                                  scan->getTableIDs(), newProperties);
                            newScan->computeFlatSchema();
                            if (anotherFilterNode) {
                                anotherFilter->setChild(0, std::move(newScan));
                                anotherFilter->computeFlatSchema();
                            } else {
                                filterNode->setChild(0, std::move(newScan));
                            }
                            filterNode->computeFlatSchema();
                            if (removeProjection) {
                                buildSide->setChild(0, std::move(filterNode));
                            }
                            buildSide->computeFlatSchema();
                            canRemoveHashJoin = true;
                            addLookupScan = true;
                        }
                    }
                }
            }

            // Bail out if we can't remove hash join
            if (!canRemoveHashJoin) {
                return op;
            }

            std::vector<std::shared_ptr<LogicalOperator>> newChildren;
            newChildren.push_back(hashJoin->getChild(0));
            newChildren.push_back(hashJoin->getChild(1));
            auto ret = std::make_shared<LogicalSemiMaskDependency>(std::move(newChildren));

            if (addLookupScan) {
                auto lookupScan = std::make_shared<LogicalScanNodeTable>(gdsNodeInput->getInternalID(),
                                                                         gdsNodeInput->getTableIDs(), lookupProperties);
                lookupScan->setScanType(LogicalScanNodeTableType::MULTIPLE_OFFSET_SCAN);
                ret->computeFlatSchema();
                lookupScan->setChild(0, std::move(ret));
                return lookupScan;
            }
            return ret;
        }

    } // namespace optimizer
} // namespace kuzu
