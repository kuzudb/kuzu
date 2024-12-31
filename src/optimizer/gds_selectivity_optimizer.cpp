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
#include "algorithm"

using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
    namespace optimizer {
        void FindUsefulProperty::collect(planner::LogicalOperator *op) {
            for (auto i = 0u; i < op->getNumChildren(); ++i) {
                collect(op->getChild(i).get());
            }
            visitOperatorSwitch(op);
        }

        void FindUsefulProperty::visitFilter(planner::LogicalOperator *op) {
            auto filter = op->ptrCast<LogicalFilter>();
            auto predicate = filter->getPredicate();
            PropertyExprCollector collector;
            collector.visit(predicate);
            for (auto &prop: collector.getPropertyExprs()) {
                used_properties.insert(prop);
            }
        }

        std::shared_ptr<planner::LogicalOperator>
        RemoveAccumulate::rewrite(const std::shared_ptr<planner::LogicalOperator> &op) {
            if (op->getNumChildren() > 1) {
                return op;
            }
            // bottom-up traversal
            for (auto i = 0u; i < op->getNumChildren(); ++i) {
                op->setChild(i, rewrite(op->getChild(i)));
            }
            auto result = visitOperatorReplaceSwitch(op);
            result->computeFlatSchema();
            return result;
        }

        std::shared_ptr<planner::LogicalOperator>
        RemoveAccumulate::visitAccumulateReplace(std::shared_ptr<planner::LogicalOperator> op) {
            return op->getChild(0);
        }

        std::shared_ptr<planner::LogicalOperator>
        RemoveAccumulate::visitFlattenReplace(std::shared_ptr<planner::LogicalOperator> op) {
            if (onlyAccumulate) {
                return op;
            }

            // TODO: Handle flatten operator
            return op->getChild(0);
        }

        std::shared_ptr<planner::LogicalOperator>
        RemoveAccumulate::visitSemiMaskerReplace(std::shared_ptr<planner::LogicalOperator> op) {
            if (onlyAccumulate) {
                return op;
            }

            return op->getChild(0);
        }

        std::shared_ptr<planner::LogicalOperator> RemoveUnusedNodes::rewrite(
                const std::shared_ptr<planner::LogicalOperator> &op) {
            // bottom-up traversal
            for (auto i = 0u; i < op->getNumChildren(); ++i) {
                op->setChild(i, rewrite(op->getChild(i)));
            }
            auto result = visitOperatorReplaceSwitch(op);
            result->computeFlatSchema();
            return result;
        }

        std::shared_ptr<planner::LogicalOperator>
        RemoveUnusedNodes::visitHashJoinReplace(std::shared_ptr<planner::LogicalOperator> op) {
            auto hashJoin = op->ptrCast<LogicalHashJoin>();
            auto probeSide = hashJoin->getChild(0);
            auto buildSide = hashJoin->getChild(1);
            bool buildSideNeeded = isBuildSideNeeded(buildSide);
            bool probeSideNeeded = isProbeSideNeeded(probeSide);
//            printf("Build side needed: %d, Probe side needed: %d\n", buildSideNeeded, probeSideNeeded);

            if (buildSideNeeded && probeSideNeeded) {
                return op;
            }
            RemoveAccumulate remover;
            if (!buildSideNeeded) {
                // Remove the hash join as well as build side
                // TODO: Remove TABLE_FUNCTION_CALL and replace with source of RESULT_COLLECTOR
                probeSide = remover.rewrite(probeSide);
                return probeSide;
            }

            // Remove the hash join as well as build side
            buildSide = remover.rewrite(buildSide);
            return buildSide;
        }

        bool RemoveUnusedNodes::isProbeSideNeeded(const std::shared_ptr<planner::LogicalOperator> &op) const {
            // For pattern SEMI_MASKER -> SCAN_NODE_TABLE
            if (op->getOperatorType() == LogicalOperatorType::FLATTEN &&
                op->getChild(0)->getOperatorType() == LogicalOperatorType::SCAN_NODE_TABLE) {
                auto scan = op->getChild(0)->ptrCast<LogicalScanNodeTable>();
                for (auto &prop: scan->getProperties()) {
                    if (used_properties.find(prop) != used_properties.end()) {
                        return true;
                    }
                }
                return false;
            }
            if (op->getOperatorType() == LogicalOperatorType::SCAN_NODE_TABLE) {
                auto scan = op->ptrCast<LogicalScanNodeTable>();
                for (auto &prop: scan->getProperties()) {
                    if (used_properties.find(prop) != used_properties.end()) {
                        return true;
                    }
                }
                return false;
            }
            // All other cases, we need the probe side
            return true;
        }


        bool RemoveUnusedNodes::isBuildSideNeeded(const std::shared_ptr<planner::LogicalOperator> &op) const {
            // For pattern SEMI_MASKER -> SCAN_NODE_TABLE
            if (op->getOperatorType() == LogicalOperatorType::SEMI_MASKER &&
                op->getChild(0)->getOperatorType() == LogicalOperatorType::SCAN_NODE_TABLE) {
                auto scan = op->getChild(0)->ptrCast<LogicalScanNodeTable>();
                for (auto &prop: scan->getProperties()) {
                    if (used_properties.find(prop) != used_properties.end()) {
                        return true;
                    }
                }
                return false;
            }

            // For pattern SCAN_NODE_TABLE with no semi masker
            if (op->getOperatorType() == LogicalOperatorType::SCAN_NODE_TABLE) {
                auto scan = op->ptrCast<LogicalScanNodeTable>();
                for (auto &prop: scan->getProperties()) {
                    if (used_properties.find(prop) != used_properties.end()) {
                        return true;
                    }
                }
                return false;
            }
            // All other cases, we need the build side
            return true;
        }

        std::shared_ptr<planner::LogicalOperator>
        RemoveUnusedNodes::visitProjectionReplace(std::shared_ptr<planner::LogicalOperator> op) {
            auto projection = op->ptrCast<LogicalProjection>();
            auto projectExpressions = projection->getExpressionsToProject();
            auto srcExpression = op->getChild(0)->getSchema()->getExpressionsInScope();
            expression_vector newProjectExpressions;
            for (auto &expr: projectExpressions) {
                if (expr->expressionType != common::ExpressionType::PROPERTY ||
                    std::find(srcExpression.begin(), srcExpression.end(), expr) != srcExpression.end()) {
                    newProjectExpressions.push_back(expr);
                }
            }
            if (newProjectExpressions.size() == 0 || newProjectExpressions.size() == srcExpression.size()) {
                return op->getChild(0);
            }
            if (newProjectExpressions.size() == projectExpressions.size()) {
                return op;
            }

            auto newProjection = std::make_shared<LogicalProjection>(newProjectExpressions, op->getChild(0));
            newProjection->computeFlatSchema();
            return newProjection;
        }

        std::shared_ptr<planner::LogicalOperator>
        RemoveUnusedNodes::visitScanNodeTableReplace(std::shared_ptr<planner::LogicalOperator> op) {
            auto scanNode = op->ptrCast<LogicalScanNodeTable>();
            expression_vector newProperties;
            expression_vector lookupProperties;
            for (auto &prop: scanNode->getProperties()) {
                if (used_properties.find(prop) != used_properties.end()) {
                    newProperties.push_back(prop);
                }
                lookupProperties.push_back(prop);
            }
            lookup_properties.emplace_back(scanNode->getTableIDs(), scanNode->getNodeID(), std::move(lookupProperties));
            if (newProperties.size() == scanNode->getProperties().size()) {
                return op;
            }
            auto newScan = std::make_shared<LogicalScanNodeTable>(scanNode->getNodeID(),
                                                                  scanNode->getTableIDs(), newProperties);

            // Copy over the scan type and extra info
            newScan->setScanType(scanNode->getScanType());
            if (scanNode->getExtraInfo() != nullptr) {
                newScan->setExtraInfo(scanNode->getExtraInfoCopy());
            }
            if (scanNode->getPropertyPredicates().size() > 0) {
                newScan->setPropertyPredicates(copyVector(scanNode->getPropertyPredicates()));
            }
            // Call computeFlatSchema to update the schema
            newScan->computeFlatSchema();
            return newScan;
        }

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
            auto buildSide = hashJoin->getChild(1);

            // Not so hacky anymore!!
            FindUsefulProperty collector;
            collector.collect(buildSide.get());
            RemoveUnusedNodes remover(collector.used_properties);
            buildSide = remover.rewrite(buildSide);
            RemoveAccumulate removerAccumulate;
            removerAccumulate.rewrite(buildSide);
            std::shared_ptr<LogicalOperator> ret;
            auto outExpression = hashJoin->getSchema()->getExpressionsInScope();
            KU_ASSERT(buildSide->getOperatorType() == LogicalOperatorType::SEMI_MASKER);
            // If that's the scenario then we mush have successfully moved the scan properties for lookup later.
            // So, remove the build side altogether
            if (buildSide->getChild(0)->getOperatorType() == LogicalOperatorType::SCAN_NODE_TABLE) {
                // Assert that we have no properties
                KU_ASSERT(buildSide->getChild(0)->ptrCast<LogicalScanNodeTable>()->getProperties().size() == 0);
                ret = probeSide;
            } else {
                // Case where you've semi masker
                std::vector<std::shared_ptr<LogicalOperator>> newChildren;
                newChildren.push_back(probeSide);
                newChildren.push_back(buildSide);
                ret = std::make_shared<LogicalSemiMaskDependency>(std::move(newChildren));
            }
            // TODO: we may need hash join if we have to return ids from some other node table!!
            if (remover.lookup_properties.size() > 0) {
                for (auto &lookup: remover.lookup_properties) {
                    // Check if lookup properties are part of the output expression
                    expression_vector props;
                    for (auto &prop: lookup.properties) {
                        if (std::find(outExpression.begin(), outExpression.end(), prop) != outExpression.end()) {
                            props.push_back(prop);
                        }
                    }
                    if (props.size() == 0) {
                        continue;
                    }
                    auto lookupScan = std::make_shared<LogicalScanNodeTable>(
                            lookup.idProperty,
                            lookup.tableIDs,
                            lookup.properties);
                    lookupScan->setScanType(LogicalScanNodeTableType::MULTIPLE_OFFSET_SCAN);
                    ret->computeFlatSchema();
                    lookupScan->setChild(0, std::move(ret));
                    ret = std::move(lookupScan);
                }
            }
            return ret;
        }
    } // namespace optimizer
} // namespace kuzu
