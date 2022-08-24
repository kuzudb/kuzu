#include "include/enumerator.h"

#include "src/binder/query/include/bound_regular_query.h"
#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"
#include "src/planner/logical_plan/logical_operator/include/logical_sorter.h"

namespace graphflow {
namespace planner {

static const bool ENABLE_ASP = true;

static expression_vector extractPredicatesForNode(
    expression_vector& predicates, NodeExpression& node) {
    expression_vector result;
    for (auto& predicate : predicates) {
        auto names = predicate->getDependentVariableNames();
        if (names.size() == 1 && names.contains(node.getUniqueName())) {
            result.push_back(predicate);
        }
    }
    return result;
}

unique_ptr<LogicalPlan> Enumerator::getIS2Plan(const BoundStatement& statement) {
    auto queryPart = extractQueryPart(statement);
    auto queryGraph = queryPart->getQueryGraph(0);
    auto predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    auto p = queryGraph->getQueryNode(0);
    assert(p->getRawName() == "p");
    auto c = queryGraph->getQueryNode(1);
    assert(c->getRawName() == "c");
    auto post = queryGraph->getQueryNode(2);
    assert(post->getRawName() == "post");
    auto op = queryGraph->getQueryNode(3);
    assert(op->getRawName() == "op");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");

    auto plan = createRelScanPlan(e1, p, predicates, true);
    compileHashJoinWithNode(*plan, c, predicates);
    auto scanE2Plan = createRelScanPlan(e2, c, predicates, false);
    joinOrderEnumerator.appendASPJoin(c, *plan, *scanE2Plan);
    auto scanE3Plan = createRelScanPlan(e3, post, predicates, false);
    joinOrderEnumerator.appendASPJoin(post, *plan, *scanE3Plan);
    compileHashJoinWithNode(*plan, op, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

unique_ptr<LogicalPlan> Enumerator::getIS6Plan(const BoundStatement& statement) {
    auto queryPart = extractQueryPart(statement);
    auto queryGraph = queryPart->getQueryGraph(0);
    auto predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    auto comment = queryGraph->getQueryNode(0);
    assert(comment->getRawName() == "comment");
    auto post = queryGraph->getQueryNode(1);
    assert(post->getRawName() == "post");
    auto f = queryGraph->getQueryNode(2);
    assert(f->getRawName() == "f");
    auto p = queryGraph->getQueryNode(3);
    assert(p->getRawName() == "p");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");

    auto plan = createRelScanPlan(e1, comment, predicates, true);
    auto scanE2Plan = createRelScanPlan(e2, post, predicates, false);
    joinOrderEnumerator.appendASPJoin(post, *plan, *scanE2Plan);
    compileHashJoinWithNode(*plan, f, predicates);
    auto scanE3Plan = createRelScanPlan(e3, f, predicates, false);
    joinOrderEnumerator.appendASPJoin(f, *plan, *scanE3Plan);
    compileHashJoinWithNode(*plan, p, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

unique_ptr<LogicalPlan> Enumerator::getIS7Plan(const BoundStatement& statement) {
    auto queryPart = extractQueryPart(statement);
    auto queryGraph = queryPart->getQueryGraph(0);
    auto predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    auto mAuth = queryGraph->getQueryNode(0);
    assert(mAuth->getRawName() == "mAuth");
    auto cmt0 = queryGraph->getQueryNode(1);
    assert(cmt0->getRawName() == "cmt0");
    auto cmt1 = queryGraph->getQueryNode(2);
    assert(cmt1->getRawName() == "cmt1");
    auto rAuth = queryGraph->getQueryNode(3);
    assert(rAuth->getRawName() == "rAuth");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");

    auto plan = createRelScanPlan(e1, cmt0, predicates, true);
    joinOrderEnumerator.appendExtend(
        *e2, e2->getSrcNodeName() == cmt0->getUniqueName() ? FWD : BWD, *plan);
    compileHashJoinWithNode(*plan, cmt1, predicates);
    auto scanE3Plan = createRelScanPlan(e3, cmt1, predicates, false);
    joinOrderEnumerator.appendASPJoin(cmt1, *plan, *scanE3Plan);
    compileHashJoinWithNode(*plan, rAuth, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

// (a)-[e1]->(b)-[e2]->(c)-[e3]->(d)
unique_ptr<LogicalPlan> Enumerator::getThreeHopPlan(const BoundStatement& statement) {
    auto queryPart = extractQueryPart(statement);
    auto queryGraph = queryPart->getQueryGraph(0);
    auto predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    auto a = queryGraph->getQueryNode(0);
    assert(a->getRawName() == "a");
    auto b = queryGraph->getQueryNode(1);
    assert(b->getRawName() == "b");
    auto c = queryGraph->getQueryNode(2);
    assert(c->getRawName() == "c");
    auto d = queryGraph->getQueryNode(3);
    assert(d->getRawName() == "d");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");
    //******* plan compilation ************

    // compile a-e1-b-e2-c
    auto plan = createRelScanPlan(e1, b, predicates, true);
    compileHashJoinWithNode(*plan, a, predicates);
    joinOrderEnumerator.appendExtend(
        *e2, e2->getSrcNodeName() == b->getUniqueName() ? FWD : BWD, *plan);
    compileHashJoinWithNode(*plan, c, predicates);
    // hash join with e3
    auto scanE3Plan = createRelScanPlan(e3, c, predicates, false);
    if (ENABLE_ASP) {
        joinOrderEnumerator.appendASPJoin(c, *plan, *scanE3Plan);
    } else {
        joinOrderEnumerator.appendHashJoin(c, *plan, *scanE3Plan);
    }
    compileHashJoinWithNode(*plan, d, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

// (a)-[e1]->(b)-[e2]->(c), (a)-[e3]->(c)
unique_ptr<LogicalPlan> Enumerator::getTrianglePlan(const BoundStatement& boundStatement) {
    auto queryPart = extractQueryPart(boundStatement);
    auto queryGraph = queryPart->getQueryGraph(0);
    expression_vector predicates;
    if (queryPart->getQueryGraphPredicate(0)) {
        predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    }
    auto a = queryGraph->getQueryNode(0);
    assert(a->getRawName() == "a");
    auto b = queryGraph->getQueryNode(1);
    assert(b->getRawName() == "b");
    auto c = queryGraph->getQueryNode(2);
    assert(c->getRawName() == "c");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");

    //******* plan compilation ************

    // compile a-e1-b
    auto plan = createRelScanPlan(e1, a, predicates, true);
    compileHashJoinWithNode(*plan, b, predicates);
    auto bGroupPos = plan->getSchema()->getGroupPos(b->getIDProperty());

    // compile closing with b-e2, a-e3
    auto be2 = createRelScanPlan(e2, b, predicates, false);
    appendSorter(c, *be2);
    auto ae3 = createRelScanPlan(e3, a, predicates, false);
    appendSorter(c, *ae3);
    auto buildPlans = vector<LogicalPlan*>{be2.get(), ae3.get()};
    auto hashNodes = vector<shared_ptr<NodeExpression>>{b, a};
    compileIntersectWithNode(*plan, buildPlans, c, hashNodes);

    compileHashJoinWithNode(*plan, c, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

// a-[e1]->b-[e2]->c, a-[e3]->d-[e4]->c
unique_ptr<LogicalPlan> Enumerator::getCyclePlan(
    const graphflow::binder::BoundStatement& boundStatement) {
    auto queryPart = extractQueryPart(boundStatement);
    auto queryGraph = queryPart->getQueryGraph(0);
    expression_vector predicates;
    if (queryPart->getQueryGraphPredicate(0)) {
        predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    }
    auto a = queryGraph->getQueryNode(0);
    assert(a->getRawName() == "a");
    auto b = queryGraph->getQueryNode(1);
    assert(b->getRawName() == "b");
    auto c = queryGraph->getQueryNode(2);
    assert(c->getRawName() == "c");
    auto d = queryGraph->getQueryNode(3);
    assert(d->getRawName() == "d");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");
    auto e4 = queryGraph->getQueryRel(3);
    assert(e4->getRawName() == "e4");

    // probe side: d-e3-a-e1-b
    auto plan = createRelScanPlan(e1, a, predicates, true);
    compileHashJoinWithNode(*plan, b, predicates);
    appendFlattenIfNecessary(plan->getSchema()->getGroupPos(b->getIDProperty()), *plan);
    joinOrderEnumerator.appendExtend(
        *e3, e3->getDstNodeName() == d->getUniqueName() ? FWD : BWD, *plan);
    compileHashJoinWithNode(*plan, d, predicates);
    appendFlattenIfNecessary(plan->getSchema()->getGroupPos(d->getIDProperty()), *plan);

    // build sides: d-e4, b-e2
    auto de4 = createRelScanPlan(e4, d, predicates, false);
    appendSorter(c, *de4);
    auto be2 = createRelScanPlan(e2, b, predicates, false);
    appendSorter(c, *be2);
    auto buildPlans = vector<LogicalPlan*>{de4.get(), be2.get()};
    auto hashNodes = vector<shared_ptr<NodeExpression>>{d, b};
    compileIntersectWithNode(*plan, buildPlans, c, hashNodes);

    compileHashJoinWithNode(*plan, c, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

// a-[e1]->b-[e2]->c, a-[e3]->d-[e4]->c, a-[e5]->c, b-[e6]->d
unique_ptr<LogicalPlan> Enumerator::getCliquePlan(
    const graphflow::binder::BoundStatement& boundStatement) {
    auto queryPart = extractQueryPart(boundStatement);
    auto queryGraph = queryPart->getQueryGraph(0);
    expression_vector predicates;
    if (queryPart->getQueryGraphPredicate(0)) {
        predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    }
    auto a = queryGraph->getQueryNode(0);
    assert(a->getRawName() == "a");
    auto b = queryGraph->getQueryNode(1);
    assert(b->getRawName() == "b");
    auto c = queryGraph->getQueryNode(2);
    assert(c->getRawName() == "c");
    auto d = queryGraph->getQueryNode(3);
    assert(d->getRawName() == "d");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");
    auto e4 = queryGraph->getQueryRel(3);
    assert(e4->getRawName() == "e4");
    auto e5 = queryGraph->getQueryRel(4);
    assert(e5->getRawName() == "e5");
    auto e6 = queryGraph->getQueryRel(5);
    assert(e6->getRawName() == "e6");

    // intersect subplan SP1: a-e1-b-e2-c, a-e5-c
    // probe side: a-e1-b
    auto plan = createRelScanPlan(e1, a, predicates, true);
    compileHashJoinWithNode(*plan, b, predicates);
    appendFlattenIfNecessary(plan->getSchema()->getGroupPos(b->getIDProperty()), *plan);
    // build sides: b-e2-c, a-e5-c
    auto be2 = createRelScanPlan(e2, b, predicates, false);
    appendSorter(c, *be2);
    auto ae5 = createRelScanPlan(e5, a, predicates, false);
    appendSorter(c, *ae5);
    auto sp1BuildPlans = vector<LogicalPlan*>{be2.get(), ae5.get()};
    auto sp1HashNodes = vector<shared_ptr<NodeExpression>>{b, a};
    compileIntersectWithNode(*plan, sp1BuildPlans, c, sp1HashNodes);

    // intersect subplan: SP1-e3-d, b-e6-d, d-e4-c
    compileHashJoinWithNode(*plan, c, predicates);
    appendFlattenIfNecessary(plan->getSchema()->getGroupPos(c->getIDProperty()), *plan);
    // build sides: a-e3->d, b-e6->d, c<-e4-d
    auto ae3 = createRelScanPlan(e3, a, predicates, false);
    appendSorter(d, *ae3);
    auto be6 = createRelScanPlan(e6, b, predicates, false);
    appendSorter(d, *be6);
    auto ce4 = createRelScanPlan(e4, c, predicates, false);
    appendSorter(d, *ce4);
    auto buildPlans = vector<LogicalPlan*>{ae3.get(), be6.get(), ce4.get()};
    auto hashNodes = vector<shared_ptr<NodeExpression>>{a, b, c};
    compileIntersectWithNode(*plan, buildPlans, d, hashNodes);

    compileHashJoinWithNode(*plan, d, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

NormalizedQueryPart* Enumerator::extractQueryPart(const BoundStatement& statement) {
    assert(statement.getStatementType() == StatementType::QUERY);
    auto& regularQuery = (BoundRegularQuery&)statement;
    assert(regularQuery.getNumSingleQueries() == 1);
    auto singleQuery = regularQuery.getSingleQuery(0);
    propertiesToScan.clear();
    for (auto& expression : singleQuery->getPropertiesToRead()) {
        assert(expression->expressionType == PROPERTY);
        propertiesToScan.push_back(expression);
    }
    assert(singleQuery->getNumQueryParts() == 1);
    return singleQuery->getQueryPart(0);
}

unique_ptr<LogicalPlan> Enumerator::createRelScanPlan(shared_ptr<RelExpression> rel,
    shared_ptr<NodeExpression>& boundNode, expression_vector& predicates, bool isScanNodeTable) {
    auto plan = make_unique<LogicalPlan>();
    joinOrderEnumerator.appendScanNodeID(boundNode, *plan);
    if (isScanNodeTable) {
        auto predicatesToApply = extractPredicatesForNode(predicates, *boundNode);
        joinOrderEnumerator.planFiltersForNode(predicatesToApply, *boundNode, *plan);
        joinOrderEnumerator.planPropertyScansForNode(*boundNode, *plan);
    }
    auto direction = rel->getSrcNodeName() == boundNode->getUniqueName() ? FWD : BWD;
    joinOrderEnumerator.appendExtend(*rel, direction, *plan);
    return plan;
}

void Enumerator::compileHashJoinWithNode(
    LogicalPlan& plan, shared_ptr<NodeExpression>& node, expression_vector& predicates) {
    auto buildPlan = make_unique<LogicalPlan>();
    joinOrderEnumerator.appendScanNodeID(node, *buildPlan);
    auto predicatesToApply = extractPredicatesForNode(predicates, *node);
    joinOrderEnumerator.planFiltersForNode(predicatesToApply, *node, *buildPlan);
    joinOrderEnumerator.planPropertyScansForNode(*node, *buildPlan);
    if (ENABLE_ASP) {
        joinOrderEnumerator.appendASPJoin(node, plan, *buildPlan);
    } else {
        joinOrderEnumerator.appendHashJoin(node, plan, *buildPlan);
    }
}

void Enumerator::compileIntersectWithNode(LogicalPlan& plan, vector<LogicalPlan*>& buildPlans,
    shared_ptr<NodeExpression>& intersectNode, vector<shared_ptr<NodeExpression>>& hashNodes) {
    auto intersectType = IntersectType::MW_JOIN;
    if (ENABLE_ASP) {
        intersectType = IntersectType::ASP_MW_JOIN;
        for (auto i = 0u; i < buildPlans.size(); ++i) {
            auto hashNode = hashNodes[i];
            joinOrderEnumerator.appendSemiMasker(hashNode, plan);
        }
        appendSink(plan);
    }
    for (auto i = 0u; i < buildPlans.size(); ++i) {
        auto hashNode = hashNodes[i];
        auto hashGroupPos = plan.getSchema()->getGroupPos(hashNode->getIDProperty());
        appendFlattenIfNecessary(hashGroupPos, plan);
    }
    auto intersect = make_shared<LogicalIntersect>(intersectNode, plan.getLastOperator());
    intersect->setIntersectType(intersectType);
    for (auto i = 0u; i < buildPlans.size(); ++i) {
        auto hashNode = hashNodes[i];
        auto buildPlan = buildPlans[i];
        auto buildSideSchema = buildPlan->getSchema();
        assert(buildSideSchema->getNumGroups() == 2);
        auto keyGroupPos = buildSideSchema->getGroupPos(hashNode->getIDProperty());
        assert(keyGroupPos == 0);
        auto payloadGroupPos = buildSideSchema->getGroupPos(intersectNode->getIDProperty());
        assert(payloadGroupPos == 1);
        // No edge property.
        assert(buildSideSchema->getExpressionsInScope(payloadGroupPos).size() == 1);
        auto buildInfo = make_unique<BuildInfo>(
            hashNode, buildSideSchema->copy(), buildSideSchema->getExpressionsInScope());
        intersect->addChild(buildPlan->getLastOperator(), move(buildInfo));
    }
    auto schema = plan.getSchema();
    auto outputGroupPos = schema->createGroup();
    schema->insertToGroupAndScope(intersectNode->getNodeIDPropertyExpression(), outputGroupPos);
    plan.appendOperator(move(intersect));
}

void Enumerator::appendSorter(shared_ptr<NodeExpression> expressionToSort, LogicalPlan& plan) {
    auto sorter = make_shared<LogicalSorter>(expressionToSort, plan.getLastOperator());
    plan.appendOperator(move(sorter));
}

} // namespace planner
} // namespace graphflow