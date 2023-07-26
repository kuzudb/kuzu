#pragma once

#include "insert_manager.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Merge : public PhysicalOperator {
public:
    Merge(const DataPos& markPos,
        std::vector<std::unique_ptr<NodeTableInsertManager>> nodeTableInsertManagers,
        std::vector<std::unique_ptr<RelTableInsertManager>> relTableInsertManagers,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::MERGE, std::move(child), id, paramsString},
          markPos{markPos}, nodeTableInsertManagers{std::move(nodeTableInsertManagers)},
          relTableInsertManagers(std::move(relTableInsertManagers)) {}

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    DataPos markPos;
    common::ValueVector* markVector = nullptr;

    std::vector<std::unique_ptr<NodeTableInsertManager>> nodeTableInsertManagers;
    std::vector<std::unique_ptr<RelTableInsertManager>> relTableInsertManagers;
};

} // namespace processor
} // namespace kuzu
