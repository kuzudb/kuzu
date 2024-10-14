#include "storage/stats/column_stats.h"

#include "function/hash/vector_hash_functions.h"

namespace kuzu {
namespace storage {

void ColumnStats::update(const common::ValueVector* vector) {
    if (!hashes) {
        hashes = std::make_unique<common::ValueVector>(common::LogicalTypeID::UINT64);
    }
    hashes->state = vector->state;
    function::VectorHashFunction::computeHash(*vector, vector->state->getSelVector(), *hashes,
        hashes->state->getSelVector());
    if (hashes->hasNoNullsGuarantee()) {
        for (auto i = 0u; i < hashes->state->getSelVector().getSelSize(); i++) {
            hll.insertElement(hashes->getValue<common::hash_t>(i));
        }
    } else {
        for (auto i = 0u; i < hashes->state->getSelVector().getSelSize(); i++) {
            if (!hashes->isNull(i)) {
                hll.insertElement(hashes->getValue<common::hash_t>(i));
            }
        }
    }
    hashes->state = nullptr;
    hashes->setAllNonNull();
}

} // namespace storage
} // namespace kuzu
