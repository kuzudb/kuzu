#pragma once

#include "function/gds/gds_object_manager.h"
#include "function/gds/gds_vertex_compute.h"

namespace kuzu {
namespace algo_extension {

static constexpr common::offset_t INVALID_COMPONENT_ID = common::INVALID_OFFSET;

struct OffsetManager {
    explicit OffsetManager(const common::table_id_map_t<common::offset_t>& maxOffsetMap);

    void pinTableID(common::table_id_t tableID) { curOffset = getStartOffset(tableID); }
    common::offset_t getStartOffset(common::table_id_t tableID) const {
        KU_ASSERT(tableIDToStartOffset.contains(tableID));
        return tableIDToStartOffset.at(tableID);
    }
    common::offset_t getCurrentOffset() const {
        KU_ASSERT(curOffset != common::INVALID_OFFSET);
        return curOffset;
    }

private:
    common::offset_t curOffset = common::INVALID_OFFSET;
    common::table_id_map_t<common::offset_t> tableIDToStartOffset;
};

class ComponentIDs {
public:
    std::atomic<common::offset_t>* getData(common::table_id_t tableID) {
        return denseObjects.getData(tableID);
    }
    void pinTableID(common::table_id_t tableID) { curData = denseObjects.getData(tableID); }

    void setComponentID(common::offset_t offset, common::offset_t componentID) {
        KU_ASSERT(curData != nullptr);
        curData[offset] = componentID;
    }

    common::offset_t getComponentID(common::offset_t offset) const {
        KU_ASSERT(curData != nullptr);
        return curData[offset];
    }
    bool hasValidComponentID(common::offset_t offset) const {
        return getComponentID(offset) != INVALID_COMPONENT_ID;
    }

    static ComponentIDs getSequenceComponentIDs(
        const common::table_id_map_t<common::offset_t>& maxOffsetMap,
        const OffsetManager& offsetManager, storage::MemoryManager* mm);
    static ComponentIDs getUnvisitedComponentIDs(
        const common::table_id_map_t<common::offset_t>& maxOffsetMap, storage::MemoryManager* mm);

private:
    std::atomic<common::offset_t>* curData = nullptr;
    function::GDSDenseObjectManager<std::atomic<common::offset_t>> denseObjects;
};

class InitSequenceComponentIDsVertexCompute : public function::VertexCompute {
public:
    InitSequenceComponentIDsVertexCompute(ComponentIDs& componentIDs, OffsetManager& offsetManager)
        : componentIDs{componentIDs}, offsetManager{offsetManager} {}

    bool beginOnTable(common::table_id_t tableID) override {
        componentIDs.pinTableID(tableID);
        offsetManager.pinTableID(tableID);
        return true;
    }

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            componentIDs.setComponentID(i, i + offsetManager.getCurrentOffset());
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<InitSequenceComponentIDsVertexCompute>(componentIDs, offsetManager);
    }

private:
    ComponentIDs& componentIDs;
    OffsetManager& offsetManager;
};

class ComponentIDsPair {
public:
    explicit ComponentIDsPair(ComponentIDs& componentIDs) : componentIDs{componentIDs} {}

    void pinCurTableID(common::table_id_t tableID) { curData = componentIDs.getData(tableID); }

    void pinNextTableID(common::table_id_t tableID) { nextData = componentIDs.getData(tableID); }

    bool update(common::offset_t boundOffset, common::offset_t nbrOffset);

private:
    std::atomic<common::offset_t>* curData = nullptr;
    std::atomic<common::offset_t>* nextData = nullptr;
    ComponentIDs& componentIDs;
};

class ComponentIDsOutputVertexCompute : public function::GDSResultVertexCompute {
public:
    ComponentIDsOutputVertexCompute(storage::MemoryManager* mm,
        function::GDSFuncSharedState* sharedState, ComponentIDs& componentIDs)
        : GDSResultVertexCompute{mm, sharedState}, componentIDs{componentIDs} {
        nodeIDVector = createVector(common::LogicalType::INTERNAL_ID());
        componentIDVector = createVector(common::LogicalType::UINT64());
    }

    void beginOnTableInternal(common::table_id_t tableID) override {
        componentIDs.pinTableID(tableID);
    }

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t tableID) override;

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<ComponentIDsOutputVertexCompute>(mm, sharedState, componentIDs);
    }

private:
    ComponentIDs& componentIDs;
    std::unique_ptr<common::ValueVector> nodeIDVector;
    std::unique_ptr<common::ValueVector> componentIDVector;
};

} // namespace algo_extension
} // namespace kuzu
