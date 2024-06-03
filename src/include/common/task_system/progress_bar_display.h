#pragma once

#include <stdint.h>

namespace kuzu {
namespace common {

/**
 * @brief Interface for displaying progress of a pipeline and a query.
 */
class ProgressBarDisplay {
public:
    ProgressBarDisplay() : pipelineProgress{0}, numPipelines{0}, numPipelinesFinished{0} {};

    virtual ~ProgressBarDisplay() = default;

    // Update the progress of the pipeline and the number of finished pipelines
    virtual void updateProgress(double newPipelineProgress, uint32_t newNumPipelinesFinished) = 0;

    // Finish the progress display
    virtual void finishProgress() = 0;

    void setNumPipelines(uint32_t newNumPipelines) { numPipelines = newNumPipelines; };

protected:
    double pipelineProgress;
    uint32_t numPipelines;
    uint32_t numPipelinesFinished;
};

} // namespace common
} // namespace kuzu
