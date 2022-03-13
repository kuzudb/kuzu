#include "src/loader/include/loader_progress_bar.h"

namespace graphflow {
namespace loader {

void LoaderProgressBar::addAndStartNewJob(string jobName, uint64_t numTasks) {
    lock_guard<mutex> lock(progressBarLock);
    if (numTasks == 0) {
        cerr << "\nTrying to add start a new job with 0 tasks. Jobname: " << jobName << endl;
        return;
    }
    numJobs++;
    if (numTasksInCurrentJob > 0 && numTasksFinishedInCurrentJob != numTasksInCurrentJob) {
        cerr << "\nStarting a progress bar on a new job without finishing the previous one."
             << " numTasksInCurrentJob: " << to_string(numTasksInCurrentJob)
             << " numTasksFinishedInCurrentJob: " << to_string(numTasksFinishedInCurrentJob)
             << endl;
    }
    currentJobName = jobName;
    numTasksInCurrentJob = numTasks;
    numTasksFinishedInCurrentJob = 0;
}

void LoaderProgressBar::incrementTaskFinished() {
    lock_guard<mutex> lock(progressBarLock);
    if (numTasksInCurrentJob <= 0) {
        cerr << "incrementTaskFinished called but numTasksInCurrentJob is <= 0, i.e., there is "
                "no current job being tracked "
             << endl;
        return;
    }
    if (numTasksFinishedInCurrentJob > 0) {
        // If we are already printing the progress bar, put the cursor 2 lines above
        cout << "\033[F\033[F\033[F";
    }
    numTasksFinishedInCurrentJob++;
    printProgressInfoInGreenFont();
    // Reset numTasksInCurrentJob and numTasksFinishedInCurrentJob to -1 if job is finished.
    if (numTasksFinishedInCurrentJob == numTasksInCurrentJob) {
        // We first clear the last 3 lines
        int maxValue = max((int)currentJobName.size() + 40, 80);
        string clearLine(maxValue, ' ');
        cout << "\033[F\033[F\033[F";
        cout << clearLine << "\n" << clearLine << "\n" << clearLine;
        // Then we go 2 lines above and log some info about finished job.
        cout << "\033[F\033[F";
        cout << "Done " << currentJobName << "\r";
        lastFinishedLineLenToClear = 5 + currentJobName.size();
        cout.flush();
        numJobsFinished++;
        numTasksFinishedInCurrentJob = -1;
        numTasksInCurrentJob = -1;
    }
    cerr << "\033[0m";
}

void LoaderProgressBar::printProgressInfoInGreenFont() const {
    clearLastFinishedLine();
    // First printed line
    cerr << "\033[1;32m";
    cout << "# Finished Jobs/# Started Jobs: [" << to_string(numJobsFinished) << "/"
         << to_string(numJobs) << "]" << endl;
    // Second printed line
    cout << "Current Job: " << currentJobName << endl;

    float progress = (float)numTasksFinishedInCurrentJob / (float)numTasksInCurrentJob;
    int barWidth = 70;
    cout << "["; //
    int pos = barWidth * progress;
    for (int i = 0; i < barWidth; ++i) {
        if (i < pos)
            cout << "=";
        else if (i == pos)
            cout << ">";
        else
            cout << " ";
    }
    cout << "] " << int(progress * 100.0) << " %\n";
    cout.flush();
}

} // namespace loader
} // namespace graphflow
