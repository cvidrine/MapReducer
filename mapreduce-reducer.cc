/**
 * File: mapreduce-reducer.cc
 * --------------------------
 * Presents the implementation of the MapReduceReducer class,
 * which is charged with the responsibility of collating all of the
 * intermediate files for a given hash number, sorting that collation,
 * grouping the sorted collation by key, and then pressing that result
 * through the reducer executable.
 *
 * See the documentation in mapreduce-reducer.h for more information.
 */

#include "mapreduce-reducer.h"
#include "mr-names.h"
using namespace std;

MapReduceReducer::MapReduceReducer(const string& serverHost, unsigned short serverPort,
                                   const string& cwd, const string& executable, const string& outputPath, const size_t numInputs) : 
  MapReduceWorker(serverHost, serverPort, cwd, executable, outputPath), numFiles(numInputs) {}


void MapReduceReducer::reduce() const {
  while (true) {
    string name;
    if (!requestInput(name)) break;
    alertServerOfProgress("About to process \"" + name + "\".");
    string base = extractBase(name);
    string output = outputPath + "/" + base + ".output";
    string fileList = "";
    for(size_t i=1; i<=numFiles; i++){
        string filePath = name.substr(0, name.rfind('/')+1) + numberToString(i) + "." + base + ".mapped";
        fileList = fileList + " " + filePath;
    }
    string tempPath = outputPath + "/" + base + ".sorted";
    string input = "cat " + fileList + " | sort | /afs/ir.stanford.edu/users/c/v/cvidrine/110/assign6/group-by-key.py >" + tempPath;
    int status = system(input.c_str());
    bool success = processInput(tempPath, output);
    remove(tempPath.c_str());
    notifyServer(name, success);
  }
}




