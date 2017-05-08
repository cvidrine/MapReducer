/*
 * File: mapreduce-mapper.cc
 * -------------------------
 * Presents the implementation of the MapReduceMapper class,
 * which is charged with the responsibility of pressing through
 * a supplied input file through the provided executable and then
 * splaying that output into a large number of intermediate files
 * such that all keys that hash to the same value appear in the same
 * intermediate.
 */

#include "mapreduce-mapper.h"
#include "mr-names.h"
#include "string-utils.h"
#include <vector>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <iostream>
using namespace std;

MapReduceMapper::MapReduceMapper(const string& serverHost, unsigned short serverPort,
                                 const string& cwd, const string& executable,
                                 const string& outputPath, const size_t numBuckets) :
  MapReduceWorker(serverHost, serverPort, cwd, executable, outputPath), numHashCodes(numBuckets) {}


string newFileBase(const string base, size_t index){
    string croppedBase = base.substr(0, base.rfind(".")); 
    string secondNum = numberToString(index);
    return croppedBase + "." + secondNum+ ".mapped";
}

size_t MapReduceMapper::getHash(string word) const {
    return hash<string>()(word) % numHashCodes;
}

void MapReduceMapper::map() const {
   while (true) {
    string name;
    if (!requestInput(name)) break;
    alertServerOfProgress("About to process \"" + name + "\".");
    string base = extractBase(name);
    string output = outputPath + "/" + changeExtension(base, "input", "mapped");
    bool success = processInput(name, output);
    //TODO: Handle processInput failues
   vector<ofstream> outputFiles(numHashCodes);
    for(size_t i=0; i<numHashCodes; i++) outputFiles[i].open(outputPath+ "/" + newFileBase(base, i));
    ifstream input;
    input.open(output);
    string pair;
    while(getline(input, pair)){
        string word = pair.substr(0, pair.rfind(" "));
        outputFiles[getHash(word)] << pair << '\n';
    }
    for(size_t i=0; i<numHashCodes; i++) outputFiles[i].close();
    input.close();
    remove(output.c_str());
    notifyServer(name, success);
  }

  alertServerOfProgress("Server says no more input chunks, so shutting down.");
}
