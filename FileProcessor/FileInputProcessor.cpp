#include "FileProcessorBase.hpp"
#include <iostream>
#include <cmath>
#include <string>
#include <vector>
#include <map>
#include <filesystem>
#include <fstream>

class FileProcessorInput:public FileProcessorBase{
public:
    // Create new constructor
    FileProcessorInput(const std::string &operation, const std::string &path): FileProcessorBase(operation,path){
        // it should follow the initialization of the constructor call for FileProcessorBase!
        // nothing special inside
    }
    // Declare a helper method to calculate the number of lines in the file
    static int linesPerFile(const std::string &fileName){
        // Read the file as an input stream
        std::ifstream inputFile(fileName);
        // Initialize the total lines per file
        int numLines = 0;
        // declare a line variable
        std::string line;
        // Read the input file and find the total number of lines
        while(std::getline(inputFile, line)){
            ++numLines;
        }
        // Simple analysis
        std::cout << "The file " << fileName << " has " << numLines << " lines." << std::endl;
        return numLines;
    }
    // Declare a helper method to process a single file
    static std::vector<std::vector<std::string>> processFile(const std::string &input_file){
        // Declare a vector of vectors that contain strings - this is unique per file!
        std::vector<std::vector<std::string>> vecFile;
        // Read the file as an input stream
        std::ifstream inputFile(input_file);
        // Declare a string for each line in the file
        std::string fileLine;
        // Find the number of lines per file
        int numLinesPerFile = linesPerFile(input_file);
        // Set the number of lines per partition
        int numLinesPerPartition = 2000;
        // Determine number of partitions per file
        int numPartitionsPerFile = ceil(numLinesPerFile/double(numLinesPerPartition));
        std::cout << "Properties for " << input_file << std::endl;
        std::cout << "Number of lines: " << numLinesPerFile << std::endl;
        std::cout << "Number of lines per partition: " << numLinesPerPartition << std::endl;
        std::cout << "NUmber of partitions per file: " << numPartitionsPerFile << std::endl;
        // Represent the current line number
        int currentLine = 0;
        // Initialize partition
        int partition = 1;
        // Preemptively declare a vector of strings
        std::vector<std::string> simpleVec;
        // Parse the file line by line!
        while(std::getline(inputFile, fileLine)){
            // push each line into the vector of strings
            simpleVec.push_back(fileLine);
            // increment the currentLine variable
            ++currentLine;
            // if the lines falls outside the partition, then we need to push it to a new partition!
            if(currentLine >= (partition) * numLinesPerPartition && partition < numPartitionsPerFile){
                // increase the partition count
                ++partition;
                // push it to ioFiles (the vector of string vectors)
                vecFile.push_back(simpleVec);
                // clear the simple vector so that we can start covering for the next partition!
                simpleVec.clear();
            } else if (currentLine == numLinesPerFile && partition == numPartitionsPerFile){
                // this is to cover for the last partition!
                // push it to ioFiles (the vector of string vectors)
                vecFile.push_back(simpleVec);
            } else {
                // do nothing for all other lines
            }
        }
        // return for downstream processing
        return vecFile;
    }

    // Override virtual method from the base class
    virtual void runOperation() {
        std::cout << "Operation: " << this->getOperation() << std::endl;
        // std::cout << "Directory: " << this->getDirectoryPath() << std::endl;
        if (this->getOperation() == "input"){
            // Declare an empty map that will contain filenames and their corresponding file data as partitions
            std::map<std::string, std::vector<std::vector<std::string>>> directoryData;
            // Let's create a vector that holds the filenames within the directory
            std::vector<std::string> directoryFiles;
            // let's set up the directory path
            std::string filePath = this->getDirectoryPath();
            // check if filePath is directory or path to a file
            const std::filesystem::path basePath(filePath);
            if(std::filesystem::is_directory(basePath)){
                // let's find the files
                for(const auto &entry: std::filesystem::directory_iterator(filePath)){
                    std::cout << entry.path() << std::endl;
                    directoryFiles.push_back(entry.path());
                }
                // if there are no files -
                if(!directoryFiles.empty()){
                    // let's iterate each file!
                    for(auto i=0; i < directoryFiles.size(); i++){
                        // Declare a vector of vectors that contain strings - this is unique per file!
                        std::vector<std::vector<std::string>> vecFile = processFile(directoryFiles[i]);
                        // Insert into map - this contains all data broken down in partitions (lines) for one file!
                        directoryData.insert({directoryFiles[i], vecFile});
                    }
                } else {
                    std::cout << "**Warning** directory path " << this->getDirectoryPath() << " is empty!" << std::endl;
                }
            } else if (std::filesystem::is_regular_file(basePath)){
                // Declare a vector of vectors that contain strings - this is unique per file!
                std::vector<std::vector<std::string>> vecFile = processFile(filePath);
                // Insert into map - this contains all data broken down in partitions (lines) for one file!
                directoryData.insert({filePath, vecFile});
            } else {
                throw std::runtime_error("Unsupported operation encountered!!");
            }
            // Store it in the private data member!
            this->setInputDirectoryData(directoryData);
        }
    }
};

// Class factories - constructor
extern "C" FileProcessorBase* createInputObj(const std::string &operation, const std::string &path){
    return new FileProcessorInput(operation, path);
}

// Class factories - destructor
extern "C" void removeInputObj(FileProcessorBase* fpObj){
    delete fpObj;
}