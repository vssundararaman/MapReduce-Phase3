//
// Created by sakkammadam on 11/6/22.
//

#include "ShufflerBase.hpp"
#include <iostream>
#include <string>
#include <filesystem>
#include <vector>
#include <map>
#include <fstream>

class Shuffler:public ShufflerBase{
public:
    // default constructor
    explicit Shuffler(){
        // Nothing here!
    }

    // explicit constructor
    explicit Shuffler(const std::string &mapper_directory): ShufflerBase(mapper_directory){
        // It should follow the initialization as ShufflerBase!
        // Nothing special here!
    }

    // Helper methods local to Shuffler!
    // This method will process a single partitioned Mapper file and produce an object in memory
    // The memory object is a map of the file (key) and its value is another that contains tokens
    // and their count within that partitioned file
    std::map<std::string, std::map<std::string,size_t>> shuffleFile(const std::string &fullyQualifiedFile){
        // Declare the file shuffle object
        std::map<std::string, std::map<std::string, size_t>> singlePartitionShuffle;
        // Check if the file is present in the file system
        std::filesystem::path p(fullyQualifiedFile);
        // Do the following only if the file exists in the file system!
        if(std::filesystem::exists(p)){
            std::cout << "Shuffling file - " << fullyQualifiedFile << std::endl;
            // Create temp objects
            std::map<std::string, size_t> tempShuffle;
            // Read the file as an input stream
            std::ifstream inputFile(fullyQualifiedFile);
            // Declare a string for each line in the file
            std::string fileLine;
            // Parse the file line by line!
            while(std::getline(inputFile, fileLine)){
                // Capturing the token using substr - it will extract from 2nd position upto the comma occurrence
                std::string parsedToken = fileLine.substr(1,fileLine.find(',')-1);
                // Let's create an iterator that will check tempShuffle for the token
                auto mapItr = tempShuffle.find(parsedToken);
                // Check if iterator was exhausted
                if (mapItr == tempShuffle.end()) {
                    // if parsedToken was not found! - lets insert into tempShuffle
                    tempShuffle.insert({parsedToken,1});
                } else {
                    // parsedToken was found! - let's increment the corresponding value by 1
                    mapItr->second = (mapItr->second)+1;
                }
            }
            // at this point, tempShuffle has the complete token counts of a partition file
            // lets create a new fully qualified path where we will replace "/temp_mapper/" with "/temp_shuffler/"
            std::string fullyQualifiedShuffleFile = fullyQualifiedFile;
            // the string we are trying to replace
            std::string toReplace("/temp_mapper/");
            // find its position within the file path
            size_t pos = fullyQualifiedShuffleFile.find(toReplace);
            // lets replace
            fullyQualifiedShuffleFile.replace(pos, toReplace.length(), "/temp_shuffler/");

            // we will simply attach it to the desired shuffle file and save the result to singlePartitionShuffle
            singlePartitionShuffle.insert({fullyQualifiedShuffleFile, tempShuffle});
        } else {
            std::cout << "** Warning! ** Input file: " << fullyQualifiedFile << "doesn't exist in the file system!" << std::endl;
        }
        // return the singlePartitionShuffle for downstream processing
        return singlePartitionShuffle;
    }

    // Primary method that will act on processed mapped files and create shuffled results in memory
    // Overarching method that will read subdirectories and associated partitioned files within temp_mapper directory
    // For each partition file within the file subdirectory, the shuffleFile method will be called
    virtual void runShuffleOperation(){
        // Declare an empty vector that will contain mapper temp subdirectory (filename) and token data as maps
        std::vector<std::map<std::string, std::map<std::string, size_t>>> directoryPartitionShuffle;
        // Let's create a vector that will hold all the files within the temp_mapper
        std::vector<std::string> partitionDirectoryFiles;
        // Let's iterate over the temp_mapper path
        for(const auto &entry: std::filesystem::directory_iterator(this->getMapOutputDirectory())){
            // Checking if the recursive path was a directory
            if(std::filesystem::is_regular_file(entry.path())){
                // if it's a file - push it into vector
                partitionDirectoryFiles.push_back(entry.path());
            }
        }

        // Create a vector of maps to hold all partition files for that subdirectory (File)
        std::vector<std::map<std::string, std::map<std::string, size_t>>> dirFileVector;

        // if there are files, we will iterate over each fileDirectory path
        if(!partitionDirectoryFiles.empty()) {
            // Let's iterate over each subdirectory named after a file
            for (const auto &partitionFile: partitionDirectoryFiles) {
                // Let's iterate over each partition file within the directory
                // We will create a map of partition file, and it's sorted tokens and their corresponding tokens
                std::map<std::string, std::map<std::string, size_t>> shuffledPartitionFile = this->shuffleFile(partitionFile);
                // We will then append each shuffled partition file to dirFileVector
                dirFileVector.push_back(shuffledPartitionFile);
            }
        } else {
            std::cout << "**Warning** directory path " << this->getMapOutputDirectory() << " is empty!" << std::endl;
        }
        // save object as private data member (shuffledOutput) for downstream processing!
        this->setShuffledOutput(dirFileVector);
    }

};

// Class factories - constructor
extern "C" ShufflerBase* createInputObj(const std::string &mapper_directory){
    return new Shuffler(mapper_directory);
}
// Class factories - destructor
extern "C" void removeInputObj(ShufflerBase* shObj){
    delete shObj;
}