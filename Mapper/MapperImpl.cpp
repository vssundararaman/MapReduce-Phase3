//
// Created by sakkammadam on 11/6/22.
//
#include "MapperBase.hpp"
#include <iostream>
#include <string>
#include <filesystem>
#include <sstream>
#include <vector>
#include <map>
#include <tuple>

class Mapper:public MapperBase{
public:
    // Default constructor
    Mapper(){
        // Nothing in the body of the default constructor
        // Note that we have set the private data members as blank!
    }
    // Explicit constructor
    explicit Mapper(
            const int partition_num,
            const std::map<std::string, std::vector<std::string>> &processed_file_partition): MapperBase(partition_num, processed_file_partition){
        // it should follow the initialization of the constructor call for MapperBase!
        // nothing special inside
    }
    // Cleanup methods -
    // 1) lowerRemovePunc - This method will lowercase and remove punctuations, essentially standardize a line
    // 2) tokenize - This method will tokenize a line and return a vector of tokens - expects a cleaned up line


    // Helpers! - local to MapperImpl.cpp only!
    // This method will lowercase and remove punctuations - standardize a line
    std::string lowerRemovePunc(const std::string &wholeString) {
        // declare a temporary string
        std::string temp;
        // let's iterate over the "wholeString" string container, a character at a time -
        for(auto character: wholeString){
            // if it's a newline - do nothing
            if(character == '\n'){
                // do nothing
            } else if(!std::ispunct(character)){
                // if it's not a punctuation character - add it to temp string
                temp += tolower(character, std::locale());
            } else {
                // adding for syntax - nothing should come here
            }
        }
        // return the temporary string
        return temp;
    }

    // This method will tokenize a line and return a vector of tokens within tuples- expects a standardized line
    // Each tuple will contain token, integer(1), partition# the originating line belonged to
    std::vector<std::tuple<std::string, int, int>> tokenize(const std::string &cleanedUpString, size_t linePartitionNum) {
        // default 1 integer
        int defOne = 1;
        // declare a vector containing maps of tokens
        std::vector<std::tuple<std::string, int, int>>  tokens;
        // leverage stringstream to read string as a stream which will be piped into another string using getline
        std::stringstream baseLine(cleanedUpString);
        // declare a token representing each word within a stream
        std::string token;
        // Using getline to split the baseLine string using whitespace as a delimiter
        while(getline(baseLine, token, ' ')){
            // empty check is to handle the edge case where there were multiple spaces
            if(!token.empty()){
                // declare a map
                std::tuple<std::string, int, int> tempTuple;
                // insert into tempTuple the token, 1, and originating line's partition number
                tempTuple = std::make_tuple(token, defOne, linePartitionNum);
                // push into the tokens vector
                tokens.push_back(tempTuple);
            }
        }
        // return tokens vector
        return tokens;
    }

    // Primary Map method
    // It will read a line and tokenize the line into individual words
    // The method will create a vector of tuples from a line
    // Each map will contain a word and the occurrence of that word (1)
    std::vector<std::tuple<std::string, int, int>> mapLine(const std::string &fileLine, size_t partitionNum){
        // let's cleanup the original string by creating a copy - see lowerRemovePunc method
        std::string cleanedString = lowerRemovePunc(fileLine);
        // let's create a vector containing maps of tokens from the cleaned up string
        std::vector<std::tuple<std::string, int, int>> stringTokens = tokenize(cleanedString, partitionNum);
        // return it so that it can be used later
        return stringTokens;
    }

    // Override virtual method from the base class - Overarching Mapper implementation - uses lowerRemovePunc, tokenize and mapLine
    virtual void runMapOperation(){
        // let's declare an output map
        std::map<std::string, std::vector<std::vector<std::tuple<std::string, int, int>>>> outputMapperData;
        // let's invoke the private data member
        for(const auto &fileData: getProcessedFilePartition()){
            std::cout << "Mapping partition#" << this->getPartitionNum() << " data from: " << fileData.first << std::endl;
            // now we are going to read ALL the data associated for a particular file's partition
            std::vector<std::vector<std::tuple<std::string, int, int>>> partitionVector;
            // Now for each partition, we are going to read each line!
            for(size_t line=0; line < fileData.second.size(); ++line){
                // here we will call the mapLine method to clean and tokenize each line
                std::vector<std::tuple<std::string, int, int>> line2Tokens = mapLine(fileData.second[line],this->getPartitionNum());
                // push it to the partition vector
                partitionVector.push_back(line2Tokens);
            }
            // Insert the file name and associated vector spaces into Output mapper
            outputMapperData.insert({fileData.first, partitionVector});
        }
        // save outputMapperData within the private data member (mapperOutput)
        this->setMapperOutputData(outputMapperData);
    }
};

// Class factories - constructor
extern "C" MapperBase* createInputObj(
        const int partitionNum,
        const std::map<std::string, std::vector<std::string>> &inputPartition){
    return new Mapper(partitionNum, inputPartition);
}

// Class factories - destructor
extern "C" void removeInputObj(MapperBase* mpObj){
    delete mpObj;
}