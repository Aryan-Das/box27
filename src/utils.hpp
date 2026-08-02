#ifndef UTILS_HPP
#define UTILS_HPP

#include <vector>
#include <string>


std::vector<std::string_view> split(std::string_view text_view, std::string_view delimiter) {
    
    std::vector<std::string_view> tokens; 
    size_t start = 0;
    size_t end = text_view.find(delimiter);

    while (end != std::string_view::npos) {

        tokens.push_back(text_view.substr(start, end - start));
        

        start = end + delimiter.length();
        end = text_view.find(delimiter, start);
    }
    

    tokens.push_back(text_view.substr(start));
    return tokens;
}


#endif