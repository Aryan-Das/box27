#ifndef MIME_HPP
#define MIME_HPP
#include <unordered_map>
#include <string>

static const std::unordered_map<std::string, std::string> mimeTypes = {
    {".html", "text/html"},
    {".htm",  "text/html"},
    {".css",  "text/css"},
    {".js",   "application/javascript"},
    {".json", "application/json"},
    {".txt",  "text/plain"},
    {".xml",  "application/xml"},
    {".pdf",  "application/pdf"},
    {".png",  "image/png"},
    {".jpg",  "image/jpeg"},
    {".jpeg", "image/jpeg"},
    {".gif",  "image/gif"},
    {".svg",  "image/svg+xml"},
    {".ico",  "image/x-icon"},
    {".mp4",  "video/mp4"},
    {".zip",  "application/zip"},
};
 
static const std::string defaultMimeType = "application/octet-stream";

std::string getMimeType(std::string path){
    size_t dotIndex = path.rfind(".");
    if(dotIndex == std::string::npos){
        return defaultMimeType;
    }
    std::string extension = path.substr(dotIndex);
    auto it = mimeTypes.find(extension);
    if (it != mimeTypes.end())
        return it->second;

    return defaultMimeType;
}

#endif