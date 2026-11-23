
#pragma once

#include "board.h"
#include "lyly_music_config.h"

#include <cJSON.h>
#include <string>

namespace LylyMusic {

// Encode Vietnamese song names for URLs
inline std::string UrlEncode(const std::string& text) {
    const char* hex = "0123456789ABCDEF";
    std::string result;

    for (unsigned char c : text) {
        if ((c >= 'A' && c <= 'Z') ||
            (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') ||
            c == '-' || c == '_' || c == '.') {
            result += static_cast<char>(c);
        } else {
            result += '%';
            result += hex[c >> 4];
            result += hex[c & 15];
        }
    }
    return result;
}

// Search songs on the LYLY Render server
inline std::string SearchSong(const std::string& title) {
    if (title.empty()) {
        return R"({"success":false,"error":"Empty title"})";
    }

    auto network = Board::GetInstance().GetNetwork();
    if (!network) {
        return R"({"success":false,"error":"No network"})";
    }

    auto http = network->CreateHttp(0);
    if (!http) {
        return R"({"success":false,"error":"HTTP unavailable"})";
    }

    std::string url =
        std::string(SERVER_URL) +
        SEARCH_API +
        UrlEncode(title);

    if (!http->Open("GET", url)) {
        return R"({"success":false,"error":"Connection failed"})";
    }

    int status = http->GetStatusCode();
    std::string response;

    if (status == 200) {
        response = http->ReadAll();
    }

    http->Close();

    if (status != 200) {
        return R"({"success":false,"error":"HTTP request failed"})";
    }

    if (response.size() > 8192) {
        return R"({"success":false,"error":"Response too large"})";
    }

    cJSON* root = cJSON_Parse(response.c_str());
    if (!root) {
        return R"({"success":false,"error":"Invalid JSON"})";
    }

    cJSON_Delete(root);
    return response;
}

} // namespace LylyMusic
