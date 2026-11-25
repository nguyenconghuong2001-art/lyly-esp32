#include "music_mp3_streamer.h"

#include "music_mp3_decoder.h"
#include "audio_service.h"
#include "application.h"
#include "board.h"
#include "lyly_music_config.h"

#include <algorithm>
#include <cstring>
#include <cctype>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <esp_log.h>
#include <esp_random.h>
#include <cJSON.h>
#include <esp_heap_caps.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

namespace {
constexpr char TAG[] = "LYLY_MP3_HTTP";
constexpr size_t kReadBytes = 4096;
constexpr size_t kMaxStreamBytes = 32U * 1024U * 1024U;
constexpr uint32_t kWorkerStackBytes = 12288;
constexpr int kMaxResumeAttempts = 3;
constexpr size_t kCacheBytes = 256U * 1024U;
constexpr size_t kCacheStartBytes = 48U * 1024U;
constexpr TickType_t kCachePollTicks = pdMS_TO_TICKS(10) > 0 ? pdMS_TO_TICKS(10) : 1;

// Single producer (HTTP) and single consumer (MP3 decoder). The monotonic
// positions publish copies only after they finish. Streams are limited to 32 MB,
// so positions cannot wrap on ESP32. The backing store is PSRAM, not DMA memory.
struct Mp3Cache {
    uint8_t* bytes;
    MusicMp3Decoder& decoder;
    std::atomic<bool>& abort;
    std::atomic<size_t> written{0};
    std::atomic<size_t> read{0};
    std::atomic<bool> input_done{false};
    std::atomic<bool> done{false};
    bool success = false; // read by producer only after done.load()

    size_t Available() const { const size_t r = read.load(); return written.load() - r; }

    bool Push(const uint8_t* src, size_t count) {
        while (count && !abort.load() && !done.load()) {
            const size_t w = written.load();
            const size_t space = kCacheBytes - (w - read.load());
            const size_t offset = w % kCacheBytes;
            const size_t n = std::min({count, space, kCacheBytes - offset});
            if (!n) { vTaskDelay(kCachePollTicks); continue; }
            std::memcpy(bytes + offset, src, n);
            written.store(w + n);
            src += n;
            count -= n;
        }
        return count == 0 && !abort.load() && !done.load();
    }

    static void DecodeEntry(void* arg) {
        auto& c = *static_cast<Mp3Cache*>(arg);
        c.Decode();
        c.done.store(true); // last access to the producer-owned context
        vTaskDelete(nullptr);
    }

    void Decode() {
        uint8_t chunk[kReadBytes];
        while (!abort.load() && !input_done.load() && Available() < kCacheStartBytes) {
            vTaskDelay(kCachePollTicks);
        }
        if (abort.load()) return;
        ESP_LOGI(TAG, "MP3 PSRAM prebuffer ready: %u bytes", static_cast<unsigned>(Available()));
        while (!abort.load()) {
            const size_t r = read.load();
            const size_t available = written.load() - r;
            if (!available) {
                if (input_done.load()) {
                    // Reload after observing EOF: the last publish may race the earlier read.
                    if (written.load() != r) continue;
                    success = decoder.Finish();
                    return;
                }
                vTaskDelay(kCachePollTicks);
                continue;
            }
            const size_t offset = r % kCacheBytes;
            const size_t n = std::min({available, sizeof(chunk), kCacheBytes - offset});
            std::memcpy(chunk, bytes + offset, n);
            read.store(r + n);
            if (!decoder.Feed(chunk, n)) {
                if (!abort.load()) ESP_LOGE(TAG, "MP3 cache decoder/feed failed");
                return;
            }
        }
    }
};


// Never forward arbitrary model-provided URLs to the network stack.
// Only song IDs from the configured LYLY music service are accepted.
bool ValidSongId(const std::string& id) {
    if (id.empty() || id.size() > 128) return false;
    for (unsigned char ch : id) {
        if (!((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
              (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' || ch == '.')) {
            return false;
        }
    }
    return true;
}
} // namespace

MusicMp3Streamer& MusicMp3Streamer::Instance() {
    static MusicMp3Streamer instance;
    return instance;
}

bool MusicMp3Streamer::PlaySongId(const std::string& song_id) {
    if (!ValidSongId(song_id)) return false;
    return StartList({song_id});
}

bool MusicMp3Streamer::PlayAll(bool shuffle) {
    auto* network = Board::GetInstance().GetNetwork();
    if (!network) return false;
    auto http = network->CreateHttp(0);
    if (!http) return false;
    if (!http->Open("GET", std::string(LylyMusic::SERVER_URL) + "/songs")) {
        http->Close();
        return false;
    }
    constexpr size_t kCatalogLimit = 64U * 1024U;
    std::string body;
    const size_t expected = http->GetBodyLength();
    bool ok = http->GetStatusCode() == 200 && expected <= kCatalogLimit;
    char chunk[1024];
    while (ok && (!expected || body.size() < expected)) {
        const int n = http->Read(chunk, sizeof(chunk));
        if (n == 0) break;
        if (n < 0 || body.size() + static_cast<size_t>(n) > kCatalogLimit) {
            ok = false;
            break;
        }
        body.append(chunk, static_cast<size_t>(n));
    }
    if (expected && body.size() != expected) ok = false;
    http->Close();
    if (!ok) return false;
    std::unique_ptr<cJSON, decltype(&cJSON_Delete)> root(cJSON_Parse(body.c_str()), cJSON_Delete);
    if (!root || !cJSON_IsArray(root.get())) return false;
    std::vector<std::string> ids;
    cJSON* item = nullptr;
    cJSON_ArrayForEach(item, root.get()) {
        const auto* id = cJSON_GetObjectItemCaseSensitive(item, "id");
        if (!cJSON_IsString(id) || !id->valuestring) return false;
        std::string value(id->valuestring);
        if (value == "lyly-test") continue; // Keep the speaker test out of playlists.
        if (!ValidSongId(value)) return false;
        if (std::find(ids.begin(), ids.end(), value) == ids.end()) ids.push_back(std::move(value));
        if (ids.size() > 500) return false;
    }
    root.reset();
    if (shuffle) {
        for (size_t i = ids.size(); i > 1; --i) {
            std::swap(ids[i - 1], ids[esp_random() % i]);
        }
    }
    ESP_LOGI(TAG, "LYLY playlist loaded: %u songs, shuffle=%d", static_cast<unsigned>(ids.size()), shuffle);
    return StartList(std::move(ids));
}

bool MusicMp3Streamer::StartList(std::vector<std::string> ids) {
    if (ids.empty()) return false;
    std::lock_guard<std::mutex> lock(playlist_mutex_);
    playlist_ = std::move(ids);
    playlist_index_ = 0;
    restart_pending_ = true;
    if (running_.load()) {
        InterruptLocked();
        return true;
    }
    running_.store(true);
    if (xTaskCreate(WorkerEntry, "lyly_mp3", kWorkerStackBytes, this, 3, nullptr) != pdPASS) {
        playlist_.clear();
        restart_pending_ = false;
        running_.store(false);
        ESP_LOGE(TAG, "Unable to create streaming task");
        return false;
    }
    return true;
}

void MusicMp3Streamer::InterruptLocked() {
    cancel_.store(true);
    stream_abort_.store(true);
    Application::GetInstance().GetAudioService().ClearMusicPlayback();
}

bool MusicMp3Streamer::Next() {
    std::lock_guard<std::mutex> lock(playlist_mutex_);
    if (!running_.load() || playlist_.empty() || playlist_index_ + 1 >= playlist_.size()) return false;
    ++playlist_index_;
    restart_pending_ = true;
    InterruptLocked();
    return true;
}

bool MusicMp3Streamer::Previous() {
    std::lock_guard<std::mutex> lock(playlist_mutex_);
    if (!running_.load() || playlist_.empty() || playlist_index_ == 0) return false;
    --playlist_index_;
    restart_pending_ = true;
    InterruptLocked();
    return true;
}

bool MusicMp3Streamer::Repeat() {
    std::lock_guard<std::mutex> lock(playlist_mutex_);
    if (!running_.load() || playlist_.empty()) return false;
    restart_pending_ = true;
    InterruptLocked();
    return true;
}

void MusicMp3Streamer::Stop() {
    std::lock_guard<std::mutex> lock(playlist_mutex_);
    playlist_.clear();
    restart_pending_ = false;
    InterruptLocked();
}

void MusicMp3Streamer::WorkerEntry(void* context) {
    static_cast<MusicMp3Streamer*>(context)->Worker();
    vTaskDelete(nullptr);
}

void MusicMp3Streamer::Worker() {
    while (true) {
        std::string id;
        {
            std::lock_guard<std::mutex> lock(playlist_mutex_);
            if (playlist_.empty() || playlist_index_ >= playlist_.size()) {
                playlist_.clear();
                running_.store(false);
                return;
            }
            id = playlist_[playlist_index_];
            restart_pending_ = false;
            cancel_.store(false);
            stream_abort_.store(false);
            ESP_LOGI(TAG, "LYLY playlist track %u/%u: %s", static_cast<unsigned>(playlist_index_ + 1),
                     static_cast<unsigned>(playlist_.size()), id.c_str());
        }
        const bool complete = StreamOne(id);
        // StreamOne ends when the decoder has finished, not when the final PCM
        // frame reaches the output. Drain that tail before starting a new track.
        auto& audio = Application::GetInstance().GetAudioService();
        while (complete && !cancel_.load() && !audio.IsMusicDrained()) {
            vTaskDelay(kCachePollTicks);
        }
        {
            std::lock_guard<std::mutex> lock(playlist_mutex_);
            if (restart_pending_) continue; // A newer user command wins.
            if (complete && !cancel_.load() && !playlist_.empty()) ++playlist_index_;
            else playlist_.clear(); // Stop on transport/decode failure, do not loop.
        }
    }
}

bool MusicMp3Streamer::StreamOne(const std::string& requested_id) {
    bool completed = false;
    bool failed = false;
    size_t received = 0;
    size_t expected_bytes = 0;
    int resumed_connections = 0;
    bool music_power_active = false;

    do {
        auto& board = Board::GetInstance();
        auto* network = board.GetNetwork();
        auto* codec = board.GetAudioCodec();
        if (!network || !codec || cancel_.load()) {
            ESP_LOGW(TAG, "Cancelled or network/audio codec not ready");
            break;
        }

        board.SetMusicStreamingMode(true);
        music_power_active = true;

        // Preserve the existing server, endpoint and song-ID trust boundary.
        const std::string url = std::string(LylyMusic::SERVER_URL) +
                                LylyMusic::STREAM_API + requested_id + ".mp3";
        auto http = network->CreateHttp(0);
        bool opened = false;
        ESP_LOGI(TAG, "Opening song id=%s", requested_id.c_str());
        for (int attempt = 1; attempt <= 3 && !cancel_.load(); ++attempt) {
            if (!http) http = network->CreateHttp(0);
            if (!http) {
                ESP_LOGE(TAG, "No HTTP client (attempt %d)", attempt);
            } else {
                http->SetHeader("Accept", "audio/mpeg");
                if (http->Open("GET", url)) {
                    opened = true;
                    break;
                }
                http->Close();
                http.reset();
                ESP_LOGW(TAG, "MP3 connect failed (attempt %d/3)", attempt);
            }
            if (attempt < 3) vTaskDelay(pdMS_TO_TICKS(500U * static_cast<unsigned>(attempt)));
        }
        if (!opened) {
            ESP_LOGE(TAG, "Could not open music stream after retries");
            break;
        }
        if (http->GetStatusCode() != 200) {
            ESP_LOGE(TAG, "Music stream HTTP %d", http->GetStatusCode());
            http->Close();
            break;
        }

        // The /media endpoint normally provides Content-Length. Do not
        // treat a truncated response as a successfully finished song.
        expected_bytes = http->GetBodyLength();
        ESP_LOGI(TAG, "MP3 HTTP OK; content_length=%u (0=unknown)",
                 static_cast<unsigned>(expected_bytes));
        if (expected_bytes > kMaxStreamBytes) {
            ESP_LOGE(TAG, "MP3 exceeds %u-byte limit", static_cast<unsigned>(kMaxStreamBytes));
            http->Close();
            break;
        }

        MusicMp3Decoder decoder(Application::GetInstance().GetAudioService(),
                                codec->output_sample_rate(), &stream_abort_);
        if (!decoder.Open()) {
            ESP_LOGE(TAG, "Decoder open failed");
            http->Close();
            break;
        }

        auto& audio = Application::GetInstance().GetAudioService();
        audio.BeginMusicPlayback();
        if (cancel_.load()) {
            audio.ClearMusicPlayback();
            http->Close();
            break;
        }
        uint8_t* cache_bytes = nullptr;
        if (heap_caps_get_free_size(MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT) >= 64U * 1024U &&
            heap_caps_get_largest_free_block(MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT) >= 16U * 1024U) {
            cache_bytes = static_cast<uint8_t*>(heap_caps_malloc(kCacheBytes, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT));
        }
        Mp3Cache cache{cache_bytes, decoder, stream_abort_};
        bool cached = false;
        if (cache_bytes) {
            cached = xTaskCreate(Mp3Cache::DecodeEntry, "lyly_mp3_dec", kWorkerStackBytes,
                                 &cache, 3, nullptr) == pdPASS;
            if (!cached) { heap_caps_free(cache_bytes); cache_bytes = nullptr; }
        }
        if (cached) {
            ESP_LOGI(TAG, "MP3 PSRAM cache enabled: %u bytes", static_cast<unsigned>(kCacheBytes));
        } else {
            ESP_LOGW(TAG, "MP3 cache unavailable; using direct decoder");
        }
        uint8_t buffer[kReadBytes];
        size_t last_logged = 0;
        while (!cancel_.load()) {
            if (cached && cache.done.load()) { failed = true; break; }
            // This check avoids waiting for EOF if all advertised bytes arrived.
            if (expected_bytes > 0 && received == expected_bytes) {
                ESP_LOGI(TAG, "Received full MP3 body (%u bytes)",
                         static_cast<unsigned>(received));
                break;
            }

            const int n = http->Read(reinterpret_cast<char*>(buffer), sizeof(buffer));
            if (n > 0) {
                const size_t chunk_bytes = static_cast<size_t>(n);
                if (received + chunk_bytes > kMaxStreamBytes ||
                    (expected_bytes > 0 && received + chunk_bytes > expected_bytes)) {
                    ESP_LOGE(TAG, "Unexpected MP3 response size: received=%u chunk=%u expected=%u",
                             static_cast<unsigned>(received), static_cast<unsigned>(chunk_bytes),
                             static_cast<unsigned>(expected_bytes));
                    failed = true;
                    break;
                }
                received += chunk_bytes;
                if (!(cached ? cache.Push(buffer, chunk_bytes) : decoder.Feed(buffer, chunk_bytes))) {
                    if (!cancel_.load()) {
                        ESP_LOGE(TAG, "MP3 decoder/feed failed at byte %u",
                                 static_cast<unsigned>(received));
                        failed = true;
                    }
                    break;
                }
                if (received - last_logged >= 64U * 1024U) {
                    ESP_LOGI(TAG, "MP3 progress received=%u expected=%u",
                             static_cast<unsigned>(received), static_cast<unsigned>(expected_bytes));
                    if (cached) ESP_LOGI(TAG, "MP3 PSRAM queued=%u bytes",
                                         static_cast<unsigned>(cache.Available()));
                    last_logged = received;
                }
                continue;
            }

            // If the response has no reliable Content-Length, EOF is the
            // only available end marker. Log this limitation for diagnosis.
            if (n == 0 && expected_bytes == 0) {
                ESP_LOGW(TAG, "HTTP EOF after %u bytes, Content-Length unavailable",
                         static_cast<unsigned>(received));
                break;
            }

            // Negative read = transport failure. A zero-byte read with fewer
            // than Content-Length bytes = premature EOF. Both are recoverable
            // only if the server honors HTTP Range requests.
            ESP_LOGW(TAG, "MP3 interrupted: read=%d received=%u expected=%u",
                     n, static_cast<unsigned>(received), static_cast<unsigned>(expected_bytes));
            if (received == 0 || expected_bytes == 0) {
                // Do not retry an unknown-length stream: replaying from byte 0
                // risks duplicate audio. The logs identify the failure.
                failed = true;
                break;
            }

            bool resumed = false;
            for (int attempt = 1; attempt <= kMaxResumeAttempts && !cancel_.load(); ++attempt) {
                http->Close();
                vTaskDelay(pdMS_TO_TICKS(500U * static_cast<unsigned>(attempt)));
                auto next = network->CreateHttp(0);
                if (!next) continue;
                next->SetHeader("Accept", "audio/mpeg");
                next->SetHeader("Range", "bytes=" + std::to_string(received) + "-");
                ESP_LOGW(TAG, "MP3 resume attempt=%d offset=%u",
                         attempt, static_cast<unsigned>(received));
                if (!next->Open("GET", url)) {
                    ESP_LOGW(TAG, "MP3 resume connection failed, attempt=%d", attempt);
                    continue;
                }
                const int status = next->GetStatusCode();
                if (status != 206) {
                    // HTTP 200 would resend byte zero and corrupt the decoder.
                    ESP_LOGE(TAG, "MP3 resume not supported: HTTP %d (expected 206)", status);
                    next->Close();
                    break;
                }
                const size_t remaining_bytes = next->GetBodyLength();
                if (remaining_bytes > 0 && remaining_bytes != expected_bytes - received) {
                    ESP_LOGE(TAG, "MP3 resume length mismatch: got=%u remaining=%u",
                             static_cast<unsigned>(remaining_bytes),
                             static_cast<unsigned>(expected_bytes - received));
                    next->Close();
                    break;
                }
                http = std::move(next);
                ++resumed_connections;
                resumed = true;
                ESP_LOGI(TAG, "MP3 resumed at byte=%u", static_cast<unsigned>(received));
                break;
            }
            if (!resumed) {
                failed = true;
                break;
            }
        }

        const bool body_complete = !cancel_.load() && !failed &&
                                   (expected_bytes == 0 || received == expected_bytes);
        if (cached) {
            if (!body_complete) stream_abort_.store(true);
            cache.input_done.store(true);
            http->Close();
            // The decoder may still be draining the cached MP3. Do not free
            // its context or release the playback slot until it has exited.
            while (!cache.done.load()) vTaskDelay(kCachePollTicks);
            completed = body_complete && !cancel_.load() && cache.success;
            heap_caps_free(cache_bytes);
        } else if (body_complete) {
            completed = decoder.Finish();
            if (!completed && !cancel_.load()) ESP_LOGE(TAG, "MP3 decoder finish failed");
        } else if (!cancel_.load() && !failed) {
            failed = true;
            ESP_LOGE(TAG, "MP3 truncated: received=%u expected=%u",
                     static_cast<unsigned>(received), static_cast<unsigned>(expected_bytes));
        }
        if (!cached) http->Close();
        decoder.Close();
        if (completed && !cancel_.load()) {
            audio.EndMusicPlayback();
        } else {
            audio.ClearMusicPlayback();
        }
    } while (false);

    if (cancel_.load()) {
        ESP_LOGW(TAG, "MP3 cancelled, received=%u expected=%u",
                 static_cast<unsigned>(received), static_cast<unsigned>(expected_bytes));
        Application::GetInstance().GetAudioService().ClearMusicPlayback();
    } else if (!completed) {
        ESP_LOGE(TAG, "MP3 FAILED id=%s received=%u expected=%u resumes=%d",
                 requested_id.c_str(), static_cast<unsigned>(received),
                 static_cast<unsigned>(expected_bytes), resumed_connections);
    } else {
        ESP_LOGI(TAG, "MP3 complete id=%s received=%u expected=%u resumes=%d",
                 requested_id.c_str(), static_cast<unsigned>(received),
                 static_cast<unsigned>(expected_bytes), resumed_connections);
    }
    if (music_power_active) Board::GetInstance().SetMusicStreamingMode(false);
    return completed && !cancel_.load();
}
