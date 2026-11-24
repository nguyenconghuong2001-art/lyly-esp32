#include "music_mp3_streamer.h"

#include "music_mp3_decoder.h"
#include "audio_service.h"
#include "application.h"
#include "board.h"
#include "lyly_music_config.h"

#include <cctype>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <esp_log.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

namespace {
constexpr char TAG[] = "LYLY_MP3_HTTP";
constexpr size_t kReadBytes = 4096;
constexpr size_t kMaxStreamBytes = 32U * 1024U * 1024U;
constexpr uint32_t kWorkerStackBytes = 12288;
constexpr int kMaxResumeAttempts = 3;

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
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return false;

    cancel_.store(false);
    song_id_ = song_id;
    if (xTaskCreate(WorkerEntry, "lyly_mp3", kWorkerStackBytes, this, 3, nullptr) != pdPASS) {
        running_.store(false);
        song_id_.clear();
        ESP_LOGE(TAG, "Unable to create streaming task");
        return false;
    }
    return true;
}

void MusicMp3Streamer::Stop() {
    cancel_.store(true);
    Application::GetInstance().GetAudioService().ClearMusicPlayback();
}

void MusicMp3Streamer::WorkerEntry(void* context) {
    static_cast<MusicMp3Streamer*>(context)->Worker();
    vTaskDelete(nullptr);
}

void MusicMp3Streamer::Worker() {
    bool completed = false;
    bool failed = false;
    size_t received = 0;
    size_t expected_bytes = 0;
    int resumed_connections = 0;
    const std::string requested_id = song_id_;

    do {
        auto& board = Board::GetInstance();
        auto* network = board.GetNetwork();
        auto* codec = board.GetAudioCodec();
        if (!network || !codec || cancel_.load()) {
            ESP_LOGW(TAG, "Cancelled or network/audio codec not ready");
            break;
        }

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
                                codec->output_sample_rate());
        if (!decoder.Open()) {
            ESP_LOGE(TAG, "Decoder open failed");
            http->Close();
            break;
        }

        uint8_t buffer[kReadBytes];
        size_t last_logged = 0;
        while (!cancel_.load()) {
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
                if (!decoder.Feed(buffer, chunk_bytes)) {
                    ESP_LOGE(TAG, "MP3 decoder/feed failed at byte %u",
                             static_cast<unsigned>(received));
                    failed = true;
                    break;
                }
                if (received - last_logged >= 64U * 1024U) {
                    ESP_LOGI(TAG, "MP3 progress received=%u expected=%u",
                             static_cast<unsigned>(received), static_cast<unsigned>(expected_bytes));
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

        if (!cancel_.load() && !failed &&
            (expected_bytes == 0 || received == expected_bytes)) {
            completed = decoder.Finish();
            if (!completed) ESP_LOGE(TAG, "MP3 decoder finish failed");
        } else if (!cancel_.load() && !failed) {
            failed = true;
            ESP_LOGE(TAG, "MP3 truncated: received=%u expected=%u",
                     static_cast<unsigned>(received), static_cast<unsigned>(expected_bytes));
        }
        http->Close();
        decoder.Close();
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
    song_id_.clear();
    running_.store(false);
}
