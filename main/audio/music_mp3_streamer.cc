#include "music_mp3_streamer.h"

#include "music_mp3_decoder.h"
#include "audio_service.h"
#include "application.h"
#include "board.h"
#include "lyly_music_config.h"

#include <cctype>
#include <cstdint>
#include <memory>

#include <esp_log.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

namespace {
constexpr char TAG[] = "LYLY_MP3_HTTP";
constexpr size_t kReadBytes = 2048;
constexpr size_t kMaxStreamBytes = 32U * 1024U * 1024U;
constexpr uint32_t kWorkerStackBytes = 12288;

// Never forward arbitrary model-provided URLs to the network stack.
// IDs come from the trusted LYLY music search response.
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
    do {
        auto& board = Board::GetInstance();
        auto* network = board.GetNetwork();
        auto* codec = board.GetAudioCodec();
        if (!network || !codec || cancel_.load()) break;

        auto http = network->CreateHttp(0);
        if (!http) {
            ESP_LOGE(TAG, "No HTTP client");
            break;
        }
        // Only the configured LYLY music host is permitted.
        const std::string url = std::string(LylyMusic::SERVER_URL) +
                                LylyMusic::STREAM_API + song_id_ + ".mp3";
        http->SetHeader("Accept", "audio/mpeg");
        if (!http->Open("GET", url)) {
            ESP_LOGE(TAG, "Could not open music stream");
            break;
        }
        if (http->GetStatusCode() != 200) {
            ESP_LOGW(TAG, "Music stream returned HTTP %d", http->GetStatusCode());
            http->Close();
            break;
        }

        MusicMp3Decoder decoder(Application::GetInstance().GetAudioService(),
                                codec->output_sample_rate());
        if (!decoder.Open()) {
            http->Close();
            break;
        }

        uint8_t buffer[kReadBytes];
        size_t received = 0;
        bool failed = false;
        while (!cancel_.load()) {
            const int n = http->Read(reinterpret_cast<char*>(buffer), sizeof(buffer));
            if (n < 0) {
                ESP_LOGE(TAG, "HTTP MP3 read failed: %d", n);
                failed = true;
                break;
            }
            if (n == 0) break; // HTTP body ended.
            received += static_cast<size_t>(n);
            if (received > kMaxStreamBytes) {
                ESP_LOGW(TAG, "MP3 exceeded streaming limit");
                failed = true;
                break;
            }
            if (cancel_.load() || !decoder.Feed(buffer, static_cast<size_t>(n))) {
                failed = true;
                break;
            }
        }
        if (!cancel_.load() && !failed) {
            completed = decoder.Finish();
        }
        http->Close();
        decoder.Close();
    } while (false);

    if (!completed) {
        ESP_LOGW(TAG, "Music stream ended early or was cancelled");
    } else {
        ESP_LOGI(TAG, "Finished MP3 stream");
    }
    // A Stop() call may have queued cancellation during the last decode.
    if (cancel_.load()) Application::GetInstance().GetAudioService().ClearMusicPlayback();
    song_id_.clear();
    running_.store(false);
}
