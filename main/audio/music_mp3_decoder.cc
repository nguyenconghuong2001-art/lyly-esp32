#include "music_mp3_decoder.h"
#include "audio_service.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <esp_log.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

#include <esp_audio_dec_default.h>
#include <esp_audio_simple_dec.h>
#include <esp_audio_simple_dec_reg.h>

namespace {
constexpr char TAG[] = "LYLY_MP3";
constexpr size_t kInitialPcmBuffer = 8192;
constexpr size_t kMaximumPcmBuffer = 32768;
constexpr int kQueueRetryCount = 25;     // Bounded backpressure: 250 ms maximum.
constexpr TickType_t kQueueRetryTicks = pdMS_TO_TICKS(10) > 0 ? pdMS_TO_TICKS(10) : 1;
}

MusicMp3Decoder::MusicMp3Decoder(AudioService& audio, int output_sample_rate)
    : audio_(audio), target_rate_(output_sample_rate) {}

MusicMp3Decoder::~MusicMp3Decoder() { Close(); }

bool MusicMp3Decoder::Open() {
    if (decoder_) return true;
    if (target_rate_ < 8000 || target_rate_ > 48000) {
        ESP_LOGE(TAG, "Invalid output rate: %d", target_rate_);
        return false;
    }

    // This is a process-global registry. Keep it registered for the lifetime
    // of the firmware; unregistering would affect another audio decoder.
    static bool initialized = false;
    if (!initialized) {
        esp_audio_dec_register_default();
        esp_audio_simple_dec_register_default();
        initialized = true;
    }

    esp_audio_simple_dec_cfg_t cfg = {};
    cfg.dec_type = ESP_AUDIO_SIMPLE_DEC_TYPE_MP3;
    cfg.use_frame_dec = false;  // Accept arbitrary MP3 byte boundaries.
    if (esp_audio_simple_dec_open(&cfg, &decoder_) != ESP_AUDIO_ERR_OK) {
        ESP_LOGE(TAG, "Unable to open MP3 decoder (check MP3 support in sdkconfig)");
        decoder_ = nullptr;
        return false;
    }

    ResetResampler();
    pending_pcm_.reserve(static_cast<size_t>(target_rate_) * OPUS_FRAME_DURATION_MS / 1000);
    output_buffer_.resize(kInitialPcmBuffer);
    return true;
}

void MusicMp3Decoder::ResetResampler() {
    source_rate_ = 0;
    source_channels_ = 0;
    has_previous_ = false;
    previous_sample_ = 0;
    input_index_ = 0;
    next_output_index_ = 0;
    pending_pcm_.clear();
}

void MusicMp3Decoder::Close() {
    if (decoder_) {
        esp_audio_simple_dec_close(decoder_);
        decoder_ = nullptr;
    }
    output_buffer_.clear();
    ResetResampler();
}

bool MusicMp3Decoder::Feed(const uint8_t* bytes, size_t length) {
    if (!bytes || length == 0 || length > UINT32_MAX) return false;
    if (!decoder_ && !Open()) return false;

    esp_audio_simple_dec_raw_t raw = {};
    raw.buffer = const_cast<uint8_t*>(bytes); // Decoder treats input as read-only.
    raw.len = static_cast<uint32_t>(length);
    raw.eos = false;

    // Bound the iterations in case a decoder bug returns no progress.
    for (size_t iterations = 0; raw.len > 0 && iterations < 16384; ++iterations) {
        esp_audio_simple_dec_out_t frame = {};
        frame.buffer = output_buffer_.data();
        frame.len = static_cast<uint32_t>(output_buffer_.size());
        auto err = esp_audio_simple_dec_process(decoder_, &raw, &frame);

        if (err == ESP_AUDIO_ERR_BUFF_NOT_ENOUGH) {
            if (frame.needed_size <= output_buffer_.size() ||
                frame.needed_size > kMaximumPcmBuffer) {
                ESP_LOGE(TAG, "Unexpected MP3 output buffer request: %lu",
                         static_cast<unsigned long>(frame.needed_size));
                return false;
            }
            output_buffer_.resize(frame.needed_size);
            continue; // Retry with the same unconsumed input.
        }
        if (err != ESP_AUDIO_ERR_OK) {
            return false;
        }
        if (frame.decoded_size > 0 && !HandlePcm(frame.buffer, frame.decoded_size)) {
            return false;
        }
        if (raw.consumed > raw.len ||
            (raw.consumed == 0 && frame.decoded_size == 0)) {
            ESP_LOGE(TAG, "MP3 parser made no progress");
            return false;
        }
        raw.buffer += raw.consumed;
        raw.len -= raw.consumed;
    }
    return raw.len == 0;
}

bool MusicMp3Decoder::HandlePcm(const uint8_t* pcm, size_t length) {
    esp_audio_simple_dec_info_t info = {};
    if (esp_audio_simple_dec_get_info(decoder_, &info) != ESP_AUDIO_ERR_OK ||
        info.bits_per_sample != 16 || (info.channel != 1 && info.channel != 2) ||
        info.sample_rate < 8000 || info.sample_rate > 48000 ||
        length % (static_cast<size_t>(info.channel) * sizeof(int16_t)) != 0) {
        ESP_LOGE(TAG, "Unsupported MP3 PCM format");
        return false;
    }
    if (source_rate_ == 0) {
        source_rate_ = static_cast<int>(info.sample_rate);
        source_channels_ = info.channel;
        ESP_LOGI(TAG, "MP3 %d Hz / %d channels -> mono %d Hz",
                 source_rate_, source_channels_, target_rate_);
    } else if (source_rate_ != static_cast<int>(info.sample_rate) ||
               source_channels_ != info.channel) {
        ESP_LOGE(TAG, "MP3 audio format changed mid-stream");
        return false;
    }

    for (size_t i = 0; i < length; i += static_cast<size_t>(source_channels_) * 2) {
        const uint16_t lo = static_cast<uint16_t>(pcm[i]) | (static_cast<uint16_t>(pcm[i + 1]) << 8);
        int32_t mono = static_cast<int16_t>(lo);
        if (source_channels_ == 2) {
            const uint16_t ro = static_cast<uint16_t>(pcm[i + 2]) | (static_cast<uint16_t>(pcm[i + 3]) << 8);
            mono = (mono + static_cast<int16_t>(ro)) / 2;
        }
        if (!AcceptSample(static_cast<int16_t>(mono))) return false;
    }
    return true;
}

// Fractional linear resampling with continuous state across MP3 frames.
// No entire-song buffering; O(1) resampler memory + one 60-ms PCM frame.
bool MusicMp3Decoder::AcceptSample(int16_t sample) {
    const size_t max_frame = static_cast<size_t>(target_rate_) * OPUS_FRAME_DURATION_MS / 1000;
    if (max_frame == 0) return false;

    if (!has_previous_) {
        previous_sample_ = sample;
        has_previous_ = true;
        pending_pcm_.push_back(sample); // Output at t=0.
        next_output_index_ = 1;
        if (pending_pcm_.size() >= max_frame && !FlushPcm()) return false;
        return true;
    }

    ++input_index_;
    const uint64_t interval_start = (input_index_ - 1) * static_cast<uint64_t>(target_rate_);
    const uint64_t interval_end = input_index_ * static_cast<uint64_t>(target_rate_);

    while (next_output_index_ * static_cast<uint64_t>(source_rate_) <= interval_end) {
        const uint64_t output_time = next_output_index_ * static_cast<uint64_t>(source_rate_);
        const uint64_t numerator = output_time - interval_start;
        const int32_t interpolated = previous_sample_ + static_cast<int32_t>(
            (static_cast<int64_t>(sample - static_cast<int32_t>(previous_sample_)) *
             static_cast<int64_t>(numerator)) / target_rate_);
        pending_pcm_.push_back(static_cast<int16_t>(std::clamp(interpolated, -32768, 32767)));
        ++next_output_index_;
        if (pending_pcm_.size() >= max_frame && !FlushPcm()) return false;
    }
    previous_sample_ = sample;
    return true;
}

bool MusicMp3Decoder::FlushPcm() {
    if (pending_pcm_.empty()) return true;
    // AudioService enforces voice priority and its own queue capacity.
    // Do not wait forever when the assistant begins speaking.
    for (int attempt = 0; attempt < kQueueRetryCount; ++attempt) {
        if (audio_.QueueMusicPcm(std::move(pending_pcm_), target_rate_)) {
            pending_pcm_.clear();
            return true;
        }
        vTaskDelay(kQueueRetryTicks);
    }
    ESP_LOGW(TAG, "Music queue paused too long; cancel this stream");
    return false;
}

bool MusicMp3Decoder::Finish() {
    if (!decoder_) return false;
    // Drain a decoder's buffered tail without requiring more MP3 bytes.
    uint8_t dummy = 0;
    for (int i = 0; i < 8; ++i) {
        esp_audio_simple_dec_raw_t raw = {};
        raw.buffer = &dummy;
        raw.eos = true;
        esp_audio_simple_dec_out_t frame = {};
        frame.buffer = output_buffer_.data();
        frame.len = static_cast<uint32_t>(output_buffer_.size());
        const auto err = esp_audio_simple_dec_process(decoder_, &raw, &frame);
        if (err == ESP_AUDIO_ERR_BUFF_NOT_ENOUGH) {
            if (frame.needed_size <= output_buffer_.size() || frame.needed_size > kMaximumPcmBuffer) return false;
            output_buffer_.resize(frame.needed_size);
            --i;
            continue;
        }
        if (err != ESP_AUDIO_ERR_OK) break;
        if (frame.decoded_size == 0) break;
        if (!HandlePcm(frame.buffer, frame.decoded_size)) return false;
    }
    return FlushPcm();
}
