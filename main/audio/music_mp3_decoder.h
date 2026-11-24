#ifndef LYLY_MUSIC_MP3_DECODER_H
#define LYLY_MUSIC_MP3_DECODER_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

class AudioService;

// V2.3: decoder for sequential MP3 byte chunks (not a URL fetcher).
// One instance belongs to one streaming task; never call Feed concurrently.
// Must be driven from a worker task, not the main application/event loop.
class MusicMp3Decoder {
public:
    MusicMp3Decoder(AudioService& audio, int output_sample_rate,
                    const std::atomic<bool>* cancel = nullptr);
    ~MusicMp3Decoder();

    MusicMp3Decoder(const MusicMp3Decoder&) = delete;
    MusicMp3Decoder& operator=(const MusicMp3Decoder&) = delete;

    bool Open();
    // Feed the complete chunk before advancing to the next chunk.
    bool Feed(const uint8_t* bytes, size_t length);
    bool Finish();
    void Close();

private:
    bool HandlePcm(const uint8_t* pcm, size_t length);
    bool AcceptSample(int16_t sample);
    bool FlushPcm();
    void ResetResampler();

    AudioService& audio_;
    int target_rate_;
    const std::atomic<bool>* cancel_;
    void* decoder_ = nullptr;
    std::vector<uint8_t> output_buffer_;
    std::vector<int16_t> pending_pcm_;
    int source_rate_ = 0;
    int source_channels_ = 0;
    int16_t previous_sample_ = 0;
    bool has_previous_ = false;
    uint64_t input_index_ = 0;
    uint64_t next_output_index_ = 0;
};

#endif // LYLY_MUSIC_MP3_DECODER_H
