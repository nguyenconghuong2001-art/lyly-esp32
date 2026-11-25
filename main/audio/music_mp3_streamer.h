#ifndef LYLY_MUSIC_MP3_STREAMER_H
#define LYLY_MUSIC_MP3_STREAMER_H

#include <atomic>
#include <string>
#include <mutex>
#include <vector>

// LYLY-S3 only. Fetches MP3 from the configured LYLY music server.
// Play() returns when the task is launched, not when audio is playing.
// No concurrent streams. Stop() requests cancellation; the HTTP read may
// finish its current operation before the worker actually exits.
class MusicMp3Streamer {
public:
    static MusicMp3Streamer& Instance();

    bool PlaySongId(const std::string& song_id);
    bool PlayAll(bool shuffle);
    bool Next();
    bool Previous();
    bool Repeat();
    void Stop();
    bool IsRunning() const { return running_.load(); }

    MusicMp3Streamer(const MusicMp3Streamer&) = delete;
    MusicMp3Streamer& operator=(const MusicMp3Streamer&) = delete;

private:
    MusicMp3Streamer() = default;
    static void WorkerEntry(void* context);
    void Worker();
    bool StreamOne(const std::string& requested_id);
    bool StartList(std::vector<std::string> ids);
    void InterruptLocked();

    std::atomic<bool> running_{false};
    std::atomic<bool> cancel_{false};
    std::atomic<bool> stream_abort_{false}; // also stops decoder on transport failure
    std::mutex playlist_mutex_;
    std::vector<std::string> playlist_;
    size_t playlist_index_ = 0;
    bool restart_pending_ = false;
};

#endif // LYLY_MUSIC_MP3_STREAMER_H
