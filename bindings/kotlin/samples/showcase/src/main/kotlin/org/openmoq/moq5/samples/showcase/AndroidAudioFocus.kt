package org.openmoq.moq5.samples.showcase

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.media.AudioAttributes
import android.media.AudioFocusRequest
import android.media.AudioManager
import android.os.Build

/** [AudioFocusPlatform] over AudioManager: a media/movie focus request and the noisy-output broadcast. */
class AndroidAudioFocus(private val context: Context) : AudioFocusPlatform {
    private val audio = context.getSystemService(AudioManager::class.java)
    private var request: AudioFocusRequest? = null
    private var receiver: BroadcastReceiver? = null

    override fun request(lost: (transient: Boolean) -> Unit): Boolean {
        val focus =
            AudioFocusRequest.Builder(AudioManager.AUDIOFOCUS_GAIN)
                .setAudioAttributes(
                    AudioAttributes.Builder()
                        .setUsage(AudioAttributes.USAGE_MEDIA)
                        .setContentType(AudioAttributes.CONTENT_TYPE_MOVIE)
                        .build(),
                )
                .setOnAudioFocusChangeListener { change ->
                    when (change) {
                        AudioManager.AUDIOFOCUS_LOSS -> lost(false)
                        AudioManager.AUDIOFOCUS_LOSS_TRANSIENT -> lost(true)
                    }
                }
                .build()
        val granted = audio.requestAudioFocus(focus) == AudioManager.AUDIOFOCUS_REQUEST_GRANTED
        // Only a granted request is ours to abandon later.
        request = if (granted) focus else null
        return granted
    }

    override fun abandon() {
        request?.let { audio.abandonAudioFocusRequest(it) }
        request = null
    }

    override fun listenNoisy(noisy: () -> Unit) {
        val listener =
            object : BroadcastReceiver() {
                override fun onReceive(context: Context, intent: Intent) {
                    if (intent.action == AudioManager.ACTION_AUDIO_BECOMING_NOISY) noisy()
                }
            }
        // A system broadcast reaches a receiver that is not exported; before API 33 there is no flag to give.
        val flags = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) Context.RECEIVER_NOT_EXPORTED else 0
        context.registerReceiver(listener, IntentFilter(AudioManager.ACTION_AUDIO_BECOMING_NOISY), flags)
        receiver = listener
    }

    override fun stopListeningNoisy() {
        receiver?.let { context.unregisterReceiver(it) }
        receiver = null
    }
}
