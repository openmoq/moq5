package org.openmoq.moq5.samples.showcase.ui

import android.app.Activity
import android.content.res.Configuration
import android.graphics.Rect
import android.graphics.SurfaceTexture
import android.os.Build
import android.view.TextureView
import android.view.View
import android.view.Window
import android.view.WindowInsetsController
import androidx.activity.compose.BackHandler
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.WindowInsetsSides
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.only
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawing
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.windowInsetsPadding
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material.icons.filled.Info
import androidx.compose.material.icons.filled.PlayArrow
import androidx.compose.material.icons.filled.Refresh
import androidx.compose.material.icons.filled.Warning
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ElevatedCard
import androidx.compose.material3.FilledIconButton
import androidx.compose.material3.FilledTonalIconButton
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.IconButtonDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.layout.boundsInWindow
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.platform.LocalConfiguration
import androidx.compose.ui.platform.LocalView
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import kotlinx.coroutines.delay
import org.openmoq.moq5.samples.playback.PlaybackProblem
import org.openmoq.moq5.samples.playback.PlaybackProblemKind
import org.openmoq.moq5.samples.showcase.PlayerPhase
import org.openmoq.moq5.samples.showcase.PlayerState
import org.openmoq.moq5.samples.showcase.label

/**
 * The player: the video, and over it the controls, the session's status
 * and, when enabled, its statistics. The video surface is the screen's first
 * child in every state, so rotation and picture-in-picture keep the same
 * TextureView (and the session that renders into it).
 */
@Composable
fun PlayerScreen(
    player: PlayerState,
    showStats: Boolean,
    inPip: Boolean,
    pipSupported: Boolean,
    onBack: () -> Unit,
    onPlay: () -> Unit,
    onStop: () -> Unit,
    onPip: () -> Unit,
    onToggleStats: () -> Unit,
    onNoticeShown: () -> Unit,
    onSurfaceAvailable: (SurfaceTexture) -> Unit,
    onSurfaceDestroyed: (SurfaceTexture) -> Boolean,
    onVideoBounds: (Rect) -> Unit,
) {
    BackHandler(onBack = onBack)
    var controls by rememberSaveable { mutableStateOf(true) }
    val playing = player.phase is PlayerPhase.Playing
    LaunchedEffect(controls, playing) {
        if (controls && playing) {
            delay(CONTROLS_SHOWN_MS)
            controls = false
        }
    }
    val landscape = LocalConfiguration.current.orientation == Configuration.ORIENTATION_LANDSCAPE
    SystemBarsHidden(inPip || (landscape && !controls))
    Box(
        Modifier
            .fillMaxSize()
            .background(Color.Black)
            .clickable(interactionSource = remember { MutableInteractionSource() }, indication = null) { controls = !controls },
    ) {
        VideoSurface(
            ratio = videoRatio(player),
            keepScreenOn = player.running,
            onAvailable = onSurfaceAvailable,
            onDestroyed = onSurfaceDestroyed,
            modifier = Modifier
                .align(Alignment.Center)
                .onGloballyPositioned { coordinates ->
                    val bounds = coordinates.boundsInWindow()
                    onVideoBounds(Rect(bounds.left.toInt(), bounds.top.toInt(), bounds.right.toInt(), bounds.bottom.toInt()))
                },
        )
        if (!inPip) {
            Status(player, onPlay, onBack, Modifier.align(Alignment.Center))
            AnimatedVisibility(controls, enter = fadeIn(), exit = fadeOut(), modifier = Modifier.fillMaxSize()) {
                Controls(player, pipSupported, onBack, onPlay, onStop, onPip, onToggleStats)
            }
            if (showStats && (player.version != null || player.stats != null)) {
                StatsPanel(
                    player,
                    Modifier
                        .align(Alignment.TopEnd)
                        .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Top + WindowInsetsSides.End))
                        .padding(top = 64.dp, end = 12.dp),
                )
            }
            player.notice?.let { notice ->
                LaunchedEffect(notice) {
                    delay(NOTICE_SHOWN_MS)
                    onNoticeShown()
                }
                Surface(
                    color = Color.Black.copy(alpha = 0.75f),
                    shape = RoundedCornerShape(50),
                    modifier = Modifier
                        .align(Alignment.BottomCenter)
                        .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Bottom))
                        .padding(bottom = 96.dp),
                ) {
                    Text(notice, color = Color.White, modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp))
                }
            }
        }
    }
}

@Composable
private fun VideoSurface(
    ratio: Float,
    keepScreenOn: Boolean,
    onAvailable: (SurfaceTexture) -> Unit,
    onDestroyed: (SurfaceTexture) -> Boolean,
    modifier: Modifier = Modifier,
) {
    val available by rememberUpdatedState(onAvailable)
    val destroyed by rememberUpdatedState(onDestroyed)
    AndroidView(
        factory = { context ->
            TextureView(context).apply {
                surfaceTextureListener =
                    object : TextureView.SurfaceTextureListener {
                        override fun onSurfaceTextureAvailable(surface: SurfaceTexture, width: Int, height: Int) = available(surface)

                        override fun onSurfaceTextureSizeChanged(surface: SurfaceTexture, width: Int, height: Int) = Unit

                        override fun onSurfaceTextureDestroyed(surface: SurfaceTexture): Boolean = destroyed(surface)

                        override fun onSurfaceTextureUpdated(surface: SurfaceTexture) = Unit
                    }
            }
        },
        update = { it.keepScreenOn = keepScreenOn },
        modifier = modifier.aspectRatio(ratio),
    )
}

@Composable
private fun Status(player: PlayerState, onPlay: () -> Unit, onBack: () -> Unit, modifier: Modifier) {
    when (val phase = player.phase) {
        PlayerPhase.Connecting -> Progress("Connecting to " + player.target.relay, null, modifier)
        PlayerPhase.Waiting -> Progress("Waiting for the broadcast", player.version?.label(), modifier)
        PlayerPhase.Playing -> Unit
        is PlayerPhase.Stopped ->
            Column(horizontalAlignment = Alignment.CenterHorizontally, modifier = modifier) {
                FilledIconButton(onClick = onPlay, modifier = Modifier.size(72.dp)) {
                    Icon(Icons.Filled.PlayArrow, contentDescription = "Play", modifier = Modifier.size(40.dp))
                }
                Text(
                    stoppedText(phase.reason),
                    color = Color.White.copy(alpha = 0.85f),
                    style = MaterialTheme.typography.bodyMedium,
                    modifier = Modifier.padding(top = 12.dp),
                )
            }
        is PlayerPhase.Failed -> FailureCard(phase.problem, onRetry = onPlay, onBack = onBack, modifier = modifier.padding(24.dp))
    }
}

@Composable
private fun Progress(title: String, detail: String?, modifier: Modifier) {
    Column(horizontalAlignment = Alignment.CenterHorizontally, modifier = modifier) {
        CircularProgressIndicator(color = Color.White)
        Text(title, color = Color.White, style = MaterialTheme.typography.titleMedium, modifier = Modifier.padding(top = 16.dp))
        if (detail != null) Text(detail, color = Color.White.copy(alpha = 0.7f), style = MaterialTheme.typography.bodySmall)
    }
}

/** A failed session, in plain words, with the way back in. */
@Composable
fun FailureCard(problem: PlaybackProblem, onRetry: () -> Unit, onBack: () -> Unit, modifier: Modifier = Modifier) {
    ElevatedCard(modifier = modifier.widthIn(max = 420.dp)) {
        Column(Modifier.padding(20.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Icon(Icons.Filled.Warning, contentDescription = null, tint = MaterialTheme.colorScheme.error)
                Spacer(Modifier.width(12.dp))
                Text(problemTitle(problem.kind), style = MaterialTheme.typography.titleMedium)
            }
            Text(problemHint(problem.kind), style = MaterialTheme.typography.bodyMedium, modifier = Modifier.padding(top = 8.dp))
            Text(
                problem.detail,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 4,
                overflow = TextOverflow.Ellipsis,
                modifier = Modifier.padding(top = 8.dp),
            )
            Row(horizontalArrangement = Arrangement.End, modifier = Modifier.fillMaxWidth().padding(top = 12.dp)) {
                TextButton(onClick = onBack) { Text("Back") }
                Spacer(Modifier.width(8.dp))
                Button(onClick = onRetry) {
                    Icon(Icons.Filled.Refresh, contentDescription = null)
                    Spacer(Modifier.width(8.dp))
                    Text("Try again")
                }
            }
        }
    }
}

@Composable
private fun Controls(
    player: PlayerState,
    pipSupported: Boolean,
    onBack: () -> Unit,
    onPlay: () -> Unit,
    onStop: () -> Unit,
    onPip: () -> Unit,
    onToggleStats: () -> Unit,
) {
    Box(Modifier.fillMaxSize()) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier
                .align(Alignment.TopCenter)
                .fillMaxWidth()
                .background(Brush.verticalGradient(listOf(Color.Black.copy(alpha = 0.7f), Color.Transparent)))
                .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Top + WindowInsetsSides.Horizontal))
                .padding(horizontal = 4.dp, vertical = 8.dp),
        ) {
            IconButton(onClick = onBack) {
                Icon(Icons.AutoMirrored.Filled.ArrowBack, contentDescription = "Back", tint = Color.White)
            }
            Column(Modifier.weight(1f)) {
                Text(player.target.title, color = Color.White, style = MaterialTheme.typography.titleMedium, maxLines = 1, overflow = TextOverflow.Ellipsis)
                Text(player.target.relay, color = Color.White.copy(alpha = 0.7f), style = MaterialTheme.typography.bodySmall, maxLines = 1)
            }
            if (player.phase is PlayerPhase.Playing) LiveBadge()
            IconButton(onClick = onToggleStats) {
                Icon(Icons.Filled.Info, contentDescription = "Statistics", tint = Color.White)
            }
        }
        Row(
            horizontalArrangement = Arrangement.spacedBy(24.dp, Alignment.CenterHorizontally),
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier
                .align(Alignment.BottomCenter)
                .fillMaxWidth()
                .background(Brush.verticalGradient(listOf(Color.Transparent, Color.Black.copy(alpha = 0.7f))))
                .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Bottom + WindowInsetsSides.Horizontal))
                .padding(16.dp),
        ) {
            val colors = IconButtonDefaults.filledTonalIconButtonColors(containerColor = Color.White.copy(alpha = 0.2f), contentColor = Color.White)
            when {
                // The failure card carries Try again; no second Play beneath it.
                player.phase is PlayerPhase.Failed -> Unit
                player.running ->
                    FilledTonalIconButton(onClick = onStop, colors = colors, modifier = Modifier.size(56.dp)) {
                        Icon(ShowcaseIcons.Stop, contentDescription = "Stop")
                    }
                else ->
                    FilledTonalIconButton(onClick = onPlay, colors = colors, modifier = Modifier.size(56.dp)) {
                        Icon(Icons.Filled.PlayArrow, contentDescription = "Play")
                    }
            }
            if (pipSupported && player.running) {
                FilledTonalIconButton(onClick = onPip, colors = colors, modifier = Modifier.size(56.dp)) {
                    Icon(ShowcaseIcons.PictureInPicture, contentDescription = "Picture in picture")
                }
            }
        }
    }
}

@Composable
private fun LiveBadge() {
    Surface(color = Color(0xFFE53935), shape = RoundedCornerShape(4.dp), modifier = Modifier.padding(end = 4.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(horizontal = 6.dp, vertical = 2.dp)) {
            Box(Modifier.size(6.dp).background(Color.White, CircleShape))
            Spacer(Modifier.width(4.dp))
            Text("LIVE", color = Color.White, style = MaterialTheme.typography.labelSmall, fontWeight = FontWeight.Bold)
        }
    }
}

@Composable
private fun StatsPanel(player: PlayerState, modifier: Modifier) {
    val lines = buildList {
        add("draft" to (player.version?.label() ?: "-"))
        player.video?.let { add("video" to "${it.width}×${it.height} ${it.mime.substringAfter('/')}") }
        player.audio?.let { add("audio" to "${it.sampleRate / 1000.0} kHz ${it.channels} ch ${it.mime.substringAfter('/')}") }
        player.stats?.let { s ->
            add("received" to s.objectsReceived.toString())
            add("queued" to "${s.objectsQueued} · ${s.bytesQueued / 1024u} KiB")
            add("dropped" to "${s.objectsDropped} obj · ${s.groupsDropped} grp")
            add("overflow" to s.overflowEvents.toString())
            add("gaps" to s.gapsReported.toString())
        }
    }
    Surface(color = Color.Black.copy(alpha = 0.6f), shape = RoundedCornerShape(12.dp), modifier = modifier) {
        Column(Modifier.padding(12.dp)) {
            for ((name, value) in lines) {
                Row {
                    Text(name, color = Color.White.copy(alpha = 0.6f), fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall, modifier = Modifier.width(88.dp))
                    Text(value, color = Color.White, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall, textAlign = TextAlign.Start)
                }
            }
        }
    }
}

/** Hide the status and navigation bars while [hidden]; a swipe shows them for a moment. */
@Composable
private fun SystemBarsHidden(hidden: Boolean) {
    val view = LocalView.current
    DisposableEffect(hidden) {
        val window = (view.context as? Activity)?.window
        if (window != null) systemBars(window, view, hidden)
        onDispose { if (window != null) systemBars(window, view, false) }
    }
}

private fun systemBars(window: Window, view: View, hidden: Boolean) {
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
        val controller = window.insetsController ?: return
        if (hidden) {
            controller.systemBarsBehavior = WindowInsetsController.BEHAVIOR_SHOW_TRANSIENT_BARS_BY_SWIPE
            controller.hide(android.view.WindowInsets.Type.systemBars())
        } else {
            controller.show(android.view.WindowInsets.Type.systemBars())
        }
    } else {
        @Suppress("DEPRECATION")
        view.systemUiVisibility =
            if (hidden) {
                View.SYSTEM_UI_FLAG_IMMERSIVE_STICKY or View.SYSTEM_UI_FLAG_FULLSCREEN or View.SYSTEM_UI_FLAG_HIDE_NAVIGATION or
                    View.SYSTEM_UI_FLAG_LAYOUT_STABLE or View.SYSTEM_UI_FLAG_LAYOUT_FULLSCREEN or View.SYSTEM_UI_FLAG_LAYOUT_HIDE_NAVIGATION
            } else {
                View.SYSTEM_UI_FLAG_LAYOUT_STABLE or View.SYSTEM_UI_FLAG_LAYOUT_FULLSCREEN or View.SYSTEM_UI_FLAG_LAYOUT_HIDE_NAVIGATION
            }
    }
}

private fun videoRatio(player: PlayerState): Float {
    val video = player.video ?: return 16f / 9f
    return if (video.width > 0 && video.height > 0) video.width.toFloat() / video.height else 16f / 9f
}

private fun stoppedText(reason: String?): String =
    when {
        reason == null -> "Stopped"
        reason.startsWith("ReceiverEnded") -> "The broadcast ended"
        else -> "Stopped: " + reason.substringBefore(';')
    }

private fun problemTitle(kind: PlaybackProblemKind): String =
    when (kind) {
        PlaybackProblemKind.Unreachable -> "Can't reach the relay"
        PlaybackProblemKind.TimedOut -> "The relay didn't answer"
        PlaybackProblemKind.Refused -> "The session was refused"
        PlaybackProblemKind.TrackFailed -> "A track stopped working"
        PlaybackProblemKind.Unsupported -> "This broadcast can't be played here"
        PlaybackProblemKind.Playback -> "Playback stopped"
        PlaybackProblemKind.Other -> "Something went wrong"
    }

private fun problemHint(kind: PlaybackProblemKind): String =
    when (kind) {
        PlaybackProblemKind.Unreachable -> "Check the relay address and your connection."
        PlaybackProblemKind.TimedOut -> "The relay may be busy or unreachable from this network."
        PlaybackProblemKind.Refused -> "The broadcast may have ended, or the relay refused this draft. Another draft in settings may work."
        PlaybackProblemKind.TrackFailed -> "The broadcast may have restarted. Try again."
        PlaybackProblemKind.Unsupported -> "This player decodes H.264 video with AAC audio."
        PlaybackProblemKind.Playback -> "Something in the media could not be played. Try again."
        PlaybackProblemKind.Other -> "Try again."
    }

private const val CONTROLS_SHOWN_MS = 3_000L
private const val NOTICE_SHOWN_MS = 4_000L
