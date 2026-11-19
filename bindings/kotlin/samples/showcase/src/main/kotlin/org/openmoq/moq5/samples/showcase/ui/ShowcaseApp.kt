package org.openmoq.moq5.samples.showcase.ui

import android.graphics.Rect
import androidx.compose.runtime.Composable
import org.openmoq.moq5.samples.showcase.Screen
import org.openmoq.moq5.samples.showcase.ShowcaseUi
import org.openmoq.moq5.samples.showcase.ShowcaseViewModel

/** The app's two screens, wired to the model. */
@Composable
fun ShowcaseApp(
    ui: ShowcaseUi,
    model: ShowcaseViewModel,
    inPip: Boolean,
    pipSupported: Boolean,
    onPip: () -> Unit,
    onVideoBounds: (Rect) -> Unit,
) {
    when (val screen = ui.screen) {
        Screen.Connect ->
            ConnectScreen(
                url = ui.url,
                namespace = ui.namespace,
                settings = ui.settings,
                recents = ui.recents,
                onUrl = model::editUrl,
                onNamespace = model::editNamespace,
                onWatch = model::watch,
                onRemoveRecent = model::removeRecent,
                onSettings = model::changeSettings,
            )
        is Screen.Watching ->
            PlayerScreen(
                player = screen.player,
                showStats = ui.settings.showStats,
                inPip = inPip,
                pipSupported = pipSupported,
                onBack = model::leave,
                onPlay = model::play,
                onStop = model::stop,
                onPip = onPip,
                onToggleStats = { model.changeSettings(ui.settings.copy(showStats = !ui.settings.showStats)) },
                onNoticeShown = model::clearNotice,
                onSurfaceAvailable = model::surfaceAvailable,
                onSurfaceDestroyed = model::surfaceDestroyed,
                onVideoBounds = onVideoBounds,
            )
    }
}
