package org.openmoq.moq5.samples.showcase.ui

import android.os.Build
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.dynamicDarkColorScheme
import androidx.compose.material3.dynamicLightColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext

/** Material 3 in the system's light or dark, with the wallpaper's colours from Android 12. */
@Composable
fun ShowcaseTheme(content: @Composable () -> Unit) {
    val dark = isSystemInDarkTheme()
    val context = LocalContext.current
    val scheme =
        when {
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.S -> if (dark) dynamicDarkColorScheme(context) else dynamicLightColorScheme(context)
            dark -> darkColorScheme(primary = Color(0xFF9FA8FF), secondary = Color(0xFF80CBC4))
            else -> lightColorScheme(primary = Color(0xFF3949AB), secondary = Color(0xFF00897B))
        }
    MaterialTheme(colorScheme = scheme, content = content)
}
