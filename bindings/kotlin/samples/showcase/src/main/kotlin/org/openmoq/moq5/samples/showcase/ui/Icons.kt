package org.openmoq.moq5.samples.showcase.ui

import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.graphics.vector.path
import androidx.compose.ui.unit.dp

/** The two glyphs the core icon set lacks, drawn on its 24-unit grid. */
internal object ShowcaseIcons {
    val Stop: ImageVector =
        ImageVector.Builder("Stop", 24.dp, 24.dp, 24f, 24f).apply {
            path(fill = SolidColor(Color.Black)) {
                moveTo(6f, 6f)
                lineTo(18f, 6f)
                lineTo(18f, 18f)
                lineTo(6f, 18f)
                close()
            }
        }.build()

    val PictureInPicture: ImageVector =
        ImageVector.Builder("PictureInPicture", 24.dp, 24.dp, 24f, 24f).apply {
            path(stroke = SolidColor(Color.Black), strokeLineWidth = 2f) {
                moveTo(3f, 5f)
                lineTo(21f, 5f)
                lineTo(21f, 19f)
                lineTo(3f, 19f)
                close()
            }
            path(fill = SolidColor(Color.Black)) {
                moveTo(12f, 11f)
                lineTo(19f, 11f)
                lineTo(19f, 17f)
                lineTo(12f, 17f)
                close()
            }
        }.build()
}
