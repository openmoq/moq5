package org.openmoq.moq5.samples.showcase.ui

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.PlayArrow
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.AssistChip
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LargeTopAppBar
import androidx.compose.material3.ListItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.RadioButton
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.nestedscroll.nestedScroll
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import org.openmoq.moq5.samples.showcase.DraftChoice
import org.openmoq.moq5.samples.showcase.ShowcaseSettings
import org.openmoq.moq5.samples.showcase.StartChoice
import org.openmoq.moq5.samples.showcase.WatchTarget
import org.openmoq.moq5.samples.showcase.targetProblem

/** Where a viewer names a broadcast, or picks a recent one. */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ConnectScreen(
    url: String,
    namespace: String,
    settings: ShowcaseSettings,
    recents: List<WatchTarget>,
    onUrl: (String) -> Unit,
    onNamespace: (String) -> Unit,
    onWatch: (WatchTarget) -> Unit,
    onRemoveRecent: (WatchTarget) -> Unit,
    onSettings: (ShowcaseSettings) -> Unit,
) {
    var settingsOpen by rememberSaveable { mutableStateOf(false) }
    val scroll = TopAppBarDefaults.exitUntilCollapsedScrollBehavior()
    val problem = targetProblem(url.trim(), namespace.trim())
    val urlProblem = targetProblem(url.trim(), "x")
    val watch = { if (problem == null) onWatch(WatchTarget(url.trim(), namespace.trim())) }
    Scaffold(
        modifier = Modifier.nestedScroll(scroll.nestedScrollConnection),
        topBar = {
            LargeTopAppBar(
                title = { Text("MoQ5 Player") },
                actions = {
                    IconButton(onClick = { settingsOpen = true }) {
                        Icon(Icons.Filled.Settings, contentDescription = "Playback settings")
                    }
                },
                scrollBehavior = scroll,
            )
        },
    ) { padding ->
        LazyColumn(
            contentPadding = PaddingValues(
                start = 16.dp,
                end = 16.dp,
                top = padding.calculateTopPadding(),
                bottom = padding.calculateBottomPadding() + 24.dp,
            ),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            item {
                Text(
                    "Watch a live broadcast over Media over QUIC, decoded on this device.",
                    style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            item {
                OutlinedTextField(
                    value = url,
                    onValueChange = onUrl,
                    label = { Text("Relay") },
                    placeholder = { Text("https://relay.example.com:4433/moq") },
                    singleLine = true,
                    isError = url.length > "https://".length && urlProblem != null,
                    supportingText = { if (url.length > "https://".length && urlProblem != null) Text(urlProblem) },
                    keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri, imeAction = ImeAction.Next, autoCorrectEnabled = false),
                    modifier = Modifier.fillMaxWidth(),
                )
            }
            item {
                val namespaceProblem = if (urlProblem == null && namespace.isNotEmpty()) problem else null
                OutlinedTextField(
                    value = namespace,
                    onValueChange = onNamespace,
                    label = { Text("Broadcast namespace") },
                    placeholder = { Text("live/channel-1") },
                    singleLine = true,
                    isError = namespaceProblem != null,
                    supportingText = { if (namespaceProblem != null) Text(namespaceProblem) },
                    keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri, imeAction = ImeAction.Go, autoCorrectEnabled = false),
                    keyboardActions = KeyboardActions(onGo = { watch() }),
                    modifier = Modifier.fillMaxWidth(),
                )
            }
            item {
                Button(onClick = watch, enabled = problem == null, modifier = Modifier.fillMaxWidth()) {
                    Icon(Icons.Filled.PlayArrow, contentDescription = null)
                    Spacer(Modifier.width(8.dp))
                    Text("Watch")
                }
            }
            item {
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    AssistChip(
                        onClick = { settingsOpen = true },
                        label = { Text(if (settings.draft == DraftChoice.Automatic) "Automatic draft" else settings.draft.label) },
                    )
                    AssistChip(onClick = { settingsOpen = true }, label = { Text(settings.start.label) })
                }
            }
            if (recents.isNotEmpty()) {
                item {
                    Text(
                        "Recent",
                        style = MaterialTheme.typography.titleSmall,
                        color = MaterialTheme.colorScheme.primary,
                        modifier = Modifier.padding(top = 12.dp),
                    )
                }
                items(recents, key = { it.url + "\t" + it.namespace }) { target ->
                    RecentRow(target, onWatch = { onWatch(target) }, onRemove = { onRemoveRecent(target) })
                }
            }
        }
    }
    if (settingsOpen) SettingsSheet(settings, onSettings, onDismiss = { settingsOpen = false })
}

@Composable
private fun RecentRow(target: WatchTarget, onWatch: () -> Unit, onRemove: () -> Unit) {
    ListItem(
        headlineContent = { Text(target.title, maxLines = 1, overflow = TextOverflow.Ellipsis) },
        supportingContent = {
            Text(target.namespace + " · " + target.relay, maxLines = 1, overflow = TextOverflow.Ellipsis)
        },
        leadingContent = {
            Surface(shape = CircleShape, color = MaterialTheme.colorScheme.secondaryContainer, modifier = Modifier.size(40.dp)) {
                Box(contentAlignment = Alignment.Center) {
                    Icon(Icons.Filled.PlayArrow, contentDescription = null, tint = MaterialTheme.colorScheme.onSecondaryContainer)
                }
            }
        },
        trailingContent = {
            IconButton(onClick = onRemove) { Icon(Icons.Filled.Close, contentDescription = "Remove " + target.title) }
        },
        modifier = Modifier.clickable(onClick = onWatch),
    )
}

/** The playback settings, applied to the next session. */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SettingsSheet(settings: ShowcaseSettings, onChange: (ShowcaseSettings) -> Unit, onDismiss: () -> Unit) {
    ModalBottomSheet(onDismissRequest = onDismiss) {
        Column(Modifier.padding(start = 24.dp, end = 24.dp, bottom = 32.dp)) {
            Text("Playback", style = MaterialTheme.typography.titleLarge)
            SectionLabel("Draft")
            for (draft in DraftChoice.entries) {
                ChoiceRow(settings.draft == draft, draft.label, draftDetail(draft)) { onChange(settings.copy(draft = draft)) }
            }
            SectionLabel("Start")
            for (start in StartChoice.entries) {
                ChoiceRow(settings.start == start, start.label, start.detail) { onChange(settings.copy(start = start)) }
            }
            HorizontalDivider(Modifier.padding(vertical = 16.dp))
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable { onChange(settings.copy(showStats = !settings.showStats)) },
            ) {
                Column(Modifier.weight(1f)) {
                    Text("Statistics", style = MaterialTheme.typography.bodyLarge)
                    Text(
                        "Show the session and receiver statistics over the video",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                Switch(checked = settings.showStats, onCheckedChange = { onChange(settings.copy(showStats = it)) })
            }
        }
    }
}

@Composable
private fun SectionLabel(text: String) {
    Text(
        text,
        style = MaterialTheme.typography.titleSmall,
        color = MaterialTheme.colorScheme.primary,
        modifier = Modifier.padding(top = 16.dp, bottom = 4.dp),
    )
}

@Composable
private fun ChoiceRow(selected: Boolean, title: String, detail: String, onSelect: () -> Unit) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .selectable(selected = selected, onClick = onSelect, role = Role.RadioButton)
            .padding(vertical = 6.dp),
    ) {
        RadioButton(selected = selected, onClick = null)
        Column(Modifier.padding(start = 12.dp)) {
            Text(title, style = MaterialTheme.typography.bodyLarge)
            Text(detail, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        }
    }
}

private fun draftDetail(draft: DraftChoice): String =
    when (draft) {
        DraftChoice.Automatic -> "Offer every draft this build speaks; the relay chooses"
        DraftChoice.Draft16 -> "Offer draft-16 only"
        DraftChoice.Draft18 -> "Offer draft-18 only"
    }

@Preview
@Composable
private fun ConnectScreenPreview() {
    ShowcaseTheme {
        ConnectScreen(
            url = "https://relay.example.com:4433/moq",
            namespace = "live/channel-1",
            settings = ShowcaseSettings(),
            recents = listOf(
                WatchTarget("https://relay.example.com:4433/moq", "live/channel-1"),
                WatchTarget("moqt://relay.example.com:4433", "studio/news"),
            ),
            onUrl = {},
            onNamespace = {},
            onWatch = {},
            onRemoveRecent = {},
            onSettings = {},
        )
    }
}
