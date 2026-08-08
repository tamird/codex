use codex_protocol::openai_models::ModelPreset;
use std::sync::RwLock;

#[derive(Debug)]
pub(crate) struct ModelCatalogBusy;

/// Picker-ready model presets shared by the running TUI and its active `ChatWidget`.
///
/// The app-server remains the source of truth. Replacing this snapshot lets `/model` reflect
/// validated `config.toml` alias changes without reconstructing the TUI session.
#[derive(Debug)]
pub(crate) struct ModelCatalog {
    models: RwLock<Vec<ModelPreset>>,
}

impl ModelCatalog {
    pub(crate) fn new(models: Vec<ModelPreset>) -> Self {
        Self {
            models: RwLock::new(models),
        }
    }

    pub(crate) fn try_list_models(&self) -> Result<Vec<ModelPreset>, ModelCatalogBusy> {
        self.models
            .try_read()
            .map(|models| models.clone())
            .map_err(|_| ModelCatalogBusy)
    }

    pub(crate) fn replace_models(&self, models: Vec<ModelPreset>) {
        *self
            .models
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = models;
    }
}
