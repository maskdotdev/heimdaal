use crate::reviewer::SnapshotReader;

use super::protocol::JsonRpcError;
use super::types::{RunnerArtifact, RunnerArtifactView, RunnerRunResult};

#[derive(Debug, Clone)]
pub(crate) struct RunnerStoredRun {
    status: String,
    result: RunnerRunResult,
    redacted_artifacts: Vec<RunnerArtifact>,
    raw_artifacts: Vec<RunnerArtifact>,
    snapshot_readers: Vec<SnapshotReader>,
}

impl RunnerStoredRun {
    pub(crate) fn from_report(
        report: &crate::reviewer::RunReport,
        result: RunnerRunResult,
    ) -> Self {
        Self {
            status: result.status.clone(),
            result,
            redacted_artifacts: report
                .artifacts
                .list()
                .into_iter()
                .map(RunnerArtifact::from_artifact_view)
                .collect(),
            raw_artifacts: report
                .artifacts
                .list_raw()
                .into_iter()
                .map(RunnerArtifact::from_artifact_view)
                .collect(),
            snapshot_readers: report.snapshot_readers(),
        }
    }

    pub(crate) fn status(&self) -> &str {
        &self.status
    }

    pub(crate) fn result(&self) -> &RunnerRunResult {
        &self.result
    }

    pub(crate) fn artifact(
        &self,
        view: RunnerArtifactView,
        artifact_id: &str,
    ) -> Option<&RunnerArtifact> {
        self.artifacts(view)
            .iter()
            .find(|artifact| artifact.artifact_id == artifact_id)
    }

    pub(crate) fn artifacts(&self, view: RunnerArtifactView) -> &[RunnerArtifact] {
        match view {
            RunnerArtifactView::Redacted => &self.redacted_artifacts,
            RunnerArtifactView::Raw => &self.raw_artifacts,
        }
    }

    pub(crate) fn snapshot_reader(
        &self,
        snapshot_id: Option<&str>,
    ) -> Result<&SnapshotReader, JsonRpcError> {
        match snapshot_id {
            Some(snapshot_id) => self
                .snapshot_readers
                .iter()
                .find(|reader| reader.snapshot_id().0 == snapshot_id)
                .ok_or_else(|| {
                    JsonRpcError::invalid_params(format!("unknown snapshotId {snapshot_id}"))
                }),
            None if self.snapshot_readers.len() == 1 => Ok(&self.snapshot_readers[0]),
            None => Err(JsonRpcError::invalid_params(
                "snapshotId is required for multi-snapshot runs",
            )),
        }
    }
}
