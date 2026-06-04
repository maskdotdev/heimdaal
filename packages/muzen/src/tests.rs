use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::bench::bench_job;
use crate::cli::{
    BenchArgs, BenchTerminalPolicy, CanaryManifestArgs, CanaryVerifyArgs, Cli, Command,
};
use crate::contracts::*;
use crate::events::{EventEmitter, EventEmitterState};
use crate::repo::RepoContext;
use crate::reviewer::capabilities_from_mask;
use crate::runtime::bench::optimization_failures;
use crate::runtime::contracts::{
    ArtifactId as ConcurrentArtifactId, ArtifactKey, CacheInfo, CacheStatus, CapabilitySet,
    ConcurrentCounters, ConcurrentRunReport, ConversationItem, FsScope, LimitInfo,
    ModelCostEstimate, ModelToolCall, ModelTurn, ProviderResourceId, ProviderResourceScope,
    RepoPath, RuntimeError, RuntimeLimits, RuntimeResult, SessionId, SessionScope, SnapshotId,
    ToolCallId, ToolEffects, ToolErrorCode, ToolGrant, ToolId, ToolMetricKey,
    ToolProviderHealthState, ToolProviderId, TurnId,
};
use crate::runtime::dispatch::RuntimeEventDispatcher;
use crate::runtime::job_runtime::{
    benchmark_failures as runtime_benchmark_failures, JobRuntime, SessionSpec,
};
use crate::runtime::model::{
    openai_provider_canary_protocols, run_openai_provider_canaries, ConcurrentModelClient,
    EnvCredentialResolver, MockReviewModel, ModelProviderCanaryEvidence,
    OpenAiProviderCanaryConfig, StaticModelRouter,
};
use crate::runtime::policy::ReviewerPolicy;
use crate::runtime::repo::RepoSnapshot;
use crate::runtime::tools::ToolEngine;
use crate::runtime::tools::{
    CustomToolArtifact, CustomToolContext, CustomToolHandler, CustomToolOptions, CustomToolOutput,
    JsonRpcToolRequest, JsonRpcToolResponse, JsonRpcToolTransport, ToolRegistry,
};
use crate::util::DEFAULT_MODEL;
use async_trait::async_trait;

#[cfg(test)]
mod suite {
    use super::*;
    use clap::Parser;

    #[test]
    fn concurrent_transcript_stores_artifact_refs_not_content() {
        let item = ConversationItem::ToolResult {
            call_id: ToolCallId("tool-1".to_string()),
            name: ToolId::from(ToolName::ReadFile),
            content: Box::new(crate::runtime::contracts::ToolResultEnvelope {
                ok: true,
                tool_call_id: ToolCallId("tool-1".to_string()),
                tool_name: ToolId::from(ToolName::ReadFile),
                provider_id: crate::reviewer::tool_adapters::ToolProviderId::builtin_review(),
                snapshot_id: SnapshotId("snapshot-1".to_string()),
                artifact_id: Some(ConcurrentArtifactId("artifact-1".to_string())),
                cache: CacheInfo {
                    status: CacheStatus::Miss,
                    key_hash: None,
                },
                limits: LimitInfo::default(),
                data: None,
                error: None,
            }),
        };

        let serialized = serde_json::to_string(&item).unwrap();
        assert!(serialized.contains("artifact-1"));
        assert!(!serialized.contains("full file content"));
    }

    #[test]
    fn path_policy_blocks_parent_escape() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello").unwrap();
        let repo = test_repo(temp.path());
        let escaped = repo.normalize_tool_path(Path::new("../outside"));
        assert!(escaped.is_err());
    }

    #[test]
    fn path_policy_blocks_dot_git() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join(".git")).unwrap();
        fs::write(temp.path().join(".git/config"), "secret").unwrap();
        let repo = test_repo(temp.path());
        let denied = repo.normalize_tool_path(Path::new(".git/config"));
        assert!(denied.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn path_policy_blocks_symlink_escape() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), "secret").unwrap();
        symlink(
            outside.path().join("secret.txt"),
            temp.path().join("link.txt"),
        )
        .unwrap();
        let repo = test_repo(temp.path());
        let files = repo.walk_files().unwrap();
        assert!(!files.iter().any(|path| path == Path::new("link.txt")));
    }

    #[test]
    fn benchmark_gate_requires_real_tools() {
        let mut report = ConcurrentRunReport {
            runtime: "concurrent",
            sessions: 10,
            completed_sessions: 10,
            model_calls: 10,
            tool_calls: 0,
            tool_counts: ToolCounts::default(),
            findings: 0,
            publishable_findings: 0,
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            elapsed_ms: 0,
            artifacts: 0,
            artifact_bytes: 0,
            counters: ConcurrentCounters::default(),
            tool_metrics: Default::default(),
            provider_health: Vec::new(),
            snapshot_metrics: Vec::new(),
            model_metrics: Default::default(),
            terminal_diagnostics: Vec::new(),
            benchmark_valid: false,
            benchmark_failures: Vec::new(),
        };
        report.benchmark_failures = runtime_benchmark_failures(&report);
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("read_file/read_head_file")));
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("read_diff")));
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("search_text")));
    }

    #[test]
    fn optimization_gate_flags_measured_regressions() {
        let mut baseline = minimal_report();
        baseline.elapsed_ms = 100;
        baseline.counters.search_scans = 2;
        let mut concurrent = minimal_report();
        concurrent.elapsed_ms = 500;
        concurrent.counters.search_scans = 3;

        let failures = optimization_failures(&baseline, &concurrent, 0.2);
        assert!(failures
            .iter()
            .any(|failure| failure.contains("scanned more batches")));
        assert!(failures
            .iter()
            .any(|failure| failure.contains("4x baseline")));
        assert!(failures
            .iter()
            .any(|failure| failure.contains("speedup below floor")));
        assert!(optimization_failures(&baseline, &baseline, 1.0).is_empty());
    }

    #[test]
    fn concurrent_report_exports_publishable_finding_count() {
        let report = ConcurrentRunReport {
            runtime: "concurrent",
            sessions: 1,
            completed_sessions: 1,
            model_calls: 1,
            tool_calls: 1,
            tool_counts: ToolCounts::default(),
            findings: 1,
            publishable_findings: 1,
            elapsed_ms: 1,
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            artifacts: 1,
            artifact_bytes: 1,
            counters: ConcurrentCounters::default(),
            tool_metrics: Default::default(),
            provider_health: Vec::new(),
            snapshot_metrics: Vec::new(),
            model_metrics: Default::default(),
            terminal_diagnostics: Vec::new(),
            benchmark_valid: true,
            benchmark_failures: Vec::new(),
        };

        let value = serde_json::to_value(report).unwrap();
        assert_eq!(value["findings"], 1);
        assert_eq!(value["publishableFindings"], 1);
    }

    #[test]
    fn bench_terminal_policy_controls_finish_tool() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "benchmark repo").unwrap();

        let normal_job = bench_job(&bench_args(temp.path(), BenchTerminalPolicy::Normal)).unwrap();
        assert!(normal_job
            .personas
            .iter()
            .all(|persona| persona.allowed_tools.finish));
        assert!(normal_job
            .personas
            .iter()
            .all(|persona| persona.allowed_tools.record_finding));

        let finding_required_job = bench_job(&bench_args(
            temp.path(),
            BenchTerminalPolicy::FindingRequired,
        ))
        .unwrap();
        assert!(finding_required_job
            .personas
            .iter()
            .all(|persona| !persona.allowed_tools.finish));
        assert!(finding_required_job
            .personas
            .iter()
            .all(|persona| persona.allowed_tools.record_finding));
        assert!(finding_required_job
            .personas
            .iter()
            .all(|persona| persona.objective.contains("record_finding exactly once")));
    }

    #[test]
    fn run_and_bench_have_no_runtime_selector() {
        let run_cli = Cli::parse_from(["muzen", "run", "--job", "job.json"]);
        match run_cli.command {
            Command::Run(args) => assert_eq!(args.job, PathBuf::from("job.json")),
            _ => panic!("expected run command"),
        }

        let bench_cli = Cli::parse_from(["muzen", "bench"]);
        match bench_cli.command {
            Command::Bench(args) => assert_eq!(args.sessions, 10),
            _ => panic!("expected bench command"),
        }

        let canary_cli = Cli::parse_from([
            "muzen",
            "canary-manifest",
            "--provider-evidence",
            "provider.json",
            "--remote-object-store-evidence",
            "snapshot.json",
            "--remote-object-store-evidence",
            "artifact.json",
            "--output",
            "manifest.json",
            "--max-evidence-age-seconds",
            "3600",
        ]);
        match canary_cli.command {
            Command::CanaryManifest(args) => {
                assert_eq!(args.provider_evidence, PathBuf::from("provider.json"));
                assert_eq!(
                    args.remote_object_store_evidence,
                    vec![
                        PathBuf::from("snapshot.json"),
                        PathBuf::from("artifact.json")
                    ]
                );
                assert_eq!(args.output, Some(PathBuf::from("manifest.json")));
                assert_eq!(args.max_evidence_age_seconds, 3600);
            }
            _ => panic!("expected canary-manifest command"),
        }

        let verify_cli = Cli::parse_from([
            "muzen",
            "canary-verify",
            "--manifest",
            "manifest.json",
            "--max-evidence-age-seconds",
            "3600",
        ]);
        match verify_cli.command {
            Command::CanaryVerify(args) => {
                assert_eq!(args.manifest, PathBuf::from("manifest.json"));
                assert_eq!(args.max_evidence_age_seconds, 3600);
            }
            _ => panic!("expected canary-verify command"),
        }

        let status_cli = Cli::parse_from([
            "muzen",
            "canary-status",
            "--manifest",
            "manifest.json",
            "--output",
            "status.json",
            "--max-evidence-age-seconds",
            "3600",
        ]);
        match status_cli.command {
            Command::CanaryStatus(args) => {
                assert_eq!(args.manifest, PathBuf::from("manifest.json"));
                assert_eq!(args.output, Some(PathBuf::from("status.json")));
                assert_eq!(args.max_evidence_age_seconds, 3600);
            }
            _ => panic!("expected canary-status command"),
        }

        let workflow_cli = Cli::parse_from([
            "muzen",
            "canary-workflow-provenance",
            "--output",
            "workflow.json",
        ]);
        match workflow_cli.command {
            Command::CanaryWorkflowProvenance(args) => {
                assert_eq!(args.output, Some(PathBuf::from("workflow.json")));
            }
            _ => panic!("expected canary-workflow-provenance command"),
        }

        let publish_cli = Cli::parse_from([
            "muzen",
            "canary-publish",
            "--output-dir",
            "canaries",
            "--provider-evidence",
            "provider.json",
            "--snapshot-base-uri",
            "memory://snapshots/canary",
            "--artifact-base-uri",
            "memory://artifacts/canary",
            "--object-store-driver",
            "memory",
            "--max-evidence-age-seconds",
            "3600",
        ]);
        match publish_cli.command {
            Command::CanaryPublish(args) => {
                assert_eq!(args.output_dir, PathBuf::from("canaries"));
                assert_eq!(args.provider_evidence, Some(PathBuf::from("provider.json")));
                assert_eq!(args.snapshot_base_uri, "memory://snapshots/canary");
                assert_eq!(args.artifact_base_uri, "memory://artifacts/canary");
                assert_eq!(
                    args.object_store_driver,
                    crate::cli::RemoteObjectStoreCanaryDriver::Memory
                );
                assert_eq!(args.max_evidence_age_seconds, 3600);
            }
            _ => panic!("expected canary-publish command"),
        }

        let preflight_cli = Cli::parse_from([
            "muzen",
            "canary-preflight",
            "--output-dir",
            "canaries",
            "--snapshot-base-uri",
            "https://snapshots.example.test/canary",
            "--artifact-base-uri",
            "https://artifacts.example.test/canary",
            "--object-store-driver",
            "http",
            "--provider-base-url",
            "https://api.example.test/v1",
            "--model",
            "canary-model",
        ]);
        match preflight_cli.command {
            Command::CanaryPreflight(args) => {
                assert_eq!(args.output_dir, PathBuf::from("canaries"));
                assert_eq!(
                    args.provider_base_url,
                    Some("https://api.example.test/v1".to_string())
                );
                assert_eq!(
                    args.object_store_driver,
                    crate::cli::RemoteObjectStoreCanaryDriver::Http
                );
                assert_eq!(args.model, "canary-model");
            }
            _ => panic!("expected canary-preflight command"),
        }

        let proof_cli = Cli::parse_from([
            "muzen",
            "canary-proof",
            "--evidence-dir",
            "canaries",
            "--output",
            "proof.json",
            "--max-evidence-age-seconds",
            "3600",
        ]);
        match proof_cli.command {
            Command::CanaryProof(args) => {
                assert_eq!(args.evidence_dir, PathBuf::from("canaries"));
                assert_eq!(args.output, Some(PathBuf::from("proof.json")));
                assert_eq!(args.max_evidence_age_seconds, 3600);
                assert_eq!(args.expected_workflow, "Muzen Canary Evidence");
                assert_eq!(args.expected_job, "publish-canary-evidence");
                assert_eq!(args.expected_repository, None);
                assert_eq!(args.expected_git_ref, None);
            }
            _ => panic!("expected canary-proof command"),
        }

        let proof_override_cli = Cli::parse_from([
            "muzen",
            "canary-proof",
            "--evidence-dir",
            "canaries",
            "--expected-workflow",
            "Custom Canary",
            "--expected-job",
            "custom-job",
            "--expected-repository",
            "heimdaal/review",
            "--expected-git-ref",
            "refs/heads/main",
        ]);
        match proof_override_cli.command {
            Command::CanaryProof(args) => {
                assert_eq!(args.expected_workflow, "Custom Canary");
                assert_eq!(args.expected_job, "custom-job");
                assert_eq!(
                    args.expected_repository,
                    Some("heimdaal/review".to_string())
                );
                assert_eq!(args.expected_git_ref, Some("refs/heads/main".to_string()));
            }
            _ => panic!("expected canary-proof command"),
        }
    }

    #[test]
    fn public_reviewer_facade_runs_mock_review() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-1",
                "head-1",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        )
        .with_path_policy(crate::reviewer::SnapshotPathPolicy::standard(64 * 1024, 20));
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "public-session",
            crate::reviewer::Role::Generalist,
            "Run through the public reviewer facade.",
            public_budget(),
        );
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "public-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let events = Arc::new(crate::reviewer::InMemoryReviewEventSink::default());
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicFacadeModel {
                path: "README.md".to_string(),
                query: "needle".to_string(),
            }))
            .review_event_sink(events.clone())
            .build()
            .unwrap();
        fs::write(temp.path().join("README.md"), "mutated needle\n").unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());

        assert_eq!(report.run_id, "public-run");
        assert_eq!(report.summary.status, "completed");
        assert_eq!(report.summary.snapshot_count, 1);
        let manifests = report.snapshot_manifests();
        assert_eq!(manifests.len(), 1);
        let manifest = &manifests[0];
        assert_eq!(manifest.file_count, 1);
        assert_eq!(manifest.changed_file_count, 1);
        assert_eq!(manifest.captured_text_file_count, 1);
        assert!(manifest.captured_text_bytes > 0);
        assert!(!manifest.manifest_hash.is_empty());
        assert!(!manifest.path_policy_hash.is_empty());
        assert_eq!(manifest.changed_files.len(), 1);
        assert_eq!(manifest.changed_files[0].path.display(), "README.md");
        let readme = manifest
            .files
            .iter()
            .find(|file| file.path.display() == "README.md")
            .expect("README.md in public snapshot manifest");
        assert!(readme.captured);
        assert!(readme.is_changed);
        assert!(readme.is_text_candidate);
        assert!(readme.content_hash.is_some());
        let reader = report
            .snapshot_reader(&report.snapshot.snapshot_id)
            .expect("snapshot reader");
        let snapshot_text = reader.read_text_path("README.md", 64 * 1024).unwrap();
        assert_eq!(snapshot_text.path.display(), "README.md");
        assert!(snapshot_text.content.contains("needle"));
        assert!(!snapshot_text.content.contains("mutated"));
        assert_eq!(
            snapshot_text.content_hash,
            readme.content_hash.clone().unwrap()
        );
        assert_eq!(report.summary.completed_sessions, 1);
        assert_eq!(report.summary.findings, 1);
        let findings = report.findings();
        assert_eq!(findings.len(), 1);
        assert!(findings[0].publishable);
        assert!(findings[0].evidence_count > 0);
        let redacted_artifact_policy = crate::reviewer::ArtifactExportPolicy::redacted_all();
        let mut raw_artifact_capabilities =
            crate::reviewer::capabilities::CapabilitySet::review_read_only();
        raw_artifact_capabilities.artifact_access.read_raw = true;
        let raw_artifact_policy =
            crate::reviewer::ArtifactExportPolicy::raw(&raw_artifact_capabilities).unwrap();
        let evidence_artifacts = report
            .finding_evidence_artifacts(&findings[0].id, redacted_artifact_policy.clone())
            .unwrap();
        assert_eq!(evidence_artifacts.len(), findings[0].evidence_count);
        for evidence_artifact in &evidence_artifacts {
            assert!(!evidence_artifact.evidence.evidence_id.is_empty());
            assert_eq!(
                evidence_artifact.evidence.artifact_id(),
                evidence_artifact.artifact.artifact_id()
            );
            assert_eq!(
                evidence_artifact.evidence.content_hash,
                evidence_artifact.artifact.content_hash
            );
            assert!(!evidence_artifact.artifact.content.is_empty());
        }
        let scoped_artifact_id = evidence_artifacts[0].artifact_id().to_string();
        let scoped_artifact_policy =
            crate::reviewer::ArtifactExportPolicy::redacted_artifacts(
                [scoped_artifact_id.as_str()],
            );
        let scoped_evidence_artifacts = report
            .finding_evidence_artifacts(&findings[0].id, scoped_artifact_policy.clone())
            .unwrap();
        assert_eq!(scoped_evidence_artifacts.len(), 1);
        assert_eq!(
            scoped_evidence_artifacts[0].artifact.artifact_id(),
            scoped_artifact_id
        );
        let scoped_export = report
            .export_artifacts(scoped_artifact_policy.clone())
            .unwrap();
        assert_eq!(scoped_export.artifact_count, 1);
        assert_eq!(
            scoped_export.first_artifact_id(),
            Some(scoped_artifact_id.as_str())
        );
        let retained_scoped_artifact_policy = scoped_artifact_policy
            .clone()
            .with_retention_policy(crate::reviewer::ArtifactRetentionPolicy::max_artifacts(1));
        let retained_scoped_export = report
            .export_artifacts(retained_scoped_artifact_policy.clone())
            .unwrap();
        assert_eq!(retained_scoped_export.artifact_count, 1);
        assert_eq!(
            retained_scoped_export.retention,
            crate::reviewer::ArtifactRetentionPolicy::max_artifacts(1)
        );
        let retained_scoped_evidence = report
            .finding_evidence_artifacts(&findings[0].id, retained_scoped_artifact_policy)
            .unwrap();
        assert_eq!(retained_scoped_evidence.len(), 1);
        let memory_artifact_store = crate::reviewer::InMemoryArtifactObjectStore::default();
        let retained_memory_policy = scoped_artifact_policy
            .clone()
            .with_retention_policy(crate::reviewer::ArtifactRetentionPolicy::max_artifacts(1));
        let retained_memory_manifest = report
            .persist_artifacts(&memory_artifact_store, retained_memory_policy)
            .unwrap();
        assert_eq!(retained_memory_manifest.artifact_count, 1);
        assert_eq!(
            retained_memory_manifest.total_bytes,
            scoped_export.total_bytes
        );
        assert_eq!(
            retained_memory_manifest.retention,
            crate::reviewer::ArtifactRetentionPolicy::max_artifacts(1)
        );
        assert_eq!(memory_artifact_store.object_count(), 1);
        let memory_object = &retained_memory_manifest.objects[0];
        assert_eq!(
            memory_object.view,
            crate::reviewer::ArtifactViewMode::Redacted
        );
        assert_eq!(memory_object.artifact_id(), scoped_artifact_id);
        assert!(memory_object.path.is_none());
        assert_eq!(
            memory_artifact_store
                .read(&memory_object.uri)
                .unwrap()
                .len(),
            memory_object.bytes
        );
        let memory_validation = retained_memory_manifest
            .validate_storage(&memory_artifact_store)
            .unwrap();
        assert!(memory_validation.valid);
        assert_eq!(memory_validation.expected_objects, 1);
        assert_eq!(memory_validation.checked_objects, 1);
        assert_eq!(memory_validation.checked_bytes, memory_object.bytes);
        let memory_cleanup = retained_memory_manifest
            .cleanup_storage(&memory_artifact_store)
            .unwrap();
        assert_eq!(memory_cleanup.expected_objects, 1);
        assert_eq!(memory_cleanup.checked_objects, 1);
        assert_eq!(memory_cleanup.removed_objects.len(), 1);
        assert_eq!(memory_cleanup.removed_bytes, memory_object.bytes);
        assert!(memory_cleanup.missing_objects.is_empty());
        assert!(memory_cleanup.stale_objects.is_empty());
        assert_eq!(memory_artifact_store.object_count(), 0);
        assert!(
            !retained_memory_manifest
                .validate_storage(&memory_artifact_store)
                .unwrap()
                .valid
        );
        assert!(!report.artifacts.list().is_empty());
        let export = report
            .export_artifacts(redacted_artifact_policy.clone())
            .unwrap();
        assert_eq!(export.view, crate::reviewer::ArtifactViewMode::Redacted);
        assert_eq!(
            export.retention,
            crate::reviewer::ArtifactRetentionPolicy::unlimited()
        );
        assert_eq!(export.artifact_count, report.artifacts.list().len());
        assert!(export.total_bytes > 0);
        let no_artifacts_policy = redacted_artifact_policy
            .clone()
            .with_retention_policy(crate::reviewer::ArtifactRetentionPolicy::max_artifacts(0));
        assert!(matches!(
            report.export_artifacts(no_artifacts_policy.clone()),
            Err(crate::reviewer::runtime::RuntimeError::LimitExceeded {
                kind: "artifact_retention_artifacts"
            })
        ));
        assert!(matches!(
            report.finding_evidence_artifacts(&findings[0].id, no_artifacts_policy.clone()),
            Err(crate::reviewer::runtime::RuntimeError::LimitExceeded {
                kind: "artifact_retention_artifacts"
            })
        ));
        let too_few_bytes_policy = redacted_artifact_policy.clone().with_retention_policy(
            crate::reviewer::ArtifactRetentionPolicy::max_bytes(export.total_bytes - 1),
        );
        assert!(matches!(
            report.export_artifacts(too_few_bytes_policy.clone()),
            Err(crate::reviewer::runtime::RuntimeError::LimitExceeded {
                kind: "artifact_retention_bytes"
            })
        ));
        let rejected_memory_store = crate::reviewer::InMemoryArtifactObjectStore::default();
        assert!(matches!(
            report.persist_artifacts(&rejected_memory_store, too_few_bytes_policy.clone()),
            Err(crate::reviewer::runtime::RuntimeError::LimitExceeded {
                kind: "artifact_retention_bytes"
            })
        ));
        assert_eq!(rejected_memory_store.object_count(), 0);
        let rejected_bundle_dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            report.export_artifact_bundle(rejected_bundle_dir.path(), too_few_bytes_policy),
            Err(crate::reviewer::runtime::RuntimeError::LimitExceeded {
                kind: "artifact_retention_bytes"
            })
        ));
        assert!(!rejected_bundle_dir.path().join("manifest.json").exists());
        assert!(!rejected_bundle_dir.path().join("artifacts").exists());
        assert!(crate::reviewer::ArtifactExportPolicy::raw(
            &crate::reviewer::capabilities::CapabilitySet::review_read_only()
        )
        .is_err());
        let raw_export = report
            .export_artifacts(raw_artifact_policy.clone())
            .unwrap();
        assert_eq!(raw_export.view, crate::reviewer::ArtifactViewMode::Raw);
        assert_eq!(raw_export.artifact_count, export.artifact_count);
        assert!(raw_export.total_bytes > 0);
        let local_artifact_dir = tempfile::tempdir().unwrap();
        let local_artifact_store =
            crate::reviewer::LocalArtifactObjectStore::new(local_artifact_dir.path().to_path_buf());
        let local_manifest = report
            .persist_artifacts(&local_artifact_store, redacted_artifact_policy.clone())
            .unwrap();
        assert_eq!(
            local_manifest.view,
            crate::reviewer::ArtifactViewMode::Redacted
        );
        assert_eq!(local_manifest.artifact_count, export.artifact_count);
        assert_eq!(local_manifest.total_bytes, export.total_bytes);
        for object in &local_manifest.objects {
            let path = object.path.as_ref().expect("local object path");
            assert!(path.starts_with(local_artifact_store.root()));
            assert!(path.exists());
            assert_eq!(fs::metadata(path).unwrap().len() as usize, object.bytes);
            let exported_artifact = export
                .artifacts
                .iter()
                .find(|artifact| artifact.artifact_id() == object.artifact_id())
                .expect("persisted artifact in export manifest");
            assert_eq!(object.content_hash, exported_artifact.content_hash);
            assert_eq!(fs::read_to_string(path).unwrap(), exported_artifact.content);
        }
        let serialized_local_manifest = serde_json::to_string_pretty(&local_manifest).unwrap();
        let restored_local_manifest: crate::reviewer::ArtifactPersistenceManifest =
            serde_json::from_str(&serialized_local_manifest).unwrap();
        let reopened_local_store =
            crate::reviewer::LocalArtifactObjectStore::new(local_artifact_dir.path().to_path_buf());
        let local_validation = restored_local_manifest
            .validate_storage(&reopened_local_store)
            .unwrap();
        assert!(local_validation.valid);
        assert_eq!(
            local_validation.expected_objects,
            restored_local_manifest.artifact_count
        );
        assert_eq!(
            local_validation.expected_bytes,
            restored_local_manifest.total_bytes
        );
        assert_eq!(
            local_validation.checked_objects,
            restored_local_manifest.artifact_count
        );
        assert_eq!(
            local_validation.checked_bytes,
            restored_local_manifest.total_bytes
        );
        let first_object = restored_local_manifest.objects[0].clone();
        let first_path = first_object
            .path
            .as_ref()
            .expect("local object path")
            .clone();
        fs::write(&first_path, "stale artifact object").unwrap();
        let stale_validation = restored_local_manifest
            .validate_storage(&reopened_local_store)
            .unwrap();
        assert!(!stale_validation.valid);
        assert!(stale_validation.missing_objects.is_empty());
        assert!(stale_validation
            .stale_objects
            .iter()
            .any(|object| object.artifact_id() == first_object.artifact_id()));
        fs::remove_file(&first_path).unwrap();
        let missing_validation = restored_local_manifest
            .validate_storage(&reopened_local_store)
            .unwrap();
        assert!(!missing_validation.valid);
        assert!(missing_validation
            .missing_objects
            .iter()
            .any(|object| object.artifact_id() == first_object.artifact_id()));
        let local_cleanup = restored_local_manifest
            .cleanup_storage(&reopened_local_store)
            .unwrap();
        assert_eq!(
            local_cleanup.expected_objects,
            restored_local_manifest.artifact_count
        );
        assert_eq!(
            local_cleanup.expected_bytes,
            restored_local_manifest.total_bytes
        );
        assert!(local_cleanup
            .missing_objects
            .iter()
            .any(|object| object.artifact_id() == first_object.artifact_id()));
        assert!(!local_cleanup.removed_objects.is_empty());
        assert!(
            !restored_local_manifest
                .validate_storage(&reopened_local_store)
                .unwrap()
                .valid
        );
        let remote_artifact_client =
            Arc::new(crate::reviewer::InMemoryRemoteArtifactObjectClient::default());
        let remote_artifact_store = crate::reviewer::RemoteArtifactObjectStore::new(
            "s3://muzen-test-artifacts/public-run/",
            remote_artifact_client.clone(),
        )
        .unwrap();
        let remote_manifest = report
            .persist_artifacts(&remote_artifact_store, redacted_artifact_policy.clone())
            .unwrap();
        assert_eq!(
            remote_manifest.view,
            crate::reviewer::ArtifactViewMode::Redacted
        );
        assert_eq!(remote_manifest.artifact_count, export.artifact_count);
        assert_eq!(remote_manifest.total_bytes, export.total_bytes);
        assert_eq!(
            remote_artifact_client.object_count(),
            remote_manifest.artifact_count
        );
        for object in &remote_manifest.objects {
            assert!(object.path.is_none());
            assert!(object.uri.starts_with(&format!(
                "{}/artifacts/redacted/",
                remote_artifact_store.base_uri()
            )));
            assert!(object.uri.ends_with(".txt"));
            assert_eq!(
                remote_artifact_client.read(&object.uri).unwrap().len(),
                object.bytes
            );
        }
        let serialized_remote_manifest = serde_json::to_string_pretty(&remote_manifest).unwrap();
        let restored_remote_manifest: crate::reviewer::ArtifactPersistenceManifest =
            serde_json::from_str(&serialized_remote_manifest).unwrap();
        let remote_validation = restored_remote_manifest
            .validate_storage(&remote_artifact_store)
            .unwrap();
        assert!(remote_validation.valid);
        assert_eq!(
            remote_validation.checked_objects,
            restored_remote_manifest.artifact_count
        );
        assert_eq!(
            remote_validation.checked_bytes,
            restored_remote_manifest.total_bytes
        );
        let first_remote_object = restored_remote_manifest.objects[0].clone();
        remote_artifact_client.write(
            first_remote_object.uri.clone(),
            b"stale remote artifact".to_vec(),
        );
        let stale_remote_validation = restored_remote_manifest
            .validate_storage(&remote_artifact_store)
            .unwrap();
        assert!(!stale_remote_validation.valid);
        assert!(stale_remote_validation.missing_objects.is_empty());
        assert!(stale_remote_validation
            .stale_objects
            .iter()
            .any(|object| object.artifact_id() == first_remote_object.artifact_id()));
        remote_artifact_client.remove(&first_remote_object.uri);
        let missing_remote_validation = restored_remote_manifest
            .validate_storage(&remote_artifact_store)
            .unwrap();
        assert!(!missing_remote_validation.valid);
        assert!(missing_remote_validation
            .missing_objects
            .iter()
            .any(|object| object.artifact_id() == first_remote_object.artifact_id()));
        let mut forged_remote_object = first_remote_object.clone();
        forged_remote_object.uri = forged_remote_object
            .uri
            .replace(remote_artifact_store.base_uri(), "s3://forged-bucket");
        assert!(matches!(
            crate::reviewer::ArtifactObjectReader::read_artifact_object(
                &remote_artifact_store,
                &forged_remote_object
            ),
            Err(crate::reviewer::runtime::RuntimeError::RepoAccessDenied)
        ));
        let remote_cleanup = restored_remote_manifest
            .cleanup_storage(&remote_artifact_store)
            .unwrap();
        assert_eq!(
            remote_cleanup.expected_objects,
            restored_remote_manifest.artifact_count
        );
        assert!(remote_cleanup
            .missing_objects
            .iter()
            .any(|object| object.artifact_id() == first_remote_object.artifact_id()));
        assert_eq!(remote_artifact_client.object_count(), 0);
        assert!(
            !restored_remote_manifest
                .validate_storage(&remote_artifact_store)
                .unwrap()
                .valid
        );
        assert!(crate::reviewer::RemoteArtifactObjectStore::new(
            "file:///tmp/muzen-artifacts",
            remote_artifact_client.clone()
        )
        .is_err());
        let bundle_dir = tempfile::tempdir().unwrap();
        let bundle = report
            .export_artifact_bundle(bundle_dir.path(), redacted_artifact_policy)
            .unwrap();
        assert_eq!(bundle.view, crate::reviewer::ArtifactViewMode::Redacted);
        assert_eq!(bundle.artifact_count, export.artifact_count);
        assert_eq!(bundle.total_bytes, export.total_bytes);
        assert_eq!(bundle.root, bundle_dir.path());
        assert_eq!(
            bundle.retention,
            crate::reviewer::ArtifactRetentionPolicy::unlimited()
        );
        assert!(bundle.manifest_path.exists());
        let manifest: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&bundle.manifest_path).unwrap()).unwrap();
        assert_eq!(
            manifest["artifactCount"].as_u64(),
            Some(export.artifact_count as u64)
        );
        assert_eq!(manifest["view"].as_str(), Some("redacted"));
        assert_eq!(
            manifest["retention"]["maxArtifacts"],
            serde_json::Value::Null
        );
        assert_eq!(manifest["retention"]["maxBytes"], serde_json::Value::Null);
        for entry in &bundle.artifacts {
            let artifact_path = bundle.root.join(&entry.relative_path);
            assert!(artifact_path.exists());
            assert_eq!(
                fs::read_to_string(artifact_path).unwrap().len(),
                entry.bytes
            );
        }
        let bundle_validation = bundle.validate_storage().unwrap();
        assert!(bundle_validation.valid);
        assert!(bundle_validation.manifest_present);
        assert_eq!(
            bundle_validation.retention,
            crate::reviewer::ArtifactRetentionPolicy::unlimited()
        );
        assert_eq!(bundle_validation.checked_artifacts, bundle.artifact_count);
        assert_eq!(bundle_validation.checked_bytes, bundle.total_bytes);
        assert_eq!(
            bundle_validation.checked_objects.len(),
            bundle.artifact_count
        );
        assert!(bundle_validation.missing_artifacts.is_empty());
        assert!(bundle_validation.stale_artifacts.is_empty());
        let first_bundle_path = bundle.root.join(&bundle.artifacts[0].relative_path);
        let first_bundle_bytes = fs::read(&first_bundle_path).unwrap();
        fs::write(&first_bundle_path, b"corrupted artifact bundle").unwrap();
        let stale_bundle = bundle.validate_storage().unwrap();
        assert!(!stale_bundle.valid);
        assert!(stale_bundle.missing_artifacts.is_empty());
        assert_eq!(stale_bundle.stale_artifacts.len(), 1);
        assert_eq!(stale_bundle.stale_artifacts[0].path, first_bundle_path);
        fs::write(&first_bundle_path, first_bundle_bytes).unwrap();
        assert!(bundle.validate_storage().unwrap().valid);
        let raw_bundle = report
            .export_artifact_bundle(bundle_dir.path().join("raw"), raw_artifact_policy)
            .unwrap();
        assert_eq!(raw_bundle.view, crate::reviewer::ArtifactViewMode::Raw);
        let raw_manifest: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&raw_bundle.manifest_path).unwrap()).unwrap();
        assert_eq!(raw_manifest["view"].as_str(), Some("raw"));
        let cleanup = bundle.cleanup_storage().unwrap();
        assert_eq!(cleanup.root, bundle.root);
        assert_eq!(cleanup.view, crate::reviewer::ArtifactViewMode::Redacted);
        assert_eq!(
            cleanup.retention,
            crate::reviewer::ArtifactRetentionPolicy::unlimited()
        );
        assert_eq!(cleanup.removed_artifacts, bundle.artifact_count);
        assert_eq!(cleanup.removed_bytes, bundle.total_bytes);
        assert_eq!(cleanup.removed_objects.len(), bundle.artifact_count);
        assert!(cleanup.missing_artifacts.is_empty());
        assert!(cleanup.removed_manifest);
        assert_eq!(cleanup.pruned_empty_directories, 1);
        assert!(!bundle.manifest_path.exists());
        for entry in &bundle.artifacts {
            assert!(!bundle.root.join(&entry.relative_path).exists());
        }
        assert!(raw_bundle.manifest_path.exists());
        let after_cleanup = bundle.validate_storage().unwrap();
        assert!(!after_cleanup.valid);
        assert!(!after_cleanup.manifest_present);
        assert_eq!(after_cleanup.missing_artifacts.len(), bundle.artifact_count);
        let event_records = events.records();
        assert!(!event_records.is_empty());
        for (index, record) in event_records.iter().enumerate() {
            assert_eq!(record.seq, index as u64 + 1);
            assert!(!record.timestamp_utc.is_empty());
            assert_eq!(record.run_id.as_deref(), Some("public-run"));
        }
        let review_event_log_dir = tempfile::tempdir().unwrap();
        let review_event_log = crate::reviewer::export_review_event_records_jsonl(
            review_event_log_dir.path().join("review-events.jsonl"),
            &event_records,
        )
        .unwrap();
        assert_eq!(
            review_event_log.schema_version,
            crate::reviewer::REVIEW_EVENT_LOG_SCHEMA_VERSION
        );
        assert_eq!(review_event_log.record_count, event_records.len());
        assert!(review_event_log.bytes > 0);
        let first_review_event_line = fs::read_to_string(&review_event_log.path)
            .unwrap()
            .lines()
            .next()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .unwrap();
        assert_eq!(
            first_review_event_line["schemaVersion"].as_str(),
            Some(crate::reviewer::REVIEW_EVENT_LOG_SCHEMA_VERSION)
        );
        assert_eq!(
            first_review_event_line["runId"].as_str(),
            Some("public-run")
        );
        let loaded_review_event_log =
            crate::reviewer::load_review_event_records_jsonl(&review_event_log.path).unwrap();
        assert_eq!(loaded_review_event_log.path, review_event_log.path);
        assert_eq!(
            loaded_review_event_log.schema_version,
            crate::reviewer::REVIEW_EVENT_LOG_SCHEMA_VERSION
        );
        assert_eq!(loaded_review_event_log.record_count, event_records.len());
        assert_eq!(loaded_review_event_log.records, event_records);
        assert_eq!(
            event_records[0].snapshot_id,
            Some(report.snapshot.snapshot_id.clone())
        );
        assert!(event_records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::ReviewEvent::ToolCallCompleted {
                ok: true,
                error_code: None,
                ..
            }
        ) && record.snapshot_id
            == Some(report.snapshot.snapshot_id.clone())
            && record.session_id.as_deref() == Some("public-session")
            && record.turn.is_some()
            && record.tool_call_id.is_some()));
        let event_types = events.events();
        assert!(matches!(
            event_types.first(),
            Some(crate::reviewer::ReviewEvent::RunStarted { .. })
        ));
        assert!(event_types.iter().any(|event| matches!(
            event,
            crate::reviewer::ReviewEvent::SessionStarted { session_id }
                if session_id == "public-session"
        )));
        assert!(event_types.iter().any(|event| matches!(
            event,
            crate::reviewer::ReviewEvent::ModelStarted { session_id, .. }
                if session_id == "public-session"
        )));
        assert!(event_types.iter().any(|event| matches!(
            event,
            crate::reviewer::ReviewEvent::ToolBatchStarted { count, .. } if *count == 3
        )));
        assert!(event_types.iter().any(|event| matches!(
            event,
            crate::reviewer::ReviewEvent::SearchBatchCompleted { searched_files, .. }
                if *searched_files > 0
        )));
        let artifact_event_ids = event_types
            .iter()
            .filter_map(|event| match event {
                crate::reviewer::ReviewEvent::ArtifactCreated { artifact_id, .. } => {
                    Some(artifact_id)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(!artifact_event_ids.is_empty());
        for artifact_id in artifact_event_ids {
            assert!(report.artifacts.get(artifact_id).is_some());
        }
        assert!(event_types.iter().any(|event| matches!(
            event,
            crate::reviewer::ReviewEvent::FindingRecorded { finding_id, .. }
                if !finding_id.is_empty()
        )));
        assert!(matches!(
            event_types.last(),
            Some(crate::reviewer::ReviewEvent::RunFinished { .. })
        ));
    }

    #[test]
    fn public_artifact_workflow_facade_persists_and_validates_without_low_level_ids() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-artifacts",
                "head-artifacts",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        )
        .with_path_policy(crate::reviewer::SnapshotPathPolicy::standard(64 * 1024, 20));
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "artifact-session",
            crate::reviewer::Role::Generalist,
            "Gather artifact evidence.",
            public_budget(),
        );
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "artifact-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicFacadeModel {
                path: "README.md".to_string(),
                query: "needle".to_string(),
            }))
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());
        let finding = report.findings().into_iter().next().unwrap();
        let export = report.redacted_artifacts().export().unwrap();
        let artifact_id = export.first_artifact_id().unwrap().to_string();
        let artifacts = report
            .redacted_artifacts()
            .only_artifacts([artifact_id.as_str()])
            .with_retention_policy(crate::reviewer::ArtifactRetentionPolicy::max_artifacts(1));

        let evidence_artifacts = artifacts.finding_evidence(&finding.id).unwrap();
        assert_eq!(evidence_artifacts.len(), 1);
        assert_eq!(evidence_artifacts[0].artifact_id(), artifact_id);

        let memory_store = crate::reviewer::InMemoryArtifactObjectStore::default();
        let memory_manifest = artifacts.persist_to(&memory_store).unwrap();
        assert!(memory_manifest.contains_artifact_id(&artifact_id));
        assert_eq!(memory_manifest.object_refs().len(), 1);
        let memory_object = memory_manifest.first_object_ref().unwrap();
        assert_eq!(
            memory_object.view(),
            crate::reviewer::ArtifactViewMode::Redacted
        );
        assert!(!memory_object.has_local_path());
        assert!(memory_object.path().is_none());
        assert!(memory_object
            .uri()
            .starts_with("memory://artifacts/redacted/"));
        assert_eq!(memory_object.content_hash().len(), 64);
        assert_eq!(
            memory_store
                .read_object(memory_object)
                .unwrap()
                .unwrap()
                .len(),
            memory_object.bytes()
        );
        let memory_validation = memory_manifest.validate_storage(&memory_store).unwrap();
        assert!(memory_validation.valid);
        assert!(!memory_validation.has_missing_artifact(&artifact_id));
        assert!(!memory_validation.has_stale_artifact(&artifact_id));
        let memory_cleanup = memory_manifest.cleanup_storage(&memory_store).unwrap();
        assert!(memory_cleanup.has_removed_artifact(&artifact_id));
        assert!(!memory_cleanup.has_missing_artifact(&artifact_id));
        assert_eq!(memory_store.object_count(), 0);
        let missing_after_cleanup = memory_manifest.validate_storage(&memory_store).unwrap();
        assert!(!missing_after_cleanup.valid);
        assert!(missing_after_cleanup.has_missing_artifact(&artifact_id));

        let local_dir = tempfile::tempdir().unwrap();
        let local_store = crate::reviewer::LocalArtifactObjectStore::new(local_dir.path());
        let local_manifest = report
            .redacted_artifacts()
            .only_artifacts([artifact_id.as_str()])
            .persist_to(&local_store)
            .unwrap();
        let local_object = local_manifest.first_object_ref().unwrap();
        assert!(local_object.has_local_path());
        assert!(local_object.path().unwrap().starts_with(local_store.root()));
        assert_eq!(
            fs::metadata(local_object.path().unwrap()).unwrap().len() as usize,
            local_object.bytes()
        );
    }

    #[test]
    fn public_artifact_bundle_lifecycle_rejects_unsafe_relative_paths() {
        let temp = tempfile::tempdir().unwrap();
        let bundle = crate::reviewer::ArtifactBundleManifest::new(
            crate::reviewer::ArtifactViewMode::Redacted,
            temp.path(),
            crate::reviewer::ArtifactRetentionPolicy::unlimited(),
            vec![crate::reviewer::ArtifactBundleEntry::new(
                "unsafe",
                0,
                "hash",
                "../outside.txt",
            )],
        );

        assert!(matches!(
            bundle.validate_storage(),
            Err(crate::reviewer::runtime::RuntimeError::RepoAccessDenied)
        ));
        assert!(matches!(
            bundle.cleanup_storage(),
            Err(crate::reviewer::runtime::RuntimeError::RepoAccessDenied)
        ));

        let forged_manifest = crate::reviewer::ArtifactBundleManifest::new(
            crate::reviewer::ArtifactViewMode::Redacted,
            temp.path(),
            crate::reviewer::ArtifactRetentionPolicy::unlimited(),
            Vec::new(),
        )
        .with_manifest_path(temp.path().join("outside-manifest.json"));
        assert!(matches!(
            forged_manifest.validate_storage(),
            Err(crate::reviewer::runtime::RuntimeError::RepoAccessDenied)
        ));
        assert!(matches!(
            forged_manifest.cleanup_storage(),
            Err(crate::reviewer::runtime::RuntimeError::RepoAccessDenied)
        ));
    }

    #[test]
    fn public_reviewer_facade_emits_tool_denial_events() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-denied",
                "head-denied",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        )
        .with_path_policy(crate::reviewer::SnapshotPathPolicy::standard(64 * 1024, 20));
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "denied-session",
            crate::reviewer::Role::Generalist,
            "Run with read_diff denied.",
            public_budget(),
        )
        .deny_tool(crate::reviewer::ids::ToolId::from(ToolName::ReadDiff));
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "denied-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let events = Arc::new(crate::reviewer::InMemoryReviewEventSink::default());
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicFacadeModel {
                path: "README.md".to_string(),
                query: "needle".to_string(),
            }))
            .review_event_sink(events.clone())
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _report = tokio.block_on(run.execute());

        let event_records = events.records();
        assert!(event_records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::ReviewEvent::ToolCallDenied {
                tool_id,
                error_code: crate::reviewer::tool_adapters::ToolErrorCode::ToolNotAllowed,
                reason,
                ..
            } if tool_id == "read_diff"
                && reason.contains("not allowed")
                && record.run_id.as_deref() == Some("denied-run")
                && record.session_id.as_deref() == Some("denied-session")
                && record.turn.is_some()
                && record.tool_call_id.is_some()
        )));
    }

    #[test]
    fn public_reviewer_facade_cancelled_run_emits_review_events() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-public-cancel",
                "head-public-cancel",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        )
        .with_path_policy(crate::reviewer::SnapshotPathPolicy::standard(64 * 1024, 20));
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "public-cancel-session",
            crate::reviewer::Role::Generalist,
            "Run cancellation through the public reviewer facade.",
            public_budget(),
        );
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "public-cancel-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let events = Arc::new(crate::reviewer::InMemoryReviewEventSink::default());
        let cancel = crate::reviewer::Cancellation::new();
        let model_calls = Arc::new(AtomicUsize::new(0));
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(CancellingModel {
                parent_cancel: cancel.clone(),
                calls: Arc::clone(&model_calls),
            }))
            .review_event_sink(events.clone())
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let report = tokio.block_on(run.execute_with_cancel(cancel));

        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
        assert_eq!(report.summary.sessions, 1);
        assert_eq!(report.summary.completed_sessions, 0);
        assert_eq!(report.summary.model_calls, 1);
        assert_eq!(report.summary.tool_calls, 0);
        let event_records = events.records();
        assert!(event_records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::ReviewEvent::ModelStarted { session_id, .. }
                if session_id == "public-cancel-session"
                    && record.run_id.as_deref() == Some("public-cancel-run")
                    && record.session_id.as_deref() == Some("public-cancel-session")
                    && record.turn.is_some()
        )));
        assert!(!event_records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::ReviewEvent::ModelCompleted { .. }
                | crate::reviewer::ReviewEvent::ToolBatchStarted { .. }
                | crate::reviewer::ReviewEvent::ToolCallCompleted { .. }
        )));
        assert!(event_records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::ReviewEvent::SessionFinished { session_id, status }
                if session_id == "public-cancel-session"
                    && status == "cancelled"
                    && record.run_id.as_deref() == Some("public-cancel-run")
                    && record.session_id.as_deref() == Some("public-cancel-session")
        )));
        assert!(matches!(
            event_records.last().map(|record| &record.event),
            Some(crate::reviewer::ReviewEvent::RunFinished { status }) if status == "partial"
        ));
    }

    #[test]
    fn public_snapshot_storage_policy_reports_memory_envelope_skips() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-storage",
                "head-storage",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        )
        .with_path_policy(crate::reviewer::SnapshotPathPolicy::standard(64 * 1024, 20))
        .with_memory_storage_limit(0);
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "storage-run",
            snapshot,
            Vec::new(),
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicFacadeModel {
                path: "README.md".to_string(),
                query: "needle".to_string(),
            }))
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());
        let manifest = report.snapshot_manifests().pop().unwrap();

        assert_eq!(manifest.max_captured_text_bytes(), 0);
        assert!(!manifest.storage_policy_hash.is_empty());
        assert_eq!(manifest.captured_text_file_count, 0);
        assert_eq!(manifest.captured_text_bytes, 0);
        assert_eq!(manifest.capture_skipped_file_count, 1);
        assert!(manifest.capture_skipped_bytes > 0);
        assert!(manifest.files[0].capture_skipped_memory_limit());
        assert!(matches!(
            report
                .snapshot_reader(&report.snapshot.snapshot_id)
                .unwrap()
                .read_text_path("README.md", 64 * 1024),
            Err(crate::reviewer::runtime::RuntimeError::LimitExceeded {
                kind: "snapshot_capture_bytes"
            })
        ));
    }

    #[test]
    fn public_snapshot_content_addressed_store_serves_captured_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let store = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "persistent needle\n").unwrap();
        let store_root = store.path().join("snapshots");
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-content-addressed",
                "head-content-addressed",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        )
        .with_path_policy(crate::reviewer::SnapshotPathPolicy::standard(64 * 1024, 20))
        .with_content_addressed_storage(store_root.clone(), 64 * 1024);
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "content-addressed-session",
            crate::reviewer::Role::Generalist,
            "Run content-addressed snapshot evidence through public facade.",
            public_budget(),
        );
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "content-addressed-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicFacadeModel {
                path: "README.md".to_string(),
                query: "needle".to_string(),
            }))
            .build()
            .unwrap();
        fs::write(temp.path().join("README.md"), "mutated needle\n").unwrap();

        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());
        let manifest = report.snapshot_manifests().pop().unwrap();
        assert!(manifest.uses_content_addressed_storage());
        let readme = manifest
            .files
            .iter()
            .find(|file| file.path.display() == "README.md")
            .expect("README.md in content-addressed snapshot manifest");
        let content_hash = readme.content_hash.clone().unwrap();
        let blob_path = store_root
            .join(content_hash.get(..2).unwrap_or("00"))
            .join(&content_hash);
        assert_eq!(
            fs::read_to_string(blob_path).unwrap(),
            "persistent needle\n"
        );

        let snapshot_text = report
            .snapshot_reader(&report.snapshot.snapshot_id)
            .unwrap()
            .read_text_path("README.md", 64 * 1024)
            .unwrap();
        assert!(snapshot_text.content.contains("persistent needle"));
        assert!(!snapshot_text.content.contains("mutated needle"));

        let artifact_text = report
            .artifacts
            .list()
            .into_iter()
            .map(|artifact| artifact.content)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(artifact_text.contains("persistent needle"));
        assert!(!artifact_text.contains("mutated needle"));
    }

    #[test]
    fn public_snapshot_content_addressed_store_lifecycle_validates_and_cleans_up() {
        let temp = tempfile::tempdir().unwrap();
        let store = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "lifecycle needle\n").unwrap();
        let store_root = store.path().join("snapshots");
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-storage-lifecycle",
                "head-storage-lifecycle",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        )
        .with_path_policy(crate::reviewer::SnapshotPathPolicy::standard(64 * 1024, 20))
        .with_content_addressed_storage(store_root.clone(), 64 * 1024);
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "storage-lifecycle-run",
            snapshot,
            Vec::new(),
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicFacadeModel {
                path: "README.md".to_string(),
                query: "needle".to_string(),
            }))
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());
        let reader = report
            .snapshot_reader(&report.snapshot.snapshot_id)
            .unwrap();

        let validation = reader.validate_storage().unwrap();
        assert_eq!(validation.snapshot_id, report.snapshot.snapshot_id);
        assert!(validation.uses_content_addressed_storage());
        assert!(validation.valid);
        assert_eq!(validation.checked_files, 1);
        assert_eq!(validation.checked_bytes, "lifecycle needle\n".len());
        assert_eq!(validation.checked_objects.len(), 1);
        assert!(validation.missing_files.is_empty());
        assert!(validation.stale_files.is_empty());
        let store_path = validation.checked_objects[0]
            .store_path
            .as_ref()
            .expect("validated content-addressed object path")
            .clone();
        assert!(store_path.exists());

        fs::write(&store_path, "corrupted lifecycle\n").unwrap();
        let stale = reader.validate_storage().unwrap();
        assert!(!stale.valid);
        assert!(stale.missing_files.is_empty());
        assert_eq!(stale.stale_files.len(), 1);
        assert_eq!(stale.stale_files[0].store_path.as_ref(), Some(&store_path));
        assert!(matches!(
            reader.read_text_path("README.md", 64 * 1024),
            Err(crate::reviewer::runtime::RuntimeError::SnapshotStale { path }) if path == "README.md"
        ));
        fs::write(&store_path, "lifecycle needle\n").unwrap();
        assert!(reader.validate_storage().unwrap().valid);

        let cleanup = reader.cleanup_storage().unwrap();
        assert_eq!(cleanup.snapshot_id, report.snapshot.snapshot_id);
        assert_eq!(cleanup.removed_files, 1);
        assert_eq!(cleanup.removed_bytes, "lifecycle needle\n".len());
        assert_eq!(cleanup.removed_objects.len(), 1);
        assert!(cleanup.missing_files.is_empty());
        assert_eq!(cleanup.pruned_empty_directories, 1);
        let removed_path = cleanup.removed_objects[0]
            .store_path
            .as_ref()
            .expect("removed content-addressed object path")
            .clone();
        assert_eq!(removed_path, store_path);
        assert!(!removed_path.exists());
        assert!(store_root.exists());

        let after_cleanup = reader.validate_storage().unwrap();
        assert!(!after_cleanup.valid);
        assert_eq!(after_cleanup.missing_files.len(), 1);
        assert_eq!(after_cleanup.missing_files[0].path.display(), "README.md");
        assert!(matches!(
            reader.read_text_path("README.md", 64 * 1024),
            Err(crate::reviewer::runtime::RuntimeError::RepoUnavailable(_))
        ));
    }

    #[test]
    fn public_snapshot_remote_object_store_serves_and_validates_captured_evidence() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "remote lifecycle needle\n").unwrap();
        let remote_client =
            Arc::new(crate::reviewer::InMemoryRemoteSnapshotObjectClient::default());
        let remote_base = "s3://muzen-test-snapshots/storage-lifecycle-run";
        let remote_store = Arc::new(
            crate::reviewer::RemoteSnapshotObjectStore::new(remote_base, remote_client.clone())
                .unwrap(),
        );
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-remote-storage-lifecycle",
                "head-remote-storage-lifecycle",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        )
        .with_path_policy(crate::reviewer::SnapshotPathPolicy::standard(64 * 1024, 20))
        .with_remote_object_storage(remote_base, 64 * 1024, remote_store.clone())
        .unwrap();
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "remote-storage-lifecycle-run",
            snapshot,
            Vec::new(),
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicFacadeModel {
                path: "README.md".to_string(),
                query: "needle".to_string(),
            }))
            .build()
            .unwrap();
        fs::write(temp.path().join("README.md"), "mutated remote needle\n").unwrap();

        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());
        let manifest = report.snapshot_manifests().pop().unwrap();
        assert!(manifest.uses_remote_object_storage());
        assert_eq!(remote_client.object_count(), 1);
        let reader = report
            .snapshot_reader(&report.snapshot.snapshot_id)
            .unwrap();
        let snapshot_text = reader.read_text_path("README.md", 64 * 1024).unwrap();
        assert!(snapshot_text.content.contains("remote lifecycle needle"));
        assert!(!snapshot_text.content.contains("mutated remote needle"));

        let validation = reader.validate_storage().unwrap();
        assert!(validation.uses_remote_object_storage());
        assert!(validation.valid);
        assert_eq!(validation.checked_files, 1);
        assert_eq!(validation.checked_bytes, "remote lifecycle needle\n".len());
        let object = validation.checked_objects[0].clone();
        assert!(object.store_path.is_none());
        let store_uri = object
            .store_uri
            .as_ref()
            .expect("remote snapshot object uri");
        assert!(store_uri.starts_with(&format!("{remote_base}/snapshots/")));
        assert_eq!(remote_client.read(store_uri).unwrap().len(), object.bytes);

        remote_client.write(store_uri.clone(), b"stale remote snapshot".to_vec());
        let stale = reader.validate_storage().unwrap();
        assert!(!stale.valid);
        assert!(stale.missing_files.is_empty());
        assert_eq!(stale.stale_files.len(), 1);
        assert_eq!(stale.stale_files[0].store_uri.as_ref(), Some(store_uri));
        assert!(matches!(
            reader.read_text_path("README.md", 64 * 1024),
            Err(crate::reviewer::runtime::RuntimeError::SnapshotStale { path }) if path == "README.md"
        ));

        remote_client.write(store_uri.clone(), b"remote lifecycle needle\n".to_vec());
        assert!(reader.validate_storage().unwrap().valid);

        let forged_uri = store_uri.replace(remote_base, "s3://forged-snapshots");
        assert!(matches!(
            crate::reviewer::storage::SnapshotObjectStore::read_snapshot_object(
                remote_store.as_ref(),
                &forged_uri
            ),
            Err(crate::reviewer::runtime::RuntimeError::RepoAccessDenied)
        ));
        assert!(crate::reviewer::RemoteSnapshotObjectStore::new(
            "file:///tmp/muzen-snapshots",
            remote_client.clone()
        )
        .is_err());

        let cleanup = reader.cleanup_storage().unwrap();
        assert_eq!(cleanup.snapshot_id, report.snapshot.snapshot_id);
        assert_eq!(cleanup.removed_files, 1);
        assert_eq!(cleanup.removed_bytes, "remote lifecycle needle\n".len());
        assert_eq!(cleanup.removed_objects.len(), 1);
        assert_eq!(
            cleanup.removed_objects[0].store_uri.as_ref(),
            Some(store_uri)
        );
        assert!(cleanup.removed_objects[0].store_path.is_none());
        assert!(cleanup.missing_files.is_empty());
        assert_eq!(cleanup.pruned_empty_directories, 0);
        assert!(remote_client.read(store_uri).is_none());

        let after_cleanup = reader.validate_storage().unwrap();
        assert!(!after_cleanup.valid);
        assert_eq!(after_cleanup.missing_files.len(), 1);
        assert_eq!(
            after_cleanup.missing_files[0].store_uri.as_ref(),
            Some(store_uri)
        );
        assert!(matches!(
            reader.read_text_path("README.md", 64 * 1024),
            Err(crate::reviewer::runtime::RuntimeError::RepoUnavailable(_))
        ));
    }

    #[test]
    fn public_remote_snapshot_object_store_canary_proves_integrity_and_cleanup() {
        let remote_client =
            Arc::new(crate::reviewer::InMemoryRemoteSnapshotObjectClient::default());
        let evidence = crate::reviewer::storage::run_remote_snapshot_object_store_canary(
            "s3://muzen-test-snapshots/canary",
            remote_client.as_ref(),
        );

        evidence.require_passed().expect("snapshot canary passed");
        assert_eq!(
            evidence.schema_version,
            crate::reviewer::storage::REMOTE_OBJECT_STORE_CANARY_SCHEMA_VERSION
        );
        assert_eq!(
            evidence.target,
            crate::reviewer::storage::RemoteObjectStoreCanaryTarget::Snapshot
        );
        assert!(evidence.cleanup_supported);
        assert_eq!(evidence.gate.passed, 4);
        assert_eq!(evidence.gate.failed, 0);
        assert_eq!(evidence.gate.skipped, 0);
        assert_eq!(remote_client.object_count(), 0);

        let evidence_dir = tempfile::tempdir().unwrap();
        let evidence_path = evidence_dir
            .path()
            .join("canaries")
            .join("remote-snapshot-object-store.json");
        let export = crate::reviewer::storage::export_remote_object_store_canary_evidence(
            &evidence_path,
            &evidence,
        )
        .unwrap();
        assert!(export.valid);
        assert_eq!(export.path, evidence_path);
        let serialized = fs::read_to_string(&export.path).unwrap();
        assert!(serialized.ends_with('\n'));
        let loaded: crate::reviewer::storage::RemoteObjectStoreCanaryEvidence =
            serde_json::from_str(&serialized).unwrap();
        loaded.require_passed().expect("loaded canary passed");

        let mut forged = loaded.clone();
        forged.steps.pop();
        forged.gate.valid = true;
        forged.gate.failures.clear();
        let error = forged.require_passed().unwrap_err().to_string();
        assert!(error.contains("stored remote object-store canary gate does not match steps"));
        assert!(error.contains("missing ReadAfterRemove canary step"));
    }

    #[test]
    fn public_remote_artifact_object_store_canary_proves_integrity_and_cleanup() {
        let remote_client =
            Arc::new(crate::reviewer::InMemoryRemoteArtifactObjectClient::default());
        let evidence = crate::reviewer::storage::run_remote_artifact_object_store_canary(
            "s3://muzen-test-artifacts/canary",
            remote_client.as_ref(),
        );

        evidence.require_passed().expect("artifact canary passed");
        assert_eq!(
            evidence.target,
            crate::reviewer::storage::RemoteObjectStoreCanaryTarget::Artifact
        );
        assert!(evidence.cleanup_supported);
        assert_eq!(evidence.gate.passed, 4);
        assert_eq!(evidence.gate.failed, 0);
        assert_eq!(evidence.gate.skipped, 0);
        assert_eq!(remote_client.object_count(), 0);
        let object_uri = evidence.object_uri.as_ref().expect("object uri");
        assert!(object_uri.starts_with("s3://muzen-test-artifacts/canary/artifacts/redacted/"));
        assert!(remote_client.read(object_uri).is_none());
        assert!(evidence.steps.iter().all(|step| {
            step.status == crate::reviewer::storage::RemoteObjectStoreCanaryStatus::Passed
        }));
    }

    #[test]
    fn public_canary_evidence_manifest_gates_provider_and_remote_store_evidence() {
        let model_provider = passing_model_provider_canary_evidence();
        let snapshot_client =
            Arc::new(crate::reviewer::InMemoryRemoteSnapshotObjectClient::default());
        let artifact_client =
            Arc::new(crate::reviewer::InMemoryRemoteArtifactObjectClient::default());
        let snapshot = crate::reviewer::canaries::run_remote_snapshot_object_store_canary(
            "s3://muzen-test-snapshots/canary",
            snapshot_client.as_ref(),
        );
        let artifact = crate::reviewer::canaries::run_remote_artifact_object_store_canary(
            "s3://muzen-test-artifacts/canary",
            artifact_client.as_ref(),
        );
        let manifest = crate::reviewer::canaries::CanaryEvidenceManifest::with_generated_at(
            "manifest-time",
            Some(model_provider.clone()),
            vec![snapshot.clone(), artifact.clone()],
        );

        manifest.require_passed().expect("manifest passed");
        assert_eq!(
            manifest.schema_version,
            crate::reviewer::canaries::CANARY_EVIDENCE_MANIFEST_SCHEMA_VERSION
        );
        assert_eq!(
            manifest.gate.passed,
            model_provider.gate.passed + snapshot.gate.passed + artifact.gate.passed
        );
        assert_eq!(manifest.gate.failed, 0);
        assert_eq!(manifest.gate.skipped, 0);

        let evidence_dir = tempfile::tempdir().unwrap();
        let evidence_path = evidence_dir.path().join("canaries").join("manifest.json");
        let export =
            crate::reviewer::canaries::export_canary_evidence_manifest(&evidence_path, &manifest)
                .unwrap();
        assert!(export.valid);
        assert_eq!(export.path, evidence_path);
        let serialized = fs::read_to_string(&export.path).unwrap();
        assert!(serialized.ends_with('\n'));
        let loaded: crate::reviewer::canaries::CanaryEvidenceManifest =
            serde_json::from_str(&serialized).unwrap();
        loaded.require_passed().expect("loaded manifest passed");

        let missing_model = crate::reviewer::canaries::CanaryEvidenceManifest::with_generated_at(
            "manifest-time",
            None,
            vec![snapshot.clone(), artifact.clone()],
        );
        let error = missing_model.require_passed().unwrap_err().to_string();
        assert!(error.contains("missing model provider canary evidence"));

        let duplicate_snapshot =
            crate::reviewer::canaries::CanaryEvidenceManifest::with_generated_at(
                "manifest-time",
                Some(model_provider.clone()),
                vec![snapshot.clone(), snapshot.clone(), artifact.clone()],
            );
        let error = duplicate_snapshot.require_passed().unwrap_err().to_string();
        assert!(error.contains("duplicate snapshot remote object-store canary evidence: 2"));

        let mut forged = loaded;
        forged.remote_object_stores.pop();
        forged.gate.valid = true;
        forged.gate.failures.clear();
        let error = forged.require_passed().unwrap_err().to_string();
        assert!(error.contains("stored canary evidence manifest gate does not match evidence"));
        assert!(error.contains("missing artifact remote object-store canary evidence"));
    }

    #[test]
    fn public_canary_evidence_manifest_freshness_policy_rejects_stale_and_future_evidence() {
        let model_provider = passing_model_provider_canary_evidence_at("1000.000000000Z");
        let snapshot_client =
            Arc::new(crate::reviewer::InMemoryRemoteSnapshotObjectClient::default());
        let artifact_client =
            Arc::new(crate::reviewer::InMemoryRemoteArtifactObjectClient::default());
        let mut snapshot = crate::reviewer::canaries::run_remote_snapshot_object_store_canary(
            "s3://muzen-test-snapshots/canary",
            snapshot_client.as_ref(),
        );
        let mut artifact = crate::reviewer::canaries::run_remote_artifact_object_store_canary(
            "s3://muzen-test-artifacts/canary",
            artifact_client.as_ref(),
        );
        snapshot.generated_at_utc = "1000.000000000Z".to_string();
        artifact.generated_at_utc = "1000.000000000Z".to_string();
        let manifest = crate::reviewer::canaries::CanaryEvidenceManifest::with_generated_at(
            "1000.000000000Z",
            Some(model_provider),
            vec![snapshot, artifact],
        );
        let fresh =
            crate::reviewer::canaries::CanaryEvidenceFreshnessPolicy::at("1100.000000000Z", 120);
        manifest
            .require_passed_with_freshness(&fresh)
            .expect("fresh manifest passed");

        let stale =
            crate::reviewer::canaries::CanaryEvidenceFreshnessPolicy::at("1300.000000000Z", 120);
        let error = manifest
            .require_passed_with_freshness(&stale)
            .unwrap_err()
            .to_string();
        assert!(error.contains("canary evidence manifest is stale"));
        assert!(error.contains("model provider canary evidence is stale"));
        assert!(error.contains("snapshot remote object-store canary evidence is stale"));
        assert!(error.contains("artifact remote object-store canary evidence is stale"));

        let future =
            crate::reviewer::canaries::CanaryEvidenceFreshnessPolicy::at("900.000000000Z", 120);
        let error = manifest
            .require_passed_with_freshness(&future)
            .unwrap_err()
            .to_string();
        assert!(error.contains("generatedAtUtc is in the future"));
    }

    #[test]
    fn public_canary_evidence_status_report_separates_gate_and_freshness_failures() {
        let model_provider = passing_model_provider_canary_evidence_at("1000.000000000Z");
        let snapshot_client =
            Arc::new(crate::reviewer::InMemoryRemoteSnapshotObjectClient::default());
        let mut snapshot = crate::reviewer::canaries::run_remote_snapshot_object_store_canary(
            "s3://muzen-test-snapshots/canary",
            snapshot_client.as_ref(),
        );
        snapshot.generated_at_utc = "1000.000000000Z".to_string();
        let manifest = crate::reviewer::canaries::CanaryEvidenceManifest::with_generated_at(
            "1000.000000000Z",
            Some(model_provider),
            vec![snapshot],
        );

        let report = manifest.status_report(
            &crate::reviewer::canaries::CanaryEvidenceFreshnessPolicy::at("1300.000000000Z", 120),
        );

        assert!(!report.ok);
        assert_eq!(
            report.manifest_schema_version,
            crate::reviewer::canaries::CANARY_EVIDENCE_MANIFEST_SCHEMA_VERSION
        );
        assert_eq!(report.generated_at_utc, "1000.000000000Z");
        assert_eq!(report.freshness_checked_at_utc, "1300.000000000Z");
        assert_eq!(report.max_evidence_age_seconds, 120);
        assert!(report.validation_failures.iter().any(
            |failure| failure.contains("missing artifact remote object-store canary evidence")
        ));
        assert!(report
            .freshness_failures
            .iter()
            .any(|failure| failure.contains("canary evidence manifest is stale")));
        let failures = report.failures();
        assert!(failures.iter().any(
            |failure| failure.contains("missing artifact remote object-store canary evidence")
        ));
        assert!(failures
            .iter()
            .any(|failure| failure.contains("model provider canary evidence is stale")));
    }

    #[test]
    fn cli_canary_manifest_composes_and_gates_evidence_files() {
        let evidence_dir = tempfile::tempdir().unwrap();
        let provider_path = evidence_dir.path().join("provider.json");
        let snapshot_path = evidence_dir.path().join("snapshot.json");
        let artifact_path = evidence_dir.path().join("artifact.json");
        let manifest_path = evidence_dir.path().join("manifest.json");
        let invalid_manifest_path = evidence_dir.path().join("invalid-manifest.json");
        let provider = passing_model_provider_canary_evidence();
        crate::reviewer::canaries::export_model_provider_canary_evidence(&provider_path, &provider)
            .unwrap();

        let snapshot_client =
            Arc::new(crate::reviewer::InMemoryRemoteSnapshotObjectClient::default());
        let artifact_client =
            Arc::new(crate::reviewer::InMemoryRemoteArtifactObjectClient::default());
        let snapshot = crate::reviewer::canaries::run_remote_snapshot_object_store_canary(
            "s3://muzen-test-snapshots/canary",
            snapshot_client.as_ref(),
        );
        let artifact = crate::reviewer::canaries::run_remote_artifact_object_store_canary(
            "s3://muzen-test-artifacts/canary",
            artifact_client.as_ref(),
        );
        crate::reviewer::canaries::export_remote_object_store_canary_evidence(
            &snapshot_path,
            &snapshot,
        )
        .unwrap();
        crate::reviewer::canaries::export_remote_object_store_canary_evidence(
            &artifact_path,
            &artifact,
        )
        .unwrap();

        let code = crate::cli::run_canary_manifest(CanaryManifestArgs {
            provider_evidence: provider_path.clone(),
            remote_object_store_evidence: vec![snapshot_path.clone(), artifact_path.clone()],
            output: Some(manifest_path.clone()),
            max_evidence_age_seconds: 86_400,
        })
        .unwrap();
        assert_eq!(code, 0);
        let loaded = crate::reviewer::canaries::load_canary_evidence_manifest(&manifest_path)
            .expect("load manifest");
        loaded.require_passed().expect("manifest passed");

        let error = crate::cli::run_canary_manifest(CanaryManifestArgs {
            provider_evidence: provider_path,
            remote_object_store_evidence: vec![snapshot_path],
            output: Some(invalid_manifest_path.clone()),
            max_evidence_age_seconds: 86_400,
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("missing artifact remote object-store canary evidence"));
        let invalid =
            crate::reviewer::canaries::load_canary_evidence_manifest(&invalid_manifest_path)
                .expect("load invalid manifest");
        assert!(!invalid.gate.valid);
    }

    #[test]
    fn cli_canary_publish_writes_child_evidence_and_manifest() {
        let evidence_dir = tempfile::tempdir().unwrap();
        let provider_input_path = evidence_dir.path().join("input-provider.json");
        let output_dir = evidence_dir.path().join("published");
        let provider = passing_model_provider_canary_evidence();
        crate::reviewer::canaries::export_model_provider_canary_evidence(
            &provider_input_path,
            &provider,
        )
        .unwrap();

        let code = crate::cli::run_canary_publish(crate::cli::CanaryPublishArgs {
            output_dir: output_dir.clone(),
            provider_evidence: Some(provider_input_path),
            snapshot_base_uri: "memory://muzen-canaries/snapshots".to_string(),
            artifact_base_uri: "memory://muzen-canaries/artifacts".to_string(),
            object_store_driver: crate::cli::RemoteObjectStoreCanaryDriver::Memory,
            object_store_bearer_token_env: "MUZEN_TEST_UNUSED_BEARER_TOKEN".to_string(),
            model: DEFAULT_MODEL.to_string(),
            provider_base_url: None,
            max_output_tokens: 64,
            max_evidence_age_seconds: 86_400,
        })
        .unwrap();
        assert_eq!(code, 0);

        let provider_output = output_dir.join("model-provider.json");
        let snapshot_output = output_dir.join("remote-snapshot-object-store.json");
        let artifact_output = output_dir.join("remote-artifact-object-store.json");
        let manifest_output = output_dir.join("manifest.json");
        let status_output = output_dir.join("status.json");
        let publication_output = output_dir.join("publication.json");
        assert!(provider_output.exists());
        assert!(snapshot_output.exists());
        assert!(artifact_output.exists());
        assert!(manifest_output.exists());
        assert!(status_output.exists());
        assert!(publication_output.exists());

        let manifest = crate::reviewer::canaries::load_canary_evidence_manifest(&manifest_output)
            .expect("load published manifest");
        manifest
            .require_passed_with_freshness(
                &crate::reviewer::canaries::CanaryEvidenceFreshnessPolicy::current(86_400),
            )
            .expect("published manifest passed");
        let status: crate::reviewer::canaries::CanaryEvidenceStatusReport =
            serde_json::from_str(&fs::read_to_string(&status_output).unwrap()).unwrap();
        assert!(status.ok);
        assert!(status.evidence.model_provider.present);
        let publication: crate::cli::CanaryPublicationReport =
            serde_json::from_str(&fs::read_to_string(&publication_output).unwrap()).unwrap();
        assert_eq!(
            publication.provider_evidence_source,
            crate::cli::CanaryProviderEvidenceSource::ReusedEvidenceFile
        );
        assert_eq!(
            publication.object_store_driver,
            crate::cli::RemoteObjectStoreCanaryDriver::Memory
        );
        assert!(publication.provider_evidence_input.is_some());
        assert!(publication.status_ok);
        assert!(publication.failures.is_empty());
        assert_eq!(publication.files.status, "status.json");
    }

    #[test]
    fn cli_canary_publish_writes_status_for_failed_manifest_gate() {
        let evidence_dir = tempfile::tempdir().unwrap();
        let provider_input_path = evidence_dir.path().join("input-provider.json");
        let output_dir = evidence_dir.path().join("published");
        let provider = crate::reviewer::canaries::ModelProviderCanaryEvidence::with_generated_at(
            crate::util::timestamp_utc(),
            Vec::new(),
        );
        crate::reviewer::canaries::export_model_provider_canary_evidence(
            &provider_input_path,
            &provider,
        )
        .unwrap();

        let error = crate::cli::run_canary_publish(crate::cli::CanaryPublishArgs {
            output_dir: output_dir.clone(),
            provider_evidence: Some(provider_input_path),
            snapshot_base_uri: "memory://muzen-canaries/snapshots".to_string(),
            artifact_base_uri: "memory://muzen-canaries/artifacts".to_string(),
            object_store_driver: crate::cli::RemoteObjectStoreCanaryDriver::Memory,
            object_store_bearer_token_env: "MUZEN_TEST_UNUSED_BEARER_TOKEN".to_string(),
            model: DEFAULT_MODEL.to_string(),
            provider_base_url: None,
            max_output_tokens: 64,
            max_evidence_age_seconds: 86_400,
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("canary evidence manifest gate failed"));

        let manifest_output = output_dir.join("manifest.json");
        let status_output = output_dir.join("status.json");
        let publication_output = output_dir.join("publication.json");
        assert!(manifest_output.exists());
        assert!(status_output.exists());
        assert!(publication_output.exists());
        let status: crate::reviewer::canaries::CanaryEvidenceStatusReport =
            serde_json::from_str(&fs::read_to_string(&status_output).unwrap()).unwrap();
        assert!(!status.ok);
        assert!(status.evidence.model_provider.present);
        assert!(status.evidence.model_provider.reported_protocols.is_empty());
        assert!(status
            .validation_failures
            .iter()
            .any(|failure| failure.contains("missing chat_completions canary report")));
        assert!(status
            .validation_failures
            .iter()
            .any(|failure| failure.contains("missing responses canary report")));
        assert!(status
            .evidence
            .remote_object_stores
            .iter()
            .all(|evidence| evidence.gate.as_ref().is_some_and(|gate| gate.valid)));
        let publication: crate::cli::CanaryPublicationReport =
            serde_json::from_str(&fs::read_to_string(&publication_output).unwrap()).unwrap();
        assert_eq!(
            publication.provider_evidence_source,
            crate::cli::CanaryProviderEvidenceSource::ReusedEvidenceFile
        );
        assert!(!publication.status_ok);
        assert_eq!(publication.failures, status.failures());
    }

    #[test]
    fn canary_publication_report_records_live_provider_mode_without_network() {
        let args = crate::cli::CanaryPublishArgs {
            output_dir: PathBuf::from("published"),
            provider_evidence: None,
            snapshot_base_uri: "https://objects.example.test/snapshots".to_string(),
            artifact_base_uri: "https://objects.example.test/artifacts".to_string(),
            object_store_driver: crate::cli::RemoteObjectStoreCanaryDriver::Http,
            object_store_bearer_token_env: "MUZEN_REMOTE_OBJECT_STORE_BEARER_TOKEN".to_string(),
            model: "canary-model".to_string(),
            provider_base_url: Some("https://api.example.test/v1".to_string()),
            max_output_tokens: 64,
            max_evidence_age_seconds: 86_400,
        };
        let status = current_passing_canary_manifest().status_report(
            &crate::reviewer::canaries::CanaryEvidenceFreshnessPolicy::current(86_400),
        );
        let publication = crate::cli::canary_publication_report(&args, &status);

        assert_eq!(
            publication.provider_evidence_source,
            crate::cli::CanaryProviderEvidenceSource::LiveProviderCanary
        );
        assert_eq!(
            publication.object_store_driver,
            crate::cli::RemoteObjectStoreCanaryDriver::Http
        );
        assert_eq!(
            publication.provider_base_url.as_deref(),
            Some("https://api.example.test/v1")
        );
        assert_eq!(publication.model, "canary-model");
        assert!(publication.provider_evidence_input.is_none());
        assert!(publication.status_ok);
    }

    #[test]
    fn cli_canary_preflight_accepts_reused_provider_evidence_and_memory_store() {
        let evidence_dir = tempfile::tempdir().unwrap();
        let provider_input_path = evidence_dir.path().join("input-provider.json");
        let provider = passing_model_provider_canary_evidence();
        crate::reviewer::canaries::export_model_provider_canary_evidence(
            &provider_input_path,
            &provider,
        )
        .unwrap();

        let args = crate::cli::CanaryPublishArgs {
            output_dir: evidence_dir.path().join("published"),
            provider_evidence: Some(provider_input_path),
            snapshot_base_uri: "memory://muzen-canaries/snapshots".to_string(),
            artifact_base_uri: "memory://muzen-canaries/artifacts".to_string(),
            object_store_driver: crate::cli::RemoteObjectStoreCanaryDriver::Memory,
            object_store_bearer_token_env: "MUZEN_TEST_UNUSED_BEARER_TOKEN".to_string(),
            model: DEFAULT_MODEL.to_string(),
            provider_base_url: None,
            max_output_tokens: 64,
            max_evidence_age_seconds: 86_400,
        };
        let report = crate::cli::canary_publication_preflight_report_with_env(&args, &|_| None);

        assert!(report.ok);
        assert_eq!(
            report.config.provider_evidence_source,
            crate::cli::CanaryProviderEvidenceSource::ReusedEvidenceFile
        );
        assert_eq!(
            report.config.object_store_driver,
            crate::cli::RemoteObjectStoreCanaryDriver::Memory
        );
        assert!(report.checks.iter().any(|check| {
            check.name == "providerEvidence"
                && check.status == crate::cli::CanaryPublicationPreflightStatus::Passed
        }));
        assert!(report.checks.iter().any(|check| {
            check.name == "remoteObjectStoreAuth"
                && check.status == crate::cli::CanaryPublicationPreflightStatus::Passed
        }));
    }

    #[test]
    fn cli_canary_preflight_reports_missing_live_configuration() {
        let evidence_dir = tempfile::tempdir().unwrap();
        let args = crate::cli::CanaryPublishArgs {
            output_dir: evidence_dir.path().join("published"),
            provider_evidence: None,
            snapshot_base_uri: "s3://muzen-canaries/snapshots".to_string(),
            artifact_base_uri: "s3://muzen-canaries/artifacts".to_string(),
            object_store_driver: crate::cli::RemoteObjectStoreCanaryDriver::Http,
            object_store_bearer_token_env: "MUZEN_REMOTE_OBJECT_STORE_BEARER_TOKEN".to_string(),
            model: DEFAULT_MODEL.to_string(),
            provider_base_url: Some("https://api.example.test/v1".to_string()),
            max_output_tokens: 64,
            max_evidence_age_seconds: 86_400,
        };
        let report = crate::cli::canary_publication_preflight_report_with_env(&args, &|_| None);

        assert!(!report.ok);
        assert_eq!(report.config.snapshot_base_uri, args.snapshot_base_uri);
        assert_eq!(report.config.artifact_base_uri, args.artifact_base_uri);
        assert_eq!(
            report.config.provider_base_url,
            "https://api.example.test/v1"
        );
        assert!(report.checks.iter().any(|check| {
            check.name == "providerCredential"
                && check.status == crate::cli::CanaryPublicationPreflightStatus::Failed
        }));
        assert!(report.checks.iter().any(|check| {
            check.name == "snapshotBaseUri"
                && check.status == crate::cli::CanaryPublicationPreflightStatus::Failed
                && check.message.contains("HTTP object-store driver")
        }));
        assert!(report.checks.iter().any(|check| {
            check.name == "artifactBaseUri"
                && check.status == crate::cli::CanaryPublicationPreflightStatus::Failed
                && check.message.contains("HTTP object-store driver")
        }));
        assert!(report.checks.iter().any(|check| {
            check.name == "remoteObjectStoreAuth"
                && check.status == crate::cli::CanaryPublicationPreflightStatus::Warning
        }));
    }

    #[test]
    fn cli_canary_proof_accepts_live_http_evidence_bundle() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        let proof_path = evidence_dir.path().join("proof.json");

        let code = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap();

        assert_eq!(code, 0);
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(proof.ok);
        assert!(proof.failures.is_empty());
        assert_eq!(proof.workflow_expectation.event_name, "schedule");
        assert_eq!(proof.workflow_expectation.workflow, "Muzen Canary Evidence");
        assert_eq!(proof.workflow_expectation.job, "publish-canary-evidence");
        assert_eq!(
            proof.workflow_expectation.repository,
            Some("heimdaal/review".to_string())
        );
        assert_eq!(
            proof.workflow_expectation.git_ref,
            Some("refs/heads/main".to_string())
        );
        assert_eq!(proof.file_digests.len(), 8);
        assert!(proof.file_digests.iter().all(|digest| digest.bytes > 0));
        let manifest_digest = proof
            .file_digests
            .iter()
            .find(|digest| digest.label == "manifest")
            .expect("manifest digest");
        let manifest_bytes = fs::read(evidence_dir.path().join("manifest.json")).unwrap();
        assert_eq!(manifest_digest.file, "manifest.json");
        assert_eq!(manifest_digest.bytes, manifest_bytes.len() as u64);
        assert_eq!(
            manifest_digest.blake3,
            blake3::hash(&manifest_bytes).to_hex().to_string()
        );
        assert_eq!(
            proof
                .publication
                .expect("publication report")
                .provider_evidence_source,
            crate::cli::CanaryProviderEvidenceSource::LiveProviderCanary
        );
    }

    #[test]
    fn cli_canary_proof_rejects_reused_provider_evidence_bundle() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        let publication_path = evidence_dir.path().join("publication.json");
        let mut publication: crate::cli::CanaryPublicationReport =
            serde_json::from_str(&fs::read_to_string(&publication_path).unwrap()).unwrap();
        publication.provider_evidence_source =
            crate::cli::CanaryProviderEvidenceSource::ReusedEvidenceFile;
        publication.provider_evidence_input = Some("model-provider.json".to_string());
        write_test_json(&publication_path, &publication);
        let proof_path = evidence_dir.path().join("proof.json");

        let error = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap_err()
        .to_string();

        assert!(error.contains("canary proof failed"));
        assert!(error.contains("provider evidence source"));
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(!proof.ok);
        assert!(proof
            .failures
            .iter()
            .any(|failure| failure.contains("provider evidence source")));
    }

    #[test]
    fn cli_canary_proof_rejects_reused_provider_preflight_shape() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        let reused_preflight_args = crate::cli::CanaryPublishArgs {
            output_dir: evidence_dir.path().to_path_buf(),
            provider_evidence: Some(evidence_dir.path().join("model-provider.json")),
            snapshot_base_uri: "memory://muzen-canaries/snapshots".to_string(),
            artifact_base_uri: "memory://muzen-canaries/artifacts".to_string(),
            object_store_driver: crate::cli::RemoteObjectStoreCanaryDriver::Memory,
            object_store_bearer_token_env: "MUZEN_TEST_UNUSED_BEARER_TOKEN".to_string(),
            model: "canary-model".to_string(),
            provider_base_url: Some("https://example.invalid/v1".to_string()),
            max_output_tokens: 64,
            max_evidence_age_seconds: 86_400,
        };
        let reused_preflight = crate::cli::canary_publication_preflight_report_with_env(
            &reused_preflight_args,
            &|_| None,
        );
        assert!(reused_preflight.ok);
        write_test_json(
            &evidence_dir.path().join("preflight.json"),
            &reused_preflight,
        );
        let proof_path = evidence_dir.path().join("proof.json");

        let error = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap_err()
        .to_string();

        assert!(error.contains("canary proof failed"));
        assert!(error.contains("providerEvidence"));
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(!proof.ok);
        assert!(proof
            .failures
            .iter()
            .any(|failure| failure.contains("providerEvidence")));
    }

    #[test]
    fn cli_canary_proof_rejects_preflight_config_mismatch() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        let preflight_path = evidence_dir.path().join("preflight.json");
        let mut preflight: crate::cli::CanaryPublicationPreflightReport =
            serde_json::from_str(&fs::read_to_string(&preflight_path).unwrap()).unwrap();
        preflight.config.snapshot_base_uri =
            "https://objects.example.test/different-snapshots".to_string();
        write_test_json(&preflight_path, &preflight);
        let proof_path = evidence_dir.path().join("proof.json");

        let error = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap_err()
        .to_string();

        assert!(error.contains("canary proof failed"));
        assert!(error.contains("preflight snapshot base URI"));
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(!proof.ok);
        assert!(proof
            .failures
            .iter()
            .any(|failure| failure.contains("preflight snapshot base URI")));
    }

    #[test]
    fn cli_canary_proof_rejects_stale_preflight_report() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        let preflight_path = evidence_dir.path().join("preflight.json");
        let mut preflight: crate::cli::CanaryPublicationPreflightReport =
            serde_json::from_str(&fs::read_to_string(&preflight_path).unwrap()).unwrap();
        preflight.generated_at_utc = "1000.000000000Z".to_string();
        write_test_json(&preflight_path, &preflight);
        let proof_path = evidence_dir.path().join("proof.json");

        let error = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap_err()
        .to_string();

        assert!(error.contains("canary proof failed"));
        assert!(error.contains("canary preflight report is stale"));
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(!proof.ok);
        assert!(proof
            .failures
            .iter()
            .any(|failure| failure.contains("canary preflight report is stale")));
    }

    #[test]
    fn cli_canary_proof_rejects_missing_workflow_provenance() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        fs::remove_file(evidence_dir.path().join("workflow.json")).unwrap();
        let proof_path = evidence_dir.path().join("proof.json");

        let error = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap_err()
        .to_string();

        assert!(error.contains("canary proof failed"));
        assert!(error.contains("failed to read canary workflow provenance"));
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(!proof.ok);
        assert!(proof.workflow.is_none());
    }

    #[test]
    fn cli_canary_proof_rejects_manual_workflow_dispatch_provenance() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        let workflow_path = evidence_dir.path().join("workflow.json");
        let mut workflow: crate::cli::CanaryWorkflowProvenance =
            serde_json::from_str(&fs::read_to_string(&workflow_path).unwrap()).unwrap();
        workflow.event_name = "workflow_dispatch".to_string();
        write_test_json(&workflow_path, &workflow);
        let proof_path = evidence_dir.path().join("proof.json");

        let error = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap_err()
        .to_string();

        assert!(error.contains("canary proof failed"));
        assert!(error.contains("workflow event must be schedule"));
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(!proof.ok);
        assert_eq!(
            proof.workflow.expect("workflow provenance").event_name,
            "workflow_dispatch"
        );
    }

    #[test]
    fn cli_canary_proof_rejects_wrong_workflow_job_provenance() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        let workflow_path = evidence_dir.path().join("workflow.json");
        let mut workflow: crate::cli::CanaryWorkflowProvenance =
            serde_json::from_str(&fs::read_to_string(&workflow_path).unwrap()).unwrap();
        workflow.job = "manual-canary".to_string();
        write_test_json(&workflow_path, &workflow);
        let proof_path = evidence_dir.path().join("proof.json");

        let error = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap_err()
        .to_string();

        assert!(error.contains("canary proof failed"));
        assert!(error.contains("workflow job must be publish-canary-evidence"));
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(!proof.ok);
        assert_eq!(
            proof.workflow.expect("workflow provenance").job,
            "manual-canary"
        );
    }

    #[test]
    fn cli_canary_proof_rejects_wrong_workflow_repository_provenance() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        let workflow_path = evidence_dir.path().join("workflow.json");
        let mut workflow: crate::cli::CanaryWorkflowProvenance =
            serde_json::from_str(&fs::read_to_string(&workflow_path).unwrap()).unwrap();
        workflow.repository = "forked/review".to_string();
        workflow.run_url = "https://github.com/forked/review/actions/runs/1234567890".to_string();
        write_test_json(&workflow_path, &workflow);
        let proof_path = evidence_dir.path().join("proof.json");

        let error = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap_err()
        .to_string();

        assert!(error.contains("canary proof failed"));
        assert!(error.contains("workflow repository must be heimdaal/review"));
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(!proof.ok);
        assert!(proof
            .failures
            .iter()
            .any(|failure| failure.contains("workflow repository must be heimdaal/review")));
    }

    #[test]
    fn cli_canary_proof_rejects_wrong_workflow_git_ref_provenance() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        let workflow_path = evidence_dir.path().join("workflow.json");
        let mut workflow: crate::cli::CanaryWorkflowProvenance =
            serde_json::from_str(&fs::read_to_string(&workflow_path).unwrap()).unwrap();
        workflow.git_ref = "refs/heads/feature-canary".to_string();
        write_test_json(&workflow_path, &workflow);
        let proof_path = evidence_dir.path().join("proof.json");

        let error = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap_err()
        .to_string();

        assert!(error.contains("canary proof failed"));
        assert!(error.contains("workflow git ref must be refs/heads/main"));
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(!proof.ok);
        assert!(proof
            .failures
            .iter()
            .any(|failure| failure.contains("workflow git ref must be refs/heads/main")));
    }

    #[test]
    fn cli_canary_proof_rejects_wrong_workflow_run_url() {
        let evidence_dir = tempfile::tempdir().unwrap();
        write_passing_live_canary_proof_bundle(evidence_dir.path());
        let workflow_path = evidence_dir.path().join("workflow.json");
        let mut workflow: crate::cli::CanaryWorkflowProvenance =
            serde_json::from_str(&fs::read_to_string(&workflow_path).unwrap()).unwrap();
        workflow.run_url = "https://github.com/heimdaal/review/actions/runs/9999999999".to_string();
        write_test_json(&workflow_path, &workflow);
        let proof_path = evidence_dir.path().join("proof.json");

        let error = crate::cli::run_canary_proof(canary_proof_args(
            evidence_dir.path(),
            proof_path.clone(),
        ))
        .unwrap_err()
        .to_string();

        assert!(error.contains("canary proof failed"));
        assert!(error.contains("workflow run URL must be"));
        let proof: crate::cli::CanaryProofReport =
            serde_json::from_str(&fs::read_to_string(&proof_path).unwrap()).unwrap();
        assert!(!proof.ok);
        assert!(proof
            .failures
            .iter()
            .any(|failure| failure.contains("workflow run URL must be")));
    }

    #[test]
    fn cli_canary_workflow_provenance_writes_github_actions_env() {
        let evidence_dir = tempfile::tempdir().unwrap();
        let workflow_path = evidence_dir.path().join("workflow.json");

        crate::cli::run_canary_workflow_provenance_with_env(
            crate::cli::CanaryWorkflowProvenanceArgs {
                output: Some(workflow_path.clone()),
            },
            &github_actions_canary_env,
        )
        .unwrap();

        let workflow: crate::cli::CanaryWorkflowProvenance =
            serde_json::from_str(&fs::read_to_string(&workflow_path).unwrap()).unwrap();
        assert_eq!(workflow.schema_version, "muzen.canary-workflow.v1");
        assert!(workflow.generated_at_utc.ends_with('Z'));
        assert_eq!(workflow.event_name, "schedule");
        assert_eq!(workflow.workflow, "Muzen Canary Evidence");
        assert_eq!(workflow.job, "publish-canary-evidence");
        assert_eq!(workflow.run_id, "1234567890");
        assert_eq!(workflow.run_attempt, "1");
        assert_eq!(workflow.repository, "heimdaal/review");
        assert_eq!(workflow.git_ref, "refs/heads/main");
        assert_eq!(workflow.sha, "0123456789abcdef0123456789abcdef01234567");
        assert_eq!(workflow.actor, "github-actions[bot]");
        assert_eq!(workflow.server_url, "https://github.com");
        assert_eq!(
            workflow.run_url,
            "https://github.com/heimdaal/review/actions/runs/1234567890"
        );
    }

    #[test]
    fn cli_canary_verify_gates_published_manifest_files() {
        let evidence_dir = tempfile::tempdir().unwrap();
        let manifest_path = evidence_dir.path().join("manifest.json");
        let stale_manifest_path = evidence_dir.path().join("stale-manifest.json");
        let fresh_manifest = current_passing_canary_manifest();
        crate::reviewer::canaries::export_canary_evidence_manifest(&manifest_path, &fresh_manifest)
            .unwrap();

        let code = crate::cli::run_canary_verify(CanaryVerifyArgs {
            manifest: manifest_path,
            max_evidence_age_seconds: 86_400,
        })
        .unwrap();
        assert_eq!(code, 0);

        let stale_manifest = passing_canary_manifest_at("1000.000000000Z");
        crate::reviewer::canaries::export_canary_evidence_manifest(
            &stale_manifest_path,
            &stale_manifest,
        )
        .unwrap();
        let error = crate::cli::run_canary_verify(CanaryVerifyArgs {
            manifest: stale_manifest_path,
            max_evidence_age_seconds: 1,
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("canary evidence manifest is stale"));
    }

    #[test]
    fn cli_canary_status_reports_published_manifest_state() {
        let evidence_dir = tempfile::tempdir().unwrap();
        let manifest_path = evidence_dir.path().join("manifest.json");
        let status_path = evidence_dir.path().join("status.json");
        let stale_manifest_path = evidence_dir.path().join("stale-manifest.json");
        let stale_status_path = evidence_dir.path().join("stale-status.json");
        let fresh_manifest = current_passing_canary_manifest();
        crate::reviewer::canaries::export_canary_evidence_manifest(&manifest_path, &fresh_manifest)
            .unwrap();

        let code = crate::cli::run_canary_status(crate::cli::CanaryStatusArgs {
            manifest: manifest_path,
            output: Some(status_path.clone()),
            max_evidence_age_seconds: 86_400,
        })
        .unwrap();
        assert_eq!(code, 0);
        let status: crate::reviewer::canaries::CanaryEvidenceStatusReport =
            serde_json::from_str(&fs::read_to_string(&status_path).unwrap()).unwrap();
        assert!(status.ok);
        assert!(status.evidence.model_provider.present);
        assert_eq!(
            status.evidence.model_provider.required_protocols,
            crate::reviewer::canaries::openai_provider_canary_protocols().to_vec()
        );
        assert_eq!(
            status.evidence.model_provider.passed_protocols,
            crate::reviewer::canaries::openai_provider_canary_protocols().to_vec()
        );
        let snapshot_status = status
            .evidence
            .remote_object_stores
            .iter()
            .find(|evidence| {
                evidence.target
                    == crate::reviewer::canaries::RemoteObjectStoreCanaryTarget::Snapshot
            })
            .expect("snapshot evidence status");
        assert_eq!(snapshot_status.evidence_count, 1);
        assert!(snapshot_status.gate.as_ref().expect("snapshot gate").valid);
        assert!(snapshot_status
            .base_uri
            .as_deref()
            .expect("snapshot base uri")
            .contains("snapshots"));
        let artifact_status = status
            .evidence
            .remote_object_stores
            .iter()
            .find(|evidence| {
                evidence.target
                    == crate::reviewer::canaries::RemoteObjectStoreCanaryTarget::Artifact
            })
            .expect("artifact evidence status");
        assert_eq!(artifact_status.evidence_count, 1);
        assert!(artifact_status.gate.as_ref().expect("artifact gate").valid);

        let stale_manifest = passing_canary_manifest_at("1000.000000000Z");
        crate::reviewer::canaries::export_canary_evidence_manifest(
            &stale_manifest_path,
            &stale_manifest,
        )
        .unwrap();
        let error = crate::cli::run_canary_status(crate::cli::CanaryStatusArgs {
            manifest: stale_manifest_path,
            output: Some(stale_status_path.clone()),
            max_evidence_age_seconds: 1,
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("canary evidence manifest status failed"));
        assert!(error.contains("canary evidence manifest is stale"));
        let stale_status: crate::reviewer::canaries::CanaryEvidenceStatusReport =
            serde_json::from_str(&fs::read_to_string(&stale_status_path).unwrap()).unwrap();
        assert!(!stale_status.ok);
        assert!(stale_status
            .freshness_failures
            .iter()
            .any(|failure| failure.contains("canary evidence manifest is stale")));
    }

    fn passing_model_provider_canary_evidence(
    ) -> crate::reviewer::canaries::ModelProviderCanaryEvidence {
        passing_model_provider_canary_evidence_at(&crate::util::timestamp_utc())
    }

    fn current_passing_canary_manifest() -> crate::reviewer::canaries::CanaryEvidenceManifest {
        let now = crate::util::timestamp_utc();
        passing_canary_manifest_at(&now)
    }

    fn passing_canary_manifest_at(
        generated_at_utc: &str,
    ) -> crate::reviewer::canaries::CanaryEvidenceManifest {
        let model_provider = passing_model_provider_canary_evidence_at(generated_at_utc);
        let snapshot_client =
            Arc::new(crate::reviewer::InMemoryRemoteSnapshotObjectClient::default());
        let artifact_client =
            Arc::new(crate::reviewer::InMemoryRemoteArtifactObjectClient::default());
        let mut snapshot = crate::reviewer::canaries::run_remote_snapshot_object_store_canary(
            "s3://muzen-test-snapshots/canary",
            snapshot_client.as_ref(),
        );
        let mut artifact = crate::reviewer::canaries::run_remote_artifact_object_store_canary(
            "s3://muzen-test-artifacts/canary",
            artifact_client.as_ref(),
        );
        snapshot.generated_at_utc = generated_at_utc.to_string();
        artifact.generated_at_utc = generated_at_utc.to_string();
        crate::reviewer::canaries::CanaryEvidenceManifest::with_generated_at(
            generated_at_utc,
            Some(model_provider),
            vec![snapshot, artifact],
        )
    }

    fn passing_model_provider_canary_evidence_at(
        generated_at_utc: &str,
    ) -> crate::reviewer::canaries::ModelProviderCanaryEvidence {
        let reports = crate::reviewer::canaries::openai_provider_canary_protocols()
            .iter()
            .map(
                |protocol| crate::reviewer::canaries::ModelProviderCanaryReport {
                    protocol: *protocol,
                    base_url: "https://example.invalid/v1".to_string(),
                    model: "canary-model".to_string(),
                    credential_ref: "env:OPENAI_API_KEY".to_string(),
                    status: crate::reviewer::canaries::ModelProviderCanaryStatus::Passed,
                },
            )
            .collect::<Vec<_>>();
        crate::reviewer::canaries::ModelProviderCanaryEvidence::with_generated_at(
            generated_at_utc,
            reports,
        )
    }

    fn canary_proof_args(evidence_dir: &Path, proof_path: PathBuf) -> crate::cli::CanaryProofArgs {
        crate::cli::CanaryProofArgs {
            evidence_dir: evidence_dir.to_path_buf(),
            output: Some(proof_path),
            max_evidence_age_seconds: 86_400,
            expected_workflow: "Muzen Canary Evidence".to_string(),
            expected_job: "publish-canary-evidence".to_string(),
            expected_repository: Some("heimdaal/review".to_string()),
            expected_git_ref: Some("refs/heads/main".to_string()),
        }
    }

    fn write_passing_live_canary_proof_bundle(evidence_dir: &Path) {
        fs::create_dir_all(evidence_dir).unwrap();
        write_test_json(
            &evidence_dir.join("workflow.json"),
            &crate::cli::canary_workflow_provenance_from_env(&github_actions_canary_env),
        );
        let args = crate::cli::CanaryPublishArgs {
            output_dir: evidence_dir.to_path_buf(),
            provider_evidence: None,
            snapshot_base_uri: "https://objects.example.test/snapshots".to_string(),
            artifact_base_uri: "https://objects.example.test/artifacts".to_string(),
            object_store_driver: crate::cli::RemoteObjectStoreCanaryDriver::Http,
            object_store_bearer_token_env: "MUZEN_REMOTE_OBJECT_STORE_BEARER_TOKEN".to_string(),
            model: "canary-model".to_string(),
            provider_base_url: Some("https://example.invalid/v1".to_string()),
            max_output_tokens: 64,
            max_evidence_age_seconds: 86_400,
        };
        let preflight =
            crate::cli::canary_publication_preflight_report_with_env(&args, &|name| match name {
                "OPENAI_API_KEY" | "MUZEN_REMOTE_OBJECT_STORE_BEARER_TOKEN" => {
                    Some("configured".to_string())
                }
                _ => None,
            });
        assert!(preflight.ok);
        write_test_json(&evidence_dir.join("preflight.json"), &preflight);

        let provider = passing_model_provider_canary_evidence();
        crate::reviewer::canaries::export_model_provider_canary_evidence(
            evidence_dir.join("model-provider.json"),
            &provider,
        )
        .unwrap();

        let snapshot_client = crate::reviewer::InMemoryRemoteSnapshotObjectClient::default();
        let artifact_client = crate::reviewer::InMemoryRemoteArtifactObjectClient::default();
        let snapshot = crate::reviewer::canaries::run_remote_snapshot_object_store_canary(
            &args.snapshot_base_uri,
            &snapshot_client,
        );
        let artifact = crate::reviewer::canaries::run_remote_artifact_object_store_canary(
            &args.artifact_base_uri,
            &artifact_client,
        );
        crate::reviewer::canaries::export_remote_object_store_canary_evidence(
            evidence_dir.join("remote-snapshot-object-store.json"),
            &snapshot,
        )
        .unwrap();
        crate::reviewer::canaries::export_remote_object_store_canary_evidence(
            evidence_dir.join("remote-artifact-object-store.json"),
            &artifact,
        )
        .unwrap();

        let manifest = crate::reviewer::canaries::CanaryEvidenceManifest::from_evidence(
            Some(provider),
            vec![snapshot, artifact],
        );
        crate::reviewer::canaries::export_canary_evidence_manifest(
            evidence_dir.join("manifest.json"),
            &manifest,
        )
        .unwrap();
        let status = manifest.status_report(
            &crate::reviewer::canaries::CanaryEvidenceFreshnessPolicy::current(86_400),
        );
        assert!(status.ok);
        write_test_json(&evidence_dir.join("status.json"), &status);
        let publication = crate::cli::canary_publication_report(&args, &status);
        write_test_json(&evidence_dir.join("publication.json"), &publication);
    }

    fn github_actions_canary_env(name: &str) -> Option<String> {
        match name {
            "GITHUB_EVENT_NAME" => Some("schedule".to_string()),
            "GITHUB_WORKFLOW" => Some("Muzen Canary Evidence".to_string()),
            "GITHUB_JOB" => Some("publish-canary-evidence".to_string()),
            "GITHUB_RUN_ID" => Some("1234567890".to_string()),
            "GITHUB_RUN_ATTEMPT" => Some("1".to_string()),
            "GITHUB_REPOSITORY" => Some("heimdaal/review".to_string()),
            "GITHUB_REF" => Some("refs/heads/main".to_string()),
            "GITHUB_SHA" => Some("0123456789abcdef0123456789abcdef01234567".to_string()),
            "GITHUB_ACTOR" => Some("github-actions[bot]".to_string()),
            "GITHUB_SERVER_URL" => Some("https://github.com".to_string()),
            _ => None,
        }
    }

    fn write_test_json<T: serde::Serialize>(path: &Path, value: &T) {
        let mut bytes = serde_json::to_vec_pretty(value).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn public_reviewer_facade_runs_multiple_snapshots() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        fs::write(first.path().join("README.md"), "needle in first\n").unwrap();
        fs::write(second.path().join("README.md"), "needle in second\n").unwrap();
        let first_id = crate::reviewer::ids::SnapshotId("snapshot-first".to_string());
        let second_id = crate::reviewer::ids::SnapshotId("snapshot-second".to_string());
        let first_snapshot = crate::reviewer::SnapshotSpec::new(
            first.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-first",
                "head-first",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        )
        .with_snapshot_id(first_id.clone())
        .with_path_policy(crate::reviewer::SnapshotPathPolicy::standard(64 * 1024, 20));
        let second_snapshot = crate::reviewer::SnapshotSpec::new(
            second.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-second",
                "head-second",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        )
        .with_snapshot_id(second_id.clone())
        .with_path_policy(crate::reviewer::SnapshotPathPolicy::standard(64 * 1024, 20));
        let sessions = vec![
            crate::reviewer::ReviewSessionSpec::review_read_only(
                "first-session",
                crate::reviewer::Role::Generalist,
                "Review first snapshot.",
                public_budget(),
            )
            .with_snapshot_id(first_id.clone()),
            crate::reviewer::ReviewSessionSpec::review_read_only(
                "second-session",
                crate::reviewer::Role::Generalist,
                "Review second snapshot.",
                public_budget(),
            )
            .with_snapshot_id(second_id.clone()),
        ];
        let spec = crate::reviewer::RunSpec {
            run_id: "multi-snapshot-run".to_string(),
            snapshots: vec![first_snapshot, second_snapshot],
            sessions,
            limits: crate::reviewer::ReviewRunLimits::standard(2, 64 * 1024, 20),
        };
        let events = Arc::new(crate::reviewer::InMemoryReviewEventSink::default());
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicFacadeModel {
                path: "README.md".to_string(),
                query: "needle".to_string(),
            }))
            .review_event_sink(events.clone())
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());

        assert_eq!(report.snapshots.len(), 2);
        assert_eq!(report.summary.sessions, 2);
        assert_eq!(report.summary.completed_sessions, 2);
        assert_eq!(report.summary.findings, 2);
        assert_eq!(report.summary.snapshot_count, 2);
        assert_eq!(report.metrics.snapshot_metrics.len(), 2);
        assert!(report
            .metrics
            .snapshot_metrics
            .iter()
            .any(|metrics| metrics.snapshot_id == first_id && metrics.sessions == 1));
        assert!(report
            .metrics
            .snapshot_metrics
            .iter()
            .any(|metrics| metrics.snapshot_id == second_id && metrics.sessions == 1));
        let artifact_text = report
            .artifacts
            .list()
            .into_iter()
            .map(|artifact| artifact.content)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(artifact_text.contains("needle in first"));
        assert!(artifact_text.contains("needle in second"));
        let event_records = events.records();
        assert!(event_records
            .iter()
            .all(|record| record.run_id.as_deref() == Some("multi-snapshot-run")));
        let events = events.events();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    crate::reviewer::ReviewEvent::SnapshotStarted { .. }
                ))
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    crate::reviewer::ReviewEvent::SnapshotFinished { .. }
                ))
                .count(),
            2
        );
    }

    #[test]
    fn public_reviewer_facade_runs_custom_tool_and_exports_metrics() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let mut registry = crate::reviewer::ReviewToolRegistry::review_defaults().unwrap();
        let custom_tool_id = registry
            .register_read_only_tool(
                "host_custom_check",
                "Host engine supplied custom reviewer check.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                }),
                false,
                Arc::new(EchoCustomTool),
            )
            .unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-1",
                "head-1",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        );
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "custom-session",
            crate::reviewer::Role::Generalist,
            "Run host custom check.",
            public_budget(),
        )
        .with_model_profile_id("mock")
        .grant_custom_read_only_tool(custom_tool_id.clone());
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "public-custom-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicCustomToolModel(custom_tool_id.clone())))
            .review_tool_registry(registry)
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());

        assert_eq!(report.summary.completed_sessions, 1);
        let custom_metrics =
            &report.metrics.tool_metrics[&ToolMetricKey::in_process(&custom_tool_id)];
        assert_eq!(custom_metrics.calls, 1);
        assert_eq!(custom_metrics.successes, 1);
        assert_eq!(custom_metrics.artifacts, 1);
        assert!(custom_metrics.input_bytes > 0);
        assert!(custom_metrics.output_bytes > 0);
        assert!(custom_metrics.latency_ms >= custom_metrics.max_latency_ms);
        assert!(custom_metrics.max_latency_ms > 0);
        let in_process_health = report
            .metrics
            .provider_health
            .iter()
            .find(|health| health.provider_id == ToolProviderId::in_process())
            .unwrap();
        assert_eq!(
            in_process_health.state,
            crate::reviewer::tool_adapters::ToolProviderHealthState::Healthy
        );
        assert_eq!(in_process_health.calls, 1);
        assert_eq!(in_process_health.errors, 0);
        let artifact_text = report
            .artifacts
            .list()
            .into_iter()
            .map(|artifact| artifact.content)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(artifact_text.contains("[REDACTED]"));
        assert!(!artifact_text.contains("AKIA1234567890ABCDEF"));
        let mut raw_artifact_capabilities =
            crate::reviewer::capabilities::CapabilitySet::review_read_only();
        raw_artifact_capabilities.artifact_access.read_raw = true;
        let raw_artifact_text = report
            .export_artifacts(
                crate::reviewer::ArtifactExportPolicy::raw(&raw_artifact_capabilities).unwrap(),
            )
            .unwrap()
            .artifacts
            .into_iter()
            .map(|artifact| artifact.content)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(raw_artifact_text.contains("AKIA1234567890ABCDEF"));
    }

    #[test]
    fn public_reviewer_facade_passes_provider_resources_to_scoped_host_tool() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let resource_id = ProviderResourceId::parse("github/org-a/repo-a").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = crate::reviewer::ReviewToolRegistry::review_defaults().unwrap();
        let custom_tool_id = registry
            .register_scoped_read_only_tool(
                "host_resource_scoped_check",
                "Host custom check scoped to one external resource.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                }),
                false,
                vec![resource_id.clone()],
                Arc::new(ResourceScopedReviewTool {
                    expected_provider_resources: vec![resource_id.clone()],
                    calls: Arc::clone(&calls),
                }),
            )
            .unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-host-resource",
                "head-host-resource",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        );
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "host-resource-session",
            crate::reviewer::Role::Generalist,
            "Run host custom check with a provider resource.",
            public_budget(),
        )
        .with_model_profile_id("mock")
        .grant_custom_read_only_tool_for_resources(custom_tool_id.clone(), vec![resource_id]);
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "public-host-resource-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicCustomToolModel(custom_tool_id)))
            .review_tool_registry(registry)
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());

        assert_eq!(report.summary.completed_sessions, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn public_reviewer_facade_denies_host_tool_outside_provider_resource_scope() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let allowed_resource = ProviderResourceId::parse("github/org-a/repo-a").unwrap();
        let denied_resource = ProviderResourceId::parse("github/org-b/repo-b").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = crate::reviewer::ReviewToolRegistry::review_defaults().unwrap();
        let custom_tool_id = registry
            .register_scoped_read_only_tool(
                "host_resource_denied_check",
                "Host custom check scoped to a different external resource.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                }),
                false,
                vec![allowed_resource.clone()],
                Arc::new(ResourceScopedReviewTool {
                    expected_provider_resources: vec![allowed_resource],
                    calls: Arc::clone(&calls),
                }),
            )
            .unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-host-resource-denied",
                "head-host-resource-denied",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        );
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "host-resource-denied-session",
            crate::reviewer::Role::Generalist,
            "Run host custom check outside provider resource scope.",
            public_budget(),
        )
        .with_model_profile_id("mock")
        .grant_custom_read_only_tool_for_resources(custom_tool_id.clone(), vec![denied_resource]);
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "public-host-resource-denied-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let events = Arc::new(crate::reviewer::InMemoryReviewEventSink::default());
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicCustomToolModel(custom_tool_id)))
            .review_tool_registry(registry)
            .review_event_sink(events.clone())
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _report = tokio.block_on(run.execute());

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(events.records().iter().any(|record| matches!(
            &record.event,
            crate::reviewer::ReviewEvent::ToolCallDenied {
                error_code: crate::reviewer::tool_adapters::ToolErrorCode::ToolNotAllowed,
                reason,
                ..
            } if reason.contains("provider resource")
                && record.run_id.as_deref() == Some("public-host-resource-denied-run")
                && record.session_id.as_deref() == Some("host-resource-denied-session")
        )));
    }

    #[test]
    fn public_reviewer_facade_runs_scoped_jsonrpc_provider_tool() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let provider_id =
            crate::reviewer::tool_adapters::ToolProviderId::parse("public_jsonrpc_provider")
                .unwrap();
        let resource_id =
            crate::reviewer::tool_adapters::ProviderResourceId::parse("github/org-a/repo-a")
                .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = crate::reviewer::ReviewToolRegistry::review_defaults().unwrap();
        let tool_id = registry
            .register_scoped_jsonrpc_read_only_tool(
                crate::reviewer::ReviewJsonRpcReadOnlyToolRegistration {
                    provider_id: provider_id.clone(),
                    id: "public_jsonrpc_check".to_string(),
                    description: "External JSON-RPC check scoped to one provider resource."
                        .to_string(),
                    parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                    }),
                    cacheable: false,
                    provider_resources: vec![resource_id.clone()],
                    transport: Arc::new(PublicJsonRpcReviewTool {
                        provider_id: provider_id.clone(),
                        tool_id: "public_jsonrpc_check".to_string(),
                        expected_provider_resources: vec![resource_id.clone()],
                        calls: Arc::clone(&calls),
                    }),
                },
            )
            .unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-public-jsonrpc",
                "head-public-jsonrpc",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        );
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "public-jsonrpc-session",
            crate::reviewer::Role::Generalist,
            "Run public JSON-RPC provider check.",
            public_budget(),
        )
        .with_model_profile_id("mock")
        .grant_provider_read_only_tool_for_resources(
            provider_id.clone(),
            tool_id.clone(),
            vec![resource_id],
        );
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "public-jsonrpc-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicCustomToolModel(tool_id.clone())))
            .review_tool_registry(registry)
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());

        assert_eq!(report.summary.completed_sessions, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let metric_key = crate::reviewer::tool_adapters::ToolMetricKey::new(&provider_id, &tool_id);
        let metrics = &report.metrics.tool_metrics[&metric_key];
        assert_eq!(metrics.calls, 1);
        assert_eq!(metrics.successes, 1);
        let provider_health = report
            .metrics
            .provider_health
            .iter()
            .find(|health| health.provider_id == provider_id)
            .unwrap();
        assert_eq!(
            provider_health.state,
            crate::reviewer::tool_adapters::ToolProviderHealthState::Healthy
        );
    }

    #[test]
    fn public_reviewer_facade_runs_http_jsonrpc_provider_tool() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let provider_id =
            crate::reviewer::tool_adapters::ToolProviderId::parse("public_http_jsonrpc_provider")
                .unwrap();
        let resource_id =
            crate::reviewer::tool_adapters::ProviderResourceId::parse("github/org-http/repo")
                .unwrap();
        let server = LoopbackJsonRpcToolServer::spawn();
        let transport =
            crate::reviewer::tool_adapters::HttpJsonRpcToolTransport::new(server.endpoint())
                .unwrap();
        let mut registry = crate::reviewer::ReviewToolRegistry::review_defaults().unwrap();
        let tool_id = registry
            .register_scoped_jsonrpc_read_only_tool(
                crate::reviewer::ReviewJsonRpcReadOnlyToolRegistration {
                    provider_id: provider_id.clone(),
                    id: "public_http_jsonrpc_check".to_string(),
                    description: "External HTTP JSON-RPC check scoped to one provider resource."
                        .to_string(),
                    parameters: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "value": { "type": "string" }
                        },
                        "required": ["value"],
                        "additionalProperties": false
                    }),
                    cacheable: false,
                    provider_resources: vec![resource_id.clone()],
                    transport: Arc::new(transport),
                },
            )
            .unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-public-http-jsonrpc",
                "head-public-http-jsonrpc",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        );
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "public-http-jsonrpc-session",
            crate::reviewer::Role::Generalist,
            "Run public HTTP JSON-RPC provider check.",
            public_budget(),
        )
        .with_model_profile_id("mock")
        .grant_provider_read_only_tool_for_resources(
            provider_id.clone(),
            tool_id.clone(),
            vec![resource_id.clone()],
        );
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "public-http-jsonrpc-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicCustomToolModel(tool_id.clone())))
            .review_tool_registry(registry)
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());
        let wire_request = server.join();

        assert_eq!(report.summary.completed_sessions, 1);
        assert_eq!(wire_request["jsonrpc"], "2.0");
        assert_eq!(wire_request["method"], "tool.call");
        assert_eq!(wire_request["params"]["providerId"], provider_id.as_str());
        assert_eq!(wire_request["params"]["toolId"], tool_id.as_str());
        assert_eq!(
            wire_request["params"]["providerResources"],
            serde_json::json!([resource_id.as_str()])
        );
        assert_eq!(wire_request["params"]["arguments"]["value"], "ok");
        let metric_key = crate::reviewer::tool_adapters::ToolMetricKey::new(&provider_id, &tool_id);
        let metrics = &report.metrics.tool_metrics[&metric_key];
        assert_eq!(metrics.calls, 1);
        assert_eq!(metrics.successes, 1);
    }

    #[test]
    fn public_reviewer_facade_runs_jsonrpc_network_read_tool_with_authority() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let provider_id = crate::reviewer::tool_adapters::ToolProviderId::parse(
            "public_jsonrpc_network_provider",
        )
        .unwrap();
        let resource_id =
            crate::reviewer::tool_adapters::ProviderResourceId::parse("github/org-network/repo")
                .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = crate::reviewer::ReviewToolRegistry::review_defaults().unwrap();
        let tool_id = registry
            .register_scoped_jsonrpc_network_read_tool(
                crate::reviewer::ReviewJsonRpcNetworkReadToolRegistration {
                    provider_id: provider_id.clone(),
                    id: "public_jsonrpc_network_check".to_string(),
                    description: "External JSON-RPC check that needs network read.".to_string(),
                    parameters: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "value": { "type": "string" }
                        },
                        "required": ["value"],
                        "additionalProperties": false
                    }),
                    cacheable: false,
                    provider_resources: vec![resource_id.clone()],
                    transport: Arc::new(PublicJsonRpcReviewTool {
                        provider_id: provider_id.clone(),
                        tool_id: "public_jsonrpc_network_check".to_string(),
                        expected_provider_resources: vec![resource_id.clone()],
                        calls: Arc::clone(&calls),
                    }),
                },
            )
            .unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-public-jsonrpc-network",
                "head-public-jsonrpc-network",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        );
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "public-jsonrpc-network-session",
            crate::reviewer::Role::Generalist,
            "Run public JSON-RPC provider network check.",
            public_budget(),
        )
        .with_model_profile_id("mock")
        .grant_provider_network_read_tool_for_resources(
            provider_id.clone(),
            tool_id.clone(),
            vec![resource_id],
        );
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "public-jsonrpc-network-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicCustomToolModel(tool_id.clone())))
            .review_tool_registry(registry)
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());

        assert_eq!(report.summary.completed_sessions, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let metric_key = crate::reviewer::tool_adapters::ToolMetricKey::new(&provider_id, &tool_id);
        let metrics = &report.metrics.tool_metrics[&metric_key];
        assert_eq!(metrics.calls, 1);
        assert_eq!(metrics.successes, 1);
    }

    #[test]
    fn public_reviewer_facade_denies_jsonrpc_network_read_without_authority() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let provider_id =
            crate::reviewer::tool_adapters::ToolProviderId::parse("public_jsonrpc_network_denied")
                .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = crate::reviewer::ReviewToolRegistry::review_defaults().unwrap();
        let tool_id = registry
            .register_jsonrpc_network_read_tool(
                provider_id.clone(),
                "public_jsonrpc_network_denied_check",
                "External JSON-RPC check that needs denied network read.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                }),
                false,
                Arc::new(PublicJsonRpcReviewTool {
                    provider_id: provider_id.clone(),
                    tool_id: "public_jsonrpc_network_denied_check".to_string(),
                    expected_provider_resources: Vec::new(),
                    calls: Arc::clone(&calls),
                }),
            )
            .unwrap();
        let mut capabilities = crate::reviewer::capabilities::CapabilitySet::review_read_only();
        capabilities.grant_tool(
            tool_id.clone(),
            crate::reviewer::capabilities::ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: crate::reviewer::capabilities::ToolEffects {
                    network_read: true,
                    ..crate::reviewer::capabilities::ToolEffects::review_read_only()
                },
            },
        );
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-public-jsonrpc-network-denied",
                "head-public-jsonrpc-network-denied",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        );
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "public-jsonrpc-network-denied-session",
            crate::reviewer::Role::Generalist,
            "Run public JSON-RPC provider network check without network authority.",
            public_budget(),
        )
        .with_model_profile_id("mock")
        .with_capabilities(capabilities);
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "public-jsonrpc-network-denied-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let events = Arc::new(crate::reviewer::InMemoryReviewEventSink::default());
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicCustomToolModel(tool_id.clone())))
            .review_tool_registry(registry)
            .review_event_sink(events.clone())
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());

        assert_eq!(report.summary.completed_sessions, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(events.records().iter().any(|record| matches!(
            &record.event,
            crate::reviewer::ReviewEvent::ToolCallDenied {
                error_code: crate::reviewer::tool_adapters::ToolErrorCode::ToolNotAllowed,
                reason,
                ..
            } if reason.contains("network read")
                && record.run_id.as_deref() == Some("public-jsonrpc-network-denied-run")
                && record.session_id.as_deref() == Some("public-jsonrpc-network-denied-session")
        )));
        let metric_key = crate::reviewer::tool_adapters::ToolMetricKey::new(&provider_id, &tool_id);
        let metrics = &report.metrics.tool_metrics[&metric_key];
        assert_eq!(metrics.calls, 1);
        assert_eq!(metrics.errors, 1);
    }

    #[test]
    fn public_reviewer_facade_denies_jsonrpc_provider_resource_outside_scope() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let provider_id =
            crate::reviewer::tool_adapters::ToolProviderId::parse("public_jsonrpc_denied_provider")
                .unwrap();
        let allowed_resource =
            crate::reviewer::tool_adapters::ProviderResourceId::parse("github/org-a/repo-a")
                .unwrap();
        let denied_resource =
            crate::reviewer::tool_adapters::ProviderResourceId::parse("github/org-b/repo-b")
                .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = crate::reviewer::ReviewToolRegistry::review_defaults().unwrap();
        let tool_id = registry
            .register_scoped_jsonrpc_read_only_tool(
                crate::reviewer::ReviewJsonRpcReadOnlyToolRegistration {
                    provider_id: provider_id.clone(),
                    id: "public_jsonrpc_denied_check".to_string(),
                    description: "External JSON-RPC check scoped to another provider resource."
                        .to_string(),
                    parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                    }),
                    cacheable: false,
                    provider_resources: vec![allowed_resource.clone()],
                    transport: Arc::new(PublicJsonRpcReviewTool {
                        provider_id: provider_id.clone(),
                        tool_id: "public_jsonrpc_denied_check".to_string(),
                        expected_provider_resources: vec![allowed_resource],
                        calls: Arc::clone(&calls),
                    }),
                },
            )
            .unwrap();
        let snapshot = crate::reviewer::SnapshotSpec::new(
            temp.path().to_path_buf(),
            crate::reviewer::ChangeSpec::local(
                "change-public-jsonrpc-denied",
                "head-public-jsonrpc-denied",
                vec![crate::reviewer::ChangedFileSpec::modified("README.md")],
            ),
        );
        let session = crate::reviewer::ReviewSessionSpec::review_read_only(
            "public-jsonrpc-denied-session",
            crate::reviewer::Role::Generalist,
            "Run public JSON-RPC provider check outside resource scope.",
            public_budget(),
        )
        .with_model_profile_id("mock")
        .grant_provider_read_only_tool_for_resources(
            provider_id.clone(),
            tool_id.clone(),
            vec![denied_resource],
        );
        let spec = crate::reviewer::RunSpec::single_snapshot(
            "public-jsonrpc-denied-run",
            snapshot,
            vec![session],
            crate::reviewer::ReviewRunLimits::standard(1, 64 * 1024, 20),
        );
        let events = Arc::new(crate::reviewer::InMemoryReviewEventSink::default());
        let run = crate::reviewer::Run::builder(spec)
            .review_model(Arc::new(PublicCustomToolModel(tool_id.clone())))
            .review_tool_registry(registry)
            .review_event_sink(events.clone())
            .build()
            .unwrap();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(run.execute());

        assert_eq!(report.summary.completed_sessions, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(events.records().iter().any(|record| matches!(
            &record.event,
            crate::reviewer::ReviewEvent::ToolCallDenied {
                error_code: crate::reviewer::tool_adapters::ToolErrorCode::ToolNotAllowed,
                reason,
                ..
            } if reason.contains("provider resource")
                && record.run_id.as_deref() == Some("public-jsonrpc-denied-run")
                && record.session_id.as_deref() == Some("public-jsonrpc-denied-session")
        )));
        let metric_key = crate::reviewer::tool_adapters::ToolMetricKey::new(&provider_id, &tool_id);
        let metrics = &report.metrics.tool_metrics[&metric_key];
        assert_eq!(metrics.calls, 1);
        assert_eq!(metrics.errors, 1);
    }

    #[test]
    fn public_bounded_event_sink_drops_after_capacity() {
        let sink = crate::reviewer::runtime_events::BoundedInMemoryEventSink::new(1);
        crate::reviewer::runtime_events::EventSink::emit(
            &sink,
            crate::reviewer::runtime_events::RuntimeEvent::JobStarted {
                snapshot_id: crate::reviewer::ids::SnapshotId("snapshot".to_string()),
            },
        );
        crate::reviewer::runtime_events::EventSink::emit(
            &sink,
            crate::reviewer::runtime_events::RuntimeEvent::JobFinished {
                status: "done".to_string(),
            },
        );

        let records = sink.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].seq, 1);
        assert_eq!(
            records[0].context.snapshot_id,
            Some(crate::reviewer::ids::SnapshotId("snapshot".to_string()))
        );
        assert_eq!(sink.dropped_count(), 1);
        let event_log_dir = tempfile::tempdir().unwrap();
        let event_log = sink
            .export_jsonl(event_log_dir.path().join("bounded-events.jsonl"))
            .unwrap();
        assert_eq!(event_log.record_count, 1);
        assert_eq!(event_log.dropped_count, 1);
        assert_eq!(
            fs::read_to_string(&event_log.path).unwrap().lines().count(),
            1
        );

        let oldest = crate::reviewer::runtime_events::BoundedInMemoryEventSink::with_policy(
            1,
            crate::reviewer::runtime_events::EventBackpressurePolicy::DropOldest,
        );
        crate::reviewer::runtime_events::EventSink::emit(
            &oldest,
            crate::reviewer::runtime_events::RuntimeEvent::JobStarted {
                snapshot_id: crate::reviewer::ids::SnapshotId("snapshot".to_string()),
            },
        );
        crate::reviewer::runtime_events::EventSink::emit(
            &oldest,
            crate::reviewer::runtime_events::RuntimeEvent::JobFinished {
                status: "done".to_string(),
            },
        );
        let records = oldest.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].seq, 2);
        assert!(matches!(
            records[0].event,
            crate::reviewer::runtime_events::RuntimeEvent::JobFinished { .. }
        ));
        assert_eq!(oldest.dropped_count(), 1);
    }

    #[test]
    fn public_runtime_event_jsonl_matches_versioned_fixture() {
        let snapshot_id = crate::reviewer::ids::SnapshotId("compat-snapshot".to_string());
        let session_id = crate::reviewer::ids::SessionId("compat-session".to_string());
        let read_call_id = crate::reviewer::ids::ToolCallId("compat-call-read".to_string());
        let denied_call_id = crate::reviewer::ids::ToolCallId("compat-call-denied".to_string());
        let artifact_call_id = crate::reviewer::ids::ToolCallId("compat-call-artifact".to_string());
        let finding_call_id = crate::reviewer::ids::ToolCallId("compat-call-finding".to_string());
        let search_call_id = crate::reviewer::ids::ToolCallId("compat-call-search".to_string());
        let artifact_id = crate::reviewer::artifacts::ArtifactId("art_fixture".to_string());
        let provider_id = crate::reviewer::tool_adapters::ToolProviderId::builtin_review();
        let context = |session_id: Option<&crate::reviewer::ids::SessionId>,
                       turn_id: Option<u32>,
                       tool_call_id: Option<&crate::reviewer::ids::ToolCallId>,
                       artifact_id: Option<&crate::reviewer::artifacts::ArtifactId>,
                       finding_id: Option<&str>| {
            crate::reviewer::runtime_events::RuntimeEventContext {
                run_id: Some("compat-run".to_string()),
                snapshot_id: Some(snapshot_id.clone()),
                session_id: session_id.cloned(),
                turn_id: turn_id.map(crate::reviewer::runtime_events::TurnId),
                tool_call_id: tool_call_id.cloned(),
                artifact_id: artifact_id.cloned(),
                finding_id: finding_id.map(ToOwned::to_owned),
            }
        };
        let records = vec![
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 1,
                timestamp_utc: "1780520000.000000000Z".to_string(),
                context: context(None, None, None, None, None),
                event: crate::reviewer::runtime_events::RuntimeEvent::JobStarted {
                    snapshot_id: snapshot_id.clone(),
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 2,
                timestamp_utc: "1780520001.000000000Z".to_string(),
                context: context(None, None, None, None, None),
                event: crate::reviewer::runtime_events::RuntimeEvent::SnapshotStarted {
                    snapshot_id: snapshot_id.clone(),
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 3,
                timestamp_utc: "1780520002.000000000Z".to_string(),
                context: context(None, None, None, None, None),
                event: crate::reviewer::runtime_events::RuntimeEvent::RepoManifestCompleted {
                    files: 3,
                    skipped: 1,
                    bytes: 128,
                    ms: 12,
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 4,
                timestamp_utc: "1780520003.000000000Z".to_string(),
                context: context(Some(&session_id), None, None, None, None),
                event: crate::reviewer::runtime_events::RuntimeEvent::SessionStarted {
                    session_id: session_id.clone(),
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 5,
                timestamp_utc: "1780520004.000000000Z".to_string(),
                context: context(Some(&session_id), Some(1), None, None, None),
                event: crate::reviewer::runtime_events::RuntimeEvent::ModelStarted {
                    session_id: session_id.clone(),
                    turn_id: crate::reviewer::runtime_events::TurnId(1),
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 6,
                timestamp_utc: "1780520005.000000000Z".to_string(),
                context: context(Some(&session_id), Some(1), None, None, None),
                event: crate::reviewer::runtime_events::RuntimeEvent::ModelCompleted {
                    session_id: session_id.clone(),
                    turn_id: crate::reviewer::runtime_events::TurnId(1),
                    tool_call_count: 2,
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 7,
                timestamp_utc: "1780520006.000000000Z".to_string(),
                context: context(Some(&session_id), Some(1), None, None, None),
                event: crate::reviewer::runtime_events::RuntimeEvent::ToolBatchStarted {
                    session_id: session_id.clone(),
                    turn_id: crate::reviewer::runtime_events::TurnId(1),
                    count: 2,
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 8,
                timestamp_utc: "1780520007.000000000Z".to_string(),
                context: context(Some(&session_id), Some(1), Some(&read_call_id), None, None),
                event: crate::reviewer::runtime_events::RuntimeEvent::ToolCallCompleted {
                    call_id: read_call_id,
                    tool_name: crate::reviewer::ids::ToolId::parse("read_file").unwrap(),
                    provider_id: provider_id.clone(),
                    cache_status: crate::reviewer::metrics::CacheStatus::Miss,
                    output_bytes: 42,
                    ok: true,
                    error_code: None,
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 9,
                timestamp_utc: "1780520008.000000000Z".to_string(),
                context: context(
                    Some(&session_id),
                    Some(1),
                    Some(&denied_call_id),
                    None,
                    None,
                ),
                event: crate::reviewer::runtime_events::RuntimeEvent::ToolCallDenied {
                    call_id: denied_call_id,
                    tool_name: crate::reviewer::ids::ToolId::parse("read_file").unwrap(),
                    provider_id: provider_id.clone(),
                    error_code: crate::reviewer::tool_adapters::ToolErrorCode::ToolNotAllowed,
                    reason: "not granted by fixture policy".to_string(),
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 10,
                timestamp_utc: "1780520009.000000000Z".to_string(),
                context: context(
                    Some(&session_id),
                    Some(2),
                    Some(&artifact_call_id),
                    Some(&artifact_id),
                    None,
                ),
                event: crate::reviewer::runtime_events::RuntimeEvent::ArtifactCreated {
                    artifact_id: artifact_id.clone(),
                    tool_call_id: artifact_call_id,
                    tool_name: crate::reviewer::ids::ToolId::parse("search_text").unwrap(),
                    provider_id: provider_id.clone(),
                    bytes: 42,
                    content_hash: "hash-fixture".to_string(),
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 11,
                timestamp_utc: "1780520010.000000000Z".to_string(),
                context: context(
                    Some(&session_id),
                    Some(3),
                    Some(&finding_call_id),
                    None,
                    Some("finding-fixture"),
                ),
                event: crate::reviewer::runtime_events::RuntimeEvent::FindingRecorded {
                    finding_id: "finding-fixture".to_string(),
                    session_id: session_id.clone(),
                    tool_call_id: finding_call_id,
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 12,
                timestamp_utc: "1780520011.000000000Z".to_string(),
                context: context(
                    Some(&session_id),
                    Some(4),
                    Some(&search_call_id),
                    None,
                    None,
                ),
                event: crate::reviewer::runtime_events::RuntimeEvent::SearchBatchCompleted {
                    searched_files: 5,
                    skipped_files: 1,
                    bytes_scanned: 256,
                    ms: 3,
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 13,
                timestamp_utc: "1780520012.000000000Z".to_string(),
                context: context(Some(&session_id), None, None, None, None),
                event: crate::reviewer::runtime_events::RuntimeEvent::SessionFinished {
                    session_id: session_id.clone(),
                    status: "completed".to_string(),
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 14,
                timestamp_utc: "1780520013.000000000Z".to_string(),
                context: context(None, None, None, None, None),
                event: crate::reviewer::runtime_events::RuntimeEvent::SnapshotFinished {
                    snapshot_id: snapshot_id.clone(),
                    sessions: 1,
                    completed_sessions: 1,
                },
            },
            crate::reviewer::runtime_events::RuntimeEventRecord {
                seq: 15,
                timestamp_utc: "1780520014.000000000Z".to_string(),
                context: crate::reviewer::runtime_events::RuntimeEventContext {
                    run_id: Some("compat-run".to_string()),
                    ..crate::reviewer::runtime_events::RuntimeEventContext::default()
                },
                event: crate::reviewer::runtime_events::RuntimeEvent::JobFinished {
                    status: "completed".to_string(),
                },
            },
        ];
        let event_log_dir = tempfile::tempdir().unwrap();
        let manifest = crate::reviewer::runtime_events::export_event_records_jsonl(
            event_log_dir.path().join("runtime-events-v1.jsonl"),
            &records,
        )
        .unwrap();
        assert_eq!(manifest.record_count, records.len());
        assert_eq!(manifest.dropped_count, 0);

        let fixture_values = include_str!("../fixtures/runtime-events-v1.jsonl")
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(fixture_values.len(), records.len());
        for value in &fixture_values {
            assert_eq!(
                value["schemaVersion"].as_str(),
                Some(crate::util::SCHEMA_VERSION)
            );
        }
        let generated_values = fs::read_to_string(&manifest.path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(generated_values, fixture_values);
        let fixture_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/runtime-events-v1.jsonl");
        let loaded =
            crate::reviewer::runtime_events::load_event_records_jsonl(&fixture_path).unwrap();
        assert_eq!(loaded.path, fixture_path);
        assert_eq!(loaded.record_count, records.len());
        assert_eq!(loaded.migration.migrated_records, 0);
        assert_eq!(
            loaded.migration.current_schema_version,
            crate::util::SCHEMA_VERSION
        );
        assert_eq!(
            loaded
                .migration
                .source_schema_versions
                .get(crate::util::SCHEMA_VERSION),
            Some(&records.len())
        );
        assert_eq!(loaded.records, records);

        let bad_log = event_log_dir
            .path()
            .join("unsupported-runtime-events.jsonl");
        fs::write(
            &bad_log,
            include_str!("../fixtures/runtime-events-v1.jsonl").replacen(
                crate::util::SCHEMA_VERSION,
                "heimdaal.review-run.v9",
                1,
            ),
        )
        .unwrap();
        assert!(matches!(
            crate::reviewer::runtime_events::load_event_records_jsonl(&bad_log),
            Err(crate::reviewer::runtime::RuntimeError::InvalidInput(message))
                if message.contains("unsupported event log schemaVersion")
        ));
    }

    #[test]
    fn public_runtime_event_jsonl_migrates_contextless_legacy_records() {
        let legacy_fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/runtime-events-v0-contextless.jsonl");
        let loaded =
            crate::reviewer::runtime_events::load_event_records_jsonl(&legacy_fixture).unwrap();

        assert_eq!(loaded.path, legacy_fixture);
        assert_eq!(loaded.record_count, 6);
        assert_eq!(loaded.migration.migrated_records, 6);
        assert_eq!(
            loaded.migration.current_schema_version,
            crate::util::SCHEMA_VERSION
        );
        assert_eq!(
            loaded
                .migration
                .source_schema_versions
                .get(crate::reviewer::runtime_events::LEGACY_CONTEXTLESS_EVENT_LOG_SCHEMA_VERSION),
            Some(&6)
        );
        assert!(matches!(
            &loaded.records[0].event,
            crate::reviewer::runtime_events::RuntimeEvent::JobStarted { snapshot_id }
                if snapshot_id.0 == "legacy-snapshot"
                    && loaded.records[0].context.snapshot_id.as_ref().map(|id| id.0.as_str())
                        == Some("legacy-snapshot")
        ));
        assert_eq!(loaded.records[1].seq, 2);
        assert_eq!(loaded.records[1].timestamp_utc, "1780519901.000000000Z");
        assert_eq!(
            loaded.records[1]
                .context
                .session_id
                .as_ref()
                .map(|id| id.0.as_str()),
            Some("legacy-session")
        );
        assert_eq!(
            loaded.records[1].context.turn_id,
            Some(crate::reviewer::runtime_events::TurnId(7))
        );
        assert!(matches!(
            &loaded.records[1].event,
            crate::reviewer::runtime_events::RuntimeEvent::ModelStarted { session_id, turn_id }
                if session_id.0 == "legacy-session" && turn_id.0 == 7
        ));
        assert!(matches!(
            &loaded.records[2].event,
            crate::reviewer::runtime_events::RuntimeEvent::ToolCallDenied { call_id, .. }
                if call_id.0 == "legacy-denied-call"
                    && loaded.records[2].context.tool_call_id.as_ref().map(|id| id.0.as_str())
                        == Some("legacy-denied-call")
        ));
        assert!(matches!(
            &loaded.records[3].event,
            crate::reviewer::runtime_events::RuntimeEvent::ArtifactCreated {
                artifact_id,
                tool_call_id,
                ..
            } if artifact_id.0 == "legacy-artifact"
                && tool_call_id.0 == "legacy-artifact-call"
                && loaded.records[3].context.artifact_id.as_ref().map(|id| id.0.as_str())
                    == Some("legacy-artifact")
                && loaded.records[3].context.tool_call_id.as_ref().map(|id| id.0.as_str())
                    == Some("legacy-artifact-call")
        ));
        assert!(matches!(
            &loaded.records[4].event,
            crate::reviewer::runtime_events::RuntimeEvent::SessionFinished { session_id, status }
                if session_id.0 == "legacy-session"
                    && status == "cancelled"
                    && loaded.records[4].context.session_id.as_ref().map(|id| id.0.as_str())
                        == Some("legacy-session")
        ));
        assert!(matches!(
            &loaded.records[5].event,
            crate::reviewer::runtime_events::RuntimeEvent::JobFinished { status } if status == "partial"
        ));
        assert_eq!(
            loaded.records[5].context,
            crate::reviewer::runtime_events::RuntimeEventContext::default()
        );
    }

    #[test]
    fn concurrent_repo_path_denies_escapes_and_windows_prefixes() {
        assert!(RepoPath::parse("../secret.txt").is_err());
        assert!(RepoPath::parse("/etc/passwd").is_err());
        assert!(RepoPath::parse("C:\\secret.txt").is_err());
        assert!(RepoPath::parse("safe/path.rs").is_ok());
    }

    #[test]
    fn concurrent_tool_batch_rejects_finish_mixed_with_other_tools() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = std::sync::Arc::new(RuntimeLimits::standard(1, 64 * 1024, 10));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let results = runtime.block_on(engine.execute_batch(
            test_scope("session"),
            TurnId(0),
            vec![
                ModelToolCall {
                    call_id: ToolCallId("finish".to_string()),
                    index: 0,
                    name: ToolId::from(ToolName::Finish),
                    raw_arguments: r#"{"reason":"done"}"#.to_string(),
                },
                ModelToolCall {
                    call_id: ToolCallId("read".to_string()),
                    index: 1,
                    name: ToolId::from(ToolName::ReadDiff),
                    raw_arguments: "{}".to_string(),
                },
            ],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| !result.ok));
    }

    #[test]
    fn concurrent_duplicate_search_uses_one_underlying_scan() {
        let temp = tempfile::tempdir().unwrap();
        for index in 0..200 {
            fs::write(
                temp.path().join(format!("file-{index}.rs")),
                format!("fn f_{index}() {{ let needle = {index}; }}\n"),
            )
            .unwrap();
        }
        let change = test_change_with_file("file-0.rs");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = std::sync::Arc::new(RuntimeLimits::standard(10, 64 * 1024, 20));
        let engine = std::sync::Arc::new(ToolEngine::new(snapshot, limits).unwrap());
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut joins = tokio::task::JoinSet::new();
            for index in 0..10 {
                let engine = std::sync::Arc::clone(&engine);
                joins.spawn(async move {
                    let scope = test_scope(&format!("session-{index}"));
                    engine
                        .execute_batch(
                            scope,
                            TurnId(0),
                            vec![ModelToolCall {
                                call_id: ToolCallId(format!("search-{index}")),
                                index: 0,
                                name: ToolId::from(ToolName::SearchText),
                                raw_arguments: r#"{"query":"needle"}"#.to_string(),
                            }],
                            tokio_util::sync::CancellationToken::new(),
                        )
                        .await
                });
            }
            while let Some(result) = joins.join_next().await {
                let batch = result.unwrap();
                assert_eq!(batch.len(), 1);
                assert!(batch[0].ok);
            }
        });
        let counters = engine.snapshot_counters();
        assert_eq!(counters.search_scans, 1);
    }

    #[test]
    fn concurrent_queued_search_observes_cancellation_before_permit() {
        let temp = tempfile::tempdir().unwrap();
        for index in 0..50 {
            fs::write(
                temp.path().join(format!("file-{index}.rs")),
                format!("fn f_{index}() {{ let needle = {index}; }}\n"),
            )
            .unwrap();
        }
        let change = test_change_with_file("file-0.rs");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let mut limits = RuntimeLimits::standard(1, 64 * 1024, 20);
        limits.max_search_jobs_global = 1;
        let engine = Arc::new(ToolEngine::new(snapshot, Arc::new(limits)).unwrap());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let search_permit = engine.search.acquire_search_permit_for_test().await;
            let cancel = tokio_util::sync::CancellationToken::new();
            let queued_engine = Arc::clone(&engine);
            let queued_cancel = cancel.clone();
            let mut queued = tokio::spawn(async move {
                queued_engine
                    .execute_batch(
                        test_scope("queued-search"),
                        TurnId(0),
                        vec![search_call("queued-search-call")],
                        queued_cancel,
                    )
                    .await
            });
            wait_for_inflight_tool(&engine).await;
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut queued)
                    .await
                    .is_err(),
                "search should stay queued while the permit is held"
            );

            cancel.cancel();
            let results = tokio::time::timeout(std::time::Duration::from_millis(200), queued)
                .await
                .expect("queued search should observe cancellation")
                .expect("queued search task should join");
            drop(search_permit);

            assert_eq!(results.len(), 1);
            assert!(!results[0].ok);
            assert_eq!(
                results[0].error.as_ref().unwrap().code,
                ToolErrorCode::Cancelled
            );
            assert_eq!(engine.snapshot_counters().search_scans, 0);
            assert_eq!(engine.inflight_tool_count_for_test(), 0);
            let metrics = engine.snapshot_tool_metrics();
            let search_metrics = &metrics[&ToolMetricKey::builtin(ToolName::SearchText)];
            assert_eq!(search_metrics.calls, 1);
            assert_eq!(search_metrics.errors, 1);
            assert_eq!(search_metrics.cancellations, 1);
        });
    }

    #[test]
    fn concurrent_deduped_search_waiter_observes_its_own_cancellation() {
        let temp = tempfile::tempdir().unwrap();
        for index in 0..50 {
            fs::write(
                temp.path().join(format!("file-{index}.rs")),
                format!("fn f_{index}() {{ let needle = {index}; }}\n"),
            )
            .unwrap();
        }
        let change = test_change_with_file("file-0.rs");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let mut limits = RuntimeLimits::standard(2, 64 * 1024, 20);
        limits.max_search_jobs_global = 1;
        let engine = Arc::new(ToolEngine::new(snapshot, Arc::new(limits)).unwrap());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let search_permit = engine.search.acquire_search_permit_for_test().await;
            let owner_engine = Arc::clone(&engine);
            let owner = tokio::spawn(async move {
                owner_engine
                    .execute_batch(
                        test_scope("dedupe-owner"),
                        TurnId(0),
                        vec![search_call("owner-search-call")],
                        tokio_util::sync::CancellationToken::new(),
                    )
                    .await
            });
            wait_for_inflight_tool(&engine).await;

            let waiter_cancel = tokio_util::sync::CancellationToken::new();
            let waiter_engine = Arc::clone(&engine);
            let waiter_cancel_for_task = waiter_cancel.clone();
            let mut waiter = tokio::spawn(async move {
                waiter_engine
                    .execute_batch(
                        test_scope("dedupe-waiter"),
                        TurnId(0),
                        vec![search_call("waiter-search-call")],
                        waiter_cancel_for_task,
                    )
                    .await
            });
            wait_for_search_dedupe_waiter(&engine).await;
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiter)
                    .await
                    .is_err(),
                "deduped waiter should stay blocked while owner is still queued"
            );

            waiter_cancel.cancel();
            let waiter_results =
                tokio::time::timeout(std::time::Duration::from_millis(200), waiter)
                    .await
                    .expect("deduped waiter should observe its own cancellation")
                    .expect("deduped waiter task should join");
            assert_eq!(waiter_results.len(), 1);
            assert!(!waiter_results[0].ok);
            assert_eq!(
                waiter_results[0].error.as_ref().unwrap().code,
                ToolErrorCode::Cancelled
            );

            drop(search_permit);
            let owner_results = tokio::time::timeout(std::time::Duration::from_secs(1), owner)
                .await
                .expect("owner search should finish after permit release")
                .expect("owner search task should join");
            assert_eq!(owner_results.len(), 1);
            assert!(owner_results[0].ok);
            assert_eq!(engine.snapshot_counters().search_dedupe_waiters, 1);
            assert_eq!(engine.snapshot_counters().search_scans, 1);
            let metrics = engine.snapshot_tool_metrics();
            let search_metrics = &metrics[&ToolMetricKey::builtin(ToolName::SearchText)];
            assert_eq!(search_metrics.calls, 2);
            assert_eq!(search_metrics.successes, 1);
            assert_eq!(search_metrics.errors, 1);
            assert_eq!(search_metrics.cancellations, 1);
        });
    }

    #[test]
    fn concurrent_duplicate_tool_calls_in_one_turn_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let results = runtime.block_on(engine.execute_batch(
            test_scope("session"),
            TurnId(0),
            vec![
                ModelToolCall {
                    call_id: ToolCallId("read-diff-1".to_string()),
                    index: 0,
                    name: ToolId::from(ToolName::ReadDiff),
                    raw_arguments: "{}".to_string(),
                },
                ModelToolCall {
                    call_id: ToolCallId("read-diff-2".to_string()),
                    index: 1,
                    name: ToolId::from(ToolName::ReadDiff),
                    raw_arguments: "{}".to_string(),
                },
            ],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(results.len(), 2);
        assert!(results[0].ok);
        assert!(!results[1].ok);
        assert_eq!(
            results[1].error.as_ref().unwrap().code,
            ToolErrorCode::InvalidArgs
        );
        let metrics = engine.snapshot_tool_metrics();
        let read_diff_metrics = &metrics[&ToolMetricKey::builtin(ToolName::ReadDiff)];
        assert_eq!(read_diff_metrics.successes, 1);
        assert_eq!(read_diff_metrics.errors, 1);
    }

    #[test]
    fn concurrent_tool_invalid_args_and_path_denied_are_reported() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let results = runtime.block_on(engine.execute_batch(
            test_scope("session"),
            TurnId(0),
            vec![
                ModelToolCall {
                    call_id: ToolCallId("invalid-args".to_string()),
                    index: 0,
                    name: ToolId::from(ToolName::ReadFile),
                    raw_arguments: "{}".to_string(),
                },
                ModelToolCall {
                    call_id: ToolCallId("path-denied".to_string()),
                    index: 1,
                    name: ToolId::from(ToolName::ReadFile),
                    raw_arguments: serde_json::json!({ "path": "missing.md" }).to_string(),
                },
            ],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(results.len(), 2);
        assert!(!results[0].ok);
        assert_eq!(
            results[0].error.as_ref().unwrap().code,
            ToolErrorCode::InvalidArgs
        );
        assert!(!results[1].ok);
        assert_eq!(
            results[1].error.as_ref().unwrap().code,
            ToolErrorCode::PathDenied
        );
        assert_eq!(engine.snapshot_counters().tool_errors, 2);
        let metrics = engine.snapshot_tool_metrics();
        let read_file_metrics = &metrics[&ToolMetricKey::builtin(ToolName::ReadFile)];
        assert_eq!(read_file_metrics.calls, 2);
        assert_eq!(read_file_metrics.errors, 2);
    }

    #[test]
    fn concurrent_search_cache_is_scoped_by_filesystem_scope() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("src")).unwrap();
        fs::write(temp.path().join("README.md"), "needle in root\n").unwrap();
        fs::write(temp.path().join("src/lib.rs"), "needle in src\n").unwrap();
        let change = test_change_with_file("src/lib.rs");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(2, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let root = runtime.block_on(engine.execute_batch(
            test_scope("root-session"),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("root-search".to_string()),
                index: 0,
                name: ToolId::from(ToolName::SearchText),
                raw_arguments: r#"{"query":"needle"}"#.to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert!(root[0].ok);
        assert_eq!(root[0].limits.searched_files, 2);

        let mut scoped_capabilities = CapabilitySet::review_read_only();
        scoped_capabilities.fs_scope = FsScope::subtree(RepoPath::parse("src").unwrap());
        let scoped = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("src-session", scoped_capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("src-search".to_string()),
                index: 0,
                name: ToolId::from(ToolName::SearchText),
                raw_arguments: r#"{"query":"needle"}"#.to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert!(scoped[0].ok);
        assert_eq!(scoped[0].limits.searched_files, 1);
        let matches = scoped[0].data.as_ref().unwrap()["matches"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(matches.iter().all(|line| line.starts_with("src/lib.rs:")));

        let counters = engine.snapshot_counters();
        assert_eq!(counters.search_scans, 2);
        let metrics = engine.snapshot_tool_metrics();
        let search_key = ToolMetricKey::builtin(ToolName::SearchText);
        assert_eq!(
            search_key.provider_id(),
            Some(ToolProviderId::builtin_review())
        );
        let search_metrics = &metrics[&search_key];
        assert_eq!(search_metrics.calls, 2);
        assert_eq!(search_metrics.successes, 2);
    }

    #[test]
    fn concurrent_read_file_serves_captured_snapshot_after_worktree_mutation() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "original needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        fs::write(temp.path().join("README.md"), "mutated needle\n").unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let results = runtime.block_on(engine.execute_batch(
            test_scope("session"),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("read".to_string()),
                index: 0,
                name: ToolId::from(ToolName::ReadFile),
                raw_arguments: serde_json::json!({ "path": "README.md" }).to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(results.len(), 1);
        assert!(results[0].ok);
        let content = results[0].data.as_ref().unwrap()["content"]
            .as_str()
            .unwrap();
        assert!(content.contains("original needle"));
        assert!(!content.contains("mutated needle"));
    }

    #[test]
    fn concurrent_search_serves_captured_snapshot_after_worktree_mutation() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "original needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        fs::write(temp.path().join("README.md"), "mutated needle\n").unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let results = runtime.block_on(engine.execute_batch(
            test_scope("session"),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("search".to_string()),
                index: 0,
                name: ToolId::from(ToolName::SearchText),
                raw_arguments: serde_json::json!({ "query": "needle" }).to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(results.len(), 1);
        assert!(results[0].ok);
        let matches = results[0].data.as_ref().unwrap()["matches"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(matches.iter().any(|line| line.contains("original needle")));
        assert!(!matches.iter().any(|line| line.contains("mutated needle")));
    }

    #[test]
    fn concurrent_tool_grant_enforces_max_calls_per_session() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut capabilities = CapabilitySet::review_read_only();
        capabilities
            .tool_grants
            .get_mut(&ToolId::from(ToolName::ReadDiff))
            .unwrap()
            .max_calls = Some(1);

        let first = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session-a", capabilities.clone()),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("read-diff-first".to_string()),
                index: 0,
                name: ToolId::from(ToolName::ReadDiff),
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(first.len(), 1);
        assert!(first[0].ok);

        let second = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session-a", capabilities.clone()),
            TurnId(1),
            vec![ModelToolCall {
                call_id: ToolCallId("read-diff-second".to_string()),
                index: 0,
                name: ToolId::from(ToolName::ReadDiff),
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(second.len(), 1);
        assert!(!second[0].ok);
        assert_eq!(
            second[0].error.as_ref().unwrap().code,
            ToolErrorCode::BudgetExceeded
        );

        let other_session = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session-b", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("read-diff-other-session".to_string()),
                index: 0,
                name: ToolId::from(ToolName::ReadDiff),
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(other_session.len(), 1);
        assert!(other_session[0].ok);
    }

    #[test]
    fn concurrent_tool_grant_denies_effects_outside_grant() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.grant_tool(
            ToolId::from(ToolName::ReadDiff),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("effect-denied".to_string()),
                index: 0,
                name: ToolId::from(ToolName::ReadDiff),
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        assert_eq!(
            results[0].error.as_ref().unwrap().code,
            ToolErrorCode::ToolNotAllowed
        );
    }

    #[test]
    fn concurrent_custom_tool_artifact_write_requires_grant() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 10));
        let tool_id = ToolId::parse("artifact_writer").unwrap();
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_custom_with_effects(
                tool_id.clone(),
                "Custom check that writes an artifact.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                }),
                false,
                ToolEffects {
                    artifact_write: true,
                    ..ToolEffects::default()
                },
                Arc::new(EchoCustomTool),
            )
            .unwrap();
        let engine = ToolEngine::with_registry(snapshot, limits, Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.grant_tool(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("artifact-write-denied".to_string()),
                index: 0,
                name: tool_id,
                raw_arguments: r#"{"value":"ok"}"#.to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        assert_eq!(
            results[0].error.as_ref().unwrap().code,
            ToolErrorCode::ToolNotAllowed
        );
    }

    #[test]
    fn concurrent_in_process_tool_provider_timeout_is_typed() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let mut limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        limits.max_tool_provider_ms = 5;
        let tool_id = ToolId::parse("slow_custom_check").unwrap();
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_custom(
                tool_id.clone(),
                "Slow custom check used to prove provider timeout.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                false,
                Arc::new(SlowCustomTool),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = trusted_custom_capabilities();
        capabilities.grant_tool(tool_id.clone(), ToolGrant::allow_custom_read_only());

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("slow-timeout".to_string()),
                index: 0,
                name: tool_id.clone(),
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        assert_eq!(
            results[0].error.as_ref().unwrap().code,
            ToolErrorCode::Timeout
        );
        assert_eq!(results[0].provider_id, ToolProviderId::in_process());
        let metrics = engine.snapshot_tool_metrics();
        let slow_metrics = &metrics[&ToolMetricKey::in_process(&tool_id)];
        assert_eq!(slow_metrics.errors, 1);
        assert_eq!(slow_metrics.timeouts, 1);
        assert_eq!(slow_metrics.cancellations, 0);
        assert_eq!(slow_metrics.input_bytes, "{}".len());
        assert!(slow_metrics.latency_ms >= slow_metrics.max_latency_ms);
        assert!(slow_metrics.max_latency_ms > 0);
        let health = engine.snapshot_provider_health();
        let in_process_health = health
            .iter()
            .find(|snapshot| snapshot.provider_id == ToolProviderId::in_process())
            .unwrap();
        assert_eq!(in_process_health.state, ToolProviderHealthState::Degraded);
        assert_eq!(in_process_health.errors, 1);
        assert_eq!(in_process_health.timeouts, 1);
        assert_eq!(in_process_health.consecutive_errors, 1);
    }

    #[test]
    fn concurrent_jsonrpc_tool_provider_executes_external_tool() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        let tool_id = ToolId::parse("external_jsonrpc_check").unwrap();
        let provider_id = ToolProviderId::parse("jsonrpc_test_provider").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_jsonrpc_tool(
                provider_id.clone(),
                tool_id.clone(),
                "External JSON-RPC check.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "additionalProperties": false
                }),
                CustomToolOptions {
                    cacheable: false,
                    effects: ToolEffects::custom_read_only(),
                    provider_resources: Vec::new(),
                },
                Arc::new(EchoJsonRpcTransport {
                    provider_id: provider_id.clone(),
                    tool_id: tool_id.clone(),
                    calls: Arc::clone(&calls),
                }),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = trusted_custom_capabilities();
        capabilities.grant_tool(tool_id.clone(), ToolGrant::allow_custom_read_only());

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("external-call".to_string()),
                index: 0,
                name: tool_id.clone(),
                raw_arguments: r#"{"value":"ok"}"#.to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(results.len(), 1);
        assert!(results[0].ok);
        assert_eq!(results[0].provider_id, provider_id);
        assert_eq!(results[0].data.as_ref().unwrap()["value"], "ok");
        assert_eq!(
            results[0].data.as_ref().unwrap()["secret"],
            serde_json::Value::String("[REDACTED]".to_string())
        );
        let artifact_text = engine
            .artifacts
            .list()
            .into_iter()
            .map(|artifact| artifact.content)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(artifact_text.contains("[REDACTED]"));
        assert!(!artifact_text.contains("AKIA1234567890ABCDEF"));
        let raw_artifact_text = engine
            .artifacts
            .list_raw()
            .into_iter()
            .map(|artifact| artifact.content)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(raw_artifact_text.contains("AKIA1234567890ABCDEF"));
        let metrics = engine.snapshot_tool_metrics();
        let external_metrics = &metrics[&ToolMetricKey::new(&provider_id, &tool_id)];
        assert_eq!(external_metrics.calls, 1);
        assert_eq!(external_metrics.successes, 1);
        assert_eq!(external_metrics.artifacts, 1);
        let health = engine.snapshot_provider_health();
        let external_health = health
            .iter()
            .find(|snapshot| snapshot.provider_id == provider_id)
            .unwrap();
        assert_eq!(external_health.state, ToolProviderHealthState::Healthy);
        assert_eq!(external_health.calls, 1);
    }

    #[test]
    fn concurrent_jsonrpc_provider_artifact_write_requires_capability_policy() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        let tool_id = ToolId::parse("external_artifact_smuggler").unwrap();
        let provider_id = ToolProviderId::parse("jsonrpc_policy_provider").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_jsonrpc_tool(
                provider_id.clone(),
                tool_id.clone(),
                "External tool that returns an undeclared artifact.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                CustomToolOptions {
                    cacheable: false,
                    effects: ToolEffects::default(),
                    provider_resources: Vec::new(),
                },
                Arc::new(StaticJsonRpcTransport {
                    provider_id: provider_id.clone(),
                    tool_id: tool_id.clone(),
                    calls: Arc::clone(&calls),
                    response: JsonRpcToolResponse {
                        data: None,
                        artifact: Some(CustomToolArtifact {
                            key: ArtifactKey("smuggled_external_artifact".to_string()),
                            content: "external artifact".to_string(),
                        }),
                        limits: LimitInfo::default(),
                    },
                }),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.artifact_access.write = false;
        capabilities.grant_tool(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("external-artifact-write-denied".to_string()),
                index: 0,
                name: tool_id,
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        assert_eq!(
            results[0].error.as_ref().unwrap().code,
            ToolErrorCode::ToolNotAllowed
        );
        assert!(engine.artifacts.list().is_empty());
    }

    #[test]
    fn concurrent_jsonrpc_provider_network_read_requires_runtime_authority_policy() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        let tool_id = ToolId::parse("external_network_reader").unwrap();
        let provider_id = ToolProviderId::parse("jsonrpc_network_provider").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::review_defaults().unwrap();
        let network_effects = ToolEffects {
            network_read: true,
            ..ToolEffects::default()
        };
        registry
            .register_jsonrpc_tool(
                provider_id.clone(),
                tool_id.clone(),
                "External tool that requires network read.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                CustomToolOptions {
                    cacheable: false,
                    effects: network_effects,
                    provider_resources: Vec::new(),
                },
                Arc::new(StaticJsonRpcTransport {
                    provider_id,
                    tool_id: tool_id.clone(),
                    calls: Arc::clone(&calls),
                    response: JsonRpcToolResponse {
                        data: Some(serde_json::json!({"ok": true})),
                        artifact: None,
                        limits: LimitInfo::default(),
                    },
                }),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.grant_tool(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: network_effects,
            },
        );

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("external-network-denied".to_string()),
                index: 0,
                name: tool_id,
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        let error = results[0].error.as_ref().unwrap();
        assert_eq!(error.code, ToolErrorCode::ToolNotAllowed);
        assert!(error.message.contains("network read"));
    }

    #[test]
    fn concurrent_jsonrpc_provider_requires_runtime_provider_scope() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        let tool_id = ToolId::parse("external_provider_scoped").unwrap();
        let provider_id = ToolProviderId::parse("jsonrpc_scoped_provider").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_jsonrpc_tool(
                provider_id.clone(),
                tool_id.clone(),
                "External tool behind provider scope.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                CustomToolOptions {
                    cacheable: false,
                    effects: ToolEffects::default(),
                    provider_resources: Vec::new(),
                },
                Arc::new(StaticJsonRpcTransport {
                    provider_id,
                    tool_id: tool_id.clone(),
                    calls: Arc::clone(&calls),
                    response: JsonRpcToolResponse {
                        data: Some(serde_json::json!({"ok": true})),
                        artifact: None,
                        limits: LimitInfo::default(),
                    },
                }),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.runtime_authority = capabilities
            .runtime_authority
            .scoped_to_providers(vec![ToolProviderId::parse("other_provider").unwrap()]);
        capabilities.grant_tool(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("external-provider-denied".to_string()),
                index: 0,
                name: tool_id,
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        let error = results[0].error.as_ref().unwrap();
        assert_eq!(error.code, ToolErrorCode::ToolNotAllowed);
        assert!(error.message.contains("provider"));
    }

    #[test]
    fn concurrent_jsonrpc_provider_resource_requires_runtime_resource_scope() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        let tool_id = ToolId::parse("external_resource_scoped").unwrap();
        let provider_id = ToolProviderId::parse("jsonrpc_resource_provider").unwrap();
        let resource_id = ProviderResourceId::parse("github/org-a/repo-a").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_jsonrpc_tool(
                provider_id.clone(),
                tool_id.clone(),
                "External tool behind provider resource scope.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                CustomToolOptions {
                    cacheable: false,
                    effects: ToolEffects::default(),
                    provider_resources: vec![resource_id],
                },
                Arc::new(StaticJsonRpcTransport {
                    provider_id: provider_id.clone(),
                    tool_id: tool_id.clone(),
                    calls: Arc::clone(&calls),
                    response: JsonRpcToolResponse {
                        data: Some(serde_json::json!({"ok": true})),
                        artifact: None,
                        limits: LimitInfo::default(),
                    },
                }),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.runtime_authority = capabilities
            .runtime_authority
            .scoped_to_provider_resources(vec![ProviderResourceScope::new(
                provider_id,
                ProviderResourceId::parse("github/org-b/repo-b").unwrap(),
            )]);
        capabilities.grant_tool(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("external-resource-denied".to_string()),
                index: 0,
                name: tool_id,
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        let error = results[0].error.as_ref().unwrap();
        assert_eq!(error.code, ToolErrorCode::ToolNotAllowed);
        assert!(error.message.contains("provider resource"));
    }

    #[test]
    fn concurrent_jsonrpc_provider_resource_scope_is_sent_when_allowed() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        let tool_id = ToolId::parse("external_resource_allowed").unwrap();
        let provider_id = ToolProviderId::parse("jsonrpc_resource_allowed_provider").unwrap();
        let resource_id = ProviderResourceId::parse("github/org-a/repo-a").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_jsonrpc_tool(
                provider_id.clone(),
                tool_id.clone(),
                "External tool behind matching provider resource scope.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                CustomToolOptions {
                    cacheable: false,
                    effects: ToolEffects::default(),
                    provider_resources: vec![resource_id.clone()],
                },
                Arc::new(ResourceCheckingJsonRpcTransport {
                    provider_id: provider_id.clone(),
                    tool_id: tool_id.clone(),
                    calls: Arc::clone(&calls),
                    expected_provider_resources: vec![resource_id.clone()],
                    response: JsonRpcToolResponse {
                        data: Some(serde_json::json!({"ok": true})),
                        artifact: None,
                        limits: LimitInfo::default(),
                    },
                }),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.runtime_authority = capabilities
            .runtime_authority
            .scoped_to_provider_resources(vec![ProviderResourceScope::new(
                provider_id.clone(),
                resource_id,
            )]);
        capabilities.grant_tool(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("external-resource-allowed".to_string()),
                index: 0,
                name: tool_id,
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(results.len(), 1);
        assert!(results[0].ok);
        assert_eq!(results[0].provider_id, provider_id);
    }

    #[test]
    fn concurrent_jsonrpc_provider_rejects_oversized_artifacts_before_storage() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let mut limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        limits.max_tool_artifact_bytes = 4;
        let tool_id = ToolId::parse("external_large_artifact").unwrap();
        let provider_id = ToolProviderId::parse("jsonrpc_large_artifact_provider").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_jsonrpc_tool(
                provider_id.clone(),
                tool_id.clone(),
                "External tool that returns an oversized artifact.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                CustomToolOptions {
                    cacheable: false,
                    effects: ToolEffects::default(),
                    provider_resources: Vec::new(),
                },
                Arc::new(StaticJsonRpcTransport {
                    provider_id: provider_id.clone(),
                    tool_id: tool_id.clone(),
                    calls: Arc::clone(&calls),
                    response: JsonRpcToolResponse {
                        data: None,
                        artifact: Some(CustomToolArtifact {
                            key: ArtifactKey("large_external_artifact".to_string()),
                            content: "too large".to_string(),
                        }),
                        limits: LimitInfo::default(),
                    },
                }),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.grant_tool(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("external-artifact-too-large".to_string()),
                index: 0,
                name: tool_id,
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        assert_eq!(
            results[0].error.as_ref().unwrap().code,
            ToolErrorCode::TooLarge
        );
        assert!(engine.artifacts.list().is_empty());
    }

    #[test]
    fn concurrent_jsonrpc_provider_rejects_oversized_output_data() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let mut limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        limits.max_tool_output_bytes = 16;
        let tool_id = ToolId::parse("external_large_output").unwrap();
        let provider_id = ToolProviderId::parse("jsonrpc_large_output_provider").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_jsonrpc_tool(
                provider_id.clone(),
                tool_id.clone(),
                "External tool that returns oversized data.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                CustomToolOptions {
                    cacheable: false,
                    effects: ToolEffects::default(),
                    provider_resources: Vec::new(),
                },
                Arc::new(StaticJsonRpcTransport {
                    provider_id: provider_id.clone(),
                    tool_id: tool_id.clone(),
                    calls: Arc::clone(&calls),
                    response: JsonRpcToolResponse {
                        data: Some(serde_json::json!({
                            "payload": "this provider output is much too large"
                        })),
                        artifact: None,
                        limits: LimitInfo::default(),
                    },
                }),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.grant_tool(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("external-output-too-large".to_string()),
                index: 0,
                name: tool_id,
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        assert_eq!(
            results[0].error.as_ref().unwrap().code,
            ToolErrorCode::TooLarge
        );
        assert_eq!(results[0].limits.output_bytes, 0);
    }

    #[test]
    fn concurrent_tool_provider_concurrency_is_bounded_per_provider() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let mut limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        limits.max_tool_provider_concurrency_per_provider = 1;
        limits.max_tool_provider_ms = 500;
        let tool_id = ToolId::parse("counted_custom_check").unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_custom(
                tool_id.clone(),
                "Counting custom check.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "additionalProperties": true
                }),
                false,
                Arc::new(CountingSlowCustomTool {
                    active: Arc::clone(&active),
                    max_seen: Arc::clone(&max_seen),
                }),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = trusted_custom_capabilities();
        capabilities.grant_tool(tool_id.clone(), ToolGrant::allow_custom_read_only());

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![
                ModelToolCall {
                    call_id: ToolCallId("counted-a".to_string()),
                    index: 0,
                    name: tool_id.clone(),
                    raw_arguments: r#"{"value":"a"}"#.to_string(),
                },
                ModelToolCall {
                    call_id: ToolCallId("counted-b".to_string()),
                    index: 1,
                    name: tool_id.clone(),
                    raw_arguments: r#"{"value":"b"}"#.to_string(),
                },
            ],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.ok));
        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
        let metrics = engine.snapshot_tool_metrics();
        let counted_metrics = &metrics[&ToolMetricKey::in_process(&tool_id)];
        assert_eq!(counted_metrics.calls, 2);
        assert_eq!(counted_metrics.successes, 2);
        assert_eq!(
            counted_metrics.input_bytes,
            r#"{"value":"a"}"#.len() + r#"{"value":"b"}"#.len()
        );
        assert!(counted_metrics.latency_ms >= counted_metrics.max_latency_ms);
        assert!(counted_metrics.max_latency_ms > 0);
        assert!(counted_metrics.queue_wait_ms >= counted_metrics.max_queue_wait_ms);
        assert!(counted_metrics.max_queue_wait_ms > 0);
    }

    #[test]
    fn concurrent_in_process_tool_provider_panic_is_contained() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = RuntimeLimits::standard(1, 64 * 1024, 10);
        let tool_id = ToolId::parse("panic_custom_check").unwrap();
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_custom(
                tool_id.clone(),
                "Panic custom check.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                false,
                Arc::new(PanicCustomTool),
            )
            .unwrap();
        let engine =
            ToolEngine::with_registry(snapshot, Arc::new(limits), Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut capabilities = trusted_custom_capabilities();
        capabilities.grant_tool(tool_id.clone(), ToolGrant::allow_custom_read_only());

        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("panic-call".to_string()),
                index: 0,
                name: tool_id.clone(),
                raw_arguments: "{}".to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));

        assert_eq!(results.len(), 1);
        assert!(!results[0].ok);
        assert_eq!(results[0].provider_id, ToolProviderId::in_process());
        assert_eq!(
            results[0].error.as_ref().unwrap().code,
            ToolErrorCode::Internal
        );
        let metrics = engine.snapshot_tool_metrics();
        let panic_metrics = &metrics[&ToolMetricKey::in_process(&tool_id)];
        assert_eq!(panic_metrics.errors, 1);
    }

    #[test]
    fn concurrent_registry_executes_allowed_custom_tool() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "hello\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 10);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 10));
        let tool_id = ToolId::parse("host_custom_check").unwrap();
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_custom(
                tool_id.clone(),
                "Host engine supplied custom reviewer check.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    },
                    "required": ["value"],
                    "additionalProperties": false
                }),
                false,
                Arc::new(EchoCustomTool),
            )
            .unwrap();
        let engine = ToolEngine::with_registry(snapshot, limits, Arc::new(registry)).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let denied = runtime.block_on(engine.execute_batch(
            test_scope("session"),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("denied-custom".to_string()),
                index: 0,
                name: tool_id.clone(),
                raw_arguments: r#"{"value":"ok"}"#.to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(denied.len(), 1);
        assert!(!denied[0].ok);
        assert_eq!(
            denied[0].error.as_ref().unwrap().code,
            ToolErrorCode::ToolNotAllowed
        );

        let mut allowed_capabilities = trusted_custom_capabilities();
        allowed_capabilities.grant_tool(tool_id.clone(), ToolGrant::allow_custom_read_only());
        let results = runtime.block_on(engine.execute_batch(
            test_scope_with_capabilities("session", allowed_capabilities),
            TurnId(0),
            vec![ModelToolCall {
                call_id: ToolCallId("custom".to_string()),
                index: 0,
                name: tool_id.clone(),
                raw_arguments: r#"{"value":"ok"}"#.to_string(),
            }],
            tokio_util::sync::CancellationToken::new(),
        ));
        assert_eq!(results.len(), 1);
        assert!(results[0].ok);
        assert_eq!(results[0].tool_name, tool_id);
        assert!(results[0].artifact_id.is_some());
        let data = results[0].data.as_ref().unwrap().to_string();
        assert!(data.contains("[REDACTED]"));
        assert!(!data.contains("AKIA1234567890ABCDEF"));
        let metrics = engine.snapshot_tool_metrics();
        let custom_key = ToolMetricKey::in_process(&tool_id);
        assert_eq!(custom_key.provider_id(), Some(ToolProviderId::in_process()));
        let custom_metrics = &metrics[&custom_key];
        assert_eq!(custom_metrics.calls, 2);
        assert_eq!(custom_metrics.successes, 1);
        assert_eq!(custom_metrics.errors, 1);
    }

    #[test]
    fn concurrent_tool_registry_rejects_alias_collisions() {
        let mut registry = ToolRegistry::review_defaults().unwrap();
        let alias = ToolId::parse("provider_visible_name").unwrap();
        let first = ToolId::parse("first_custom_alias_check").unwrap();
        registry
            .register_custom_with_alias_and_effects(
                first.clone(),
                alias.clone(),
                "First custom check.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                CustomToolOptions {
                    cacheable: false,
                    effects: ToolEffects::custom_read_only(),
                    provider_resources: Vec::new(),
                },
                Arc::new(EchoCustomTool),
            )
            .unwrap();

        let table = registry.alias_table().unwrap();
        assert_eq!(table.alias_for(&first), Some(&alias));
        assert_eq!(table.tool_for_alias(&alias), Some(&first));

        let duplicate = registry.register_custom_with_alias_and_effects(
            ToolId::parse("second_custom_alias_check").unwrap(),
            alias,
            "Second custom check.",
            serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            CustomToolOptions {
                cacheable: false,
                effects: ToolEffects::custom_read_only(),
                provider_resources: Vec::new(),
            },
            Arc::new(EchoCustomTool),
        );
        assert!(duplicate.is_err());
    }

    #[test]
    fn concurrent_review_defaults_register_all_sync_builtin_tools() {
        let registry = ToolRegistry::review_defaults().unwrap();
        let schemas = registry.schemas();
        for tool in all_builtin_tools() {
            let id = ToolId::from(tool);
            assert!(
                registry.definition(&id).is_some(),
                "missing concurrent registry definition for {}",
                tool.as_str()
            );
            assert!(
                schemas.iter().any(|schema| schema.id == id),
                "missing concurrent model schema for {}",
                tool.as_str()
            );
        }
    }

    #[test]
    fn concurrent_job_bridge_preserves_sync_repo_root_scope() {
        let capabilities = capabilities_from_mask(ToolMask::review_read_only());
        assert!(capabilities.fs_scope.cwd.is_none());
        assert!(capabilities.fs_scope.allowed_roots.is_empty());
        assert!(capabilities
            .fs_scope
            .allows(&RepoPath::parse("README.md").unwrap()));
        assert!(capabilities
            .fs_scope
            .allows(&RepoPath::parse("src/lib.rs").unwrap()));
        for tool in all_builtin_tools() {
            assert!(
                capabilities.allows_tool(&ToolId::from(tool)),
                "missing job-bridge capability for {}",
                tool.as_str()
            );
        }
    }

    #[test]
    fn concurrent_executes_every_sync_builtin_tool() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("src")).unwrap();
        fs::write(
            temp.path().join("README.md"),
            "use docs::needle;\nimport example from 'example';\nneedle\n",
        )
        .unwrap();
        fs::write(temp.path().join("src/lib.rs"), "pub fn needle() {}\n").unwrap();
        fs::write(
            temp.path().join("src/lib_test.rs"),
            "use crate::lib;\n#[test] fn needle_test() {}\n",
        )
        .unwrap();
        let change = test_change_with_file("src/lib.rs");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let engine = ToolEngine::new(snapshot, limits).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        for (index, tool) in all_builtin_tools().enumerate() {
            let results = runtime.block_on(engine.execute_batch(
                test_scope("session"),
                TurnId(index as u32),
                vec![ModelToolCall {
                    call_id: ToolCallId(format!("call-{}", tool.as_str())),
                    index: 0,
                    name: ToolId::from(tool),
                    raw_arguments: builtin_args(tool),
                }],
                tokio_util::sync::CancellationToken::new(),
            ));
            assert_eq!(results.len(), 1, "unexpected result count for {tool:?}");
            assert_eq!(results[0].tool_name, ToolId::from(tool));
            assert!(
                results[0].ok,
                "concurrent builtin {} failed: {:?}",
                tool.as_str(),
                results[0].error
            );
        }
    }

    #[test]
    fn concurrent_runtime_emits_lifecycle_events() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let output = Arc::new(std::sync::Mutex::new(Vec::new()));
        let emitter = Arc::new(EventEmitter {
            run_id: "run-1".to_string(),
            attempt: 0,
            redaction_policy_id: "test-redaction".to_string(),
            state: std::sync::Mutex::new(EventEmitterState {
                seq: 0,
                writer: Box::new(SharedWriter(Arc::clone(&output))),
            }),
        });
        let runtime = JobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(MockReviewModel::new(
                "README.md".to_string(),
                "needle".to_string(),
            )))),
            tools,
            policy: Arc::new(ReviewerPolicy::new()),
            limits,
            review_revision_id: change.head_revision_id.clone(),
            events: RuntimeEventDispatcher::new(None, Some(emitter)),
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions(vec![SessionSpec {
            scope: test_scope("session"),
        }]));
        assert_eq!(report.completed_sessions, 1);

        let bytes = output.lock().unwrap().clone();
        let text = String::from_utf8(bytes).unwrap();
        let events = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let event_types = events
            .iter()
            .map(|event| event["eventType"].as_str().unwrap())
            .collect::<Vec<_>>();
        for expected in [
            "session_started",
            "model_call_started",
            "model_call_completed",
            "tool_call_requested",
            "artifact_recorded",
            "finding_validated",
            "session_finished",
        ] {
            assert!(
                event_types.contains(&expected),
                "missing {expected} in {event_types:?}"
            );
        }
        assert!(events
            .iter()
            .any(|event| event["findingId"].as_str().is_some()));
        let completed_tool_call_ids = events
            .iter()
            .filter(|event| event["eventType"].as_str() == Some("tool_call_completed"))
            .filter_map(|event| event["toolCallId"].as_str())
            .collect::<std::collections::HashSet<_>>();
        for artifact_event in events
            .iter()
            .filter(|event| event["eventType"].as_str() == Some("artifact_recorded"))
        {
            let tool_call_id = artifact_event["toolCallId"].as_str().unwrap();
            assert!(
                completed_tool_call_ids.contains(tool_call_id),
                "artifact event {tool_call_id} missing matching tool_call_completed"
            );
        }
    }

    #[test]
    fn concurrent_runtime_rejects_terminal_before_evidence() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let runtime = JobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(PrematureTerminalModel))),
            tools,
            policy: Arc::new(ReviewerPolicy::new()),
            limits,
            review_revision_id: change.head_revision_id.clone(),
            events: RuntimeEventDispatcher::none(),
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions(vec![SessionSpec {
            scope: test_scope("session"),
        }]));
        assert_eq!(report.findings, 0);
        assert_eq!(report.tool_counts.record_finding, 0);
        assert!(report.counters.tool_errors > 0);
        assert!(!report.benchmark_valid);
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("read_diff")));
    }

    #[test]
    fn concurrent_runtime_reports_max_turn_exhaustion_without_terminal() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let runtime = JobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(EvidenceOnlyModel))),
            tools,
            policy: Arc::new(ReviewerPolicy::new()),
            limits,
            review_revision_id: change.head_revision_id.clone(),
            events: RuntimeEventDispatcher::none(),
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions(vec![SessionSpec {
            scope: test_scope_with_budget("session", 1, 8),
        }]));
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.model_calls, 1);
        assert_eq!(report.tool_counts.read_diff, 1);
        assert_eq!(report.tool_counts.read_file, 1);
        assert_eq!(report.tool_counts.search_text, 1);
        assert_eq!(report.findings, 0);
        assert!(!report.benchmark_valid);
        assert!(report
            .benchmark_failures
            .iter()
            .any(|failure| failure.contains("only 0/1 sessions completed")));
    }

    #[test]
    fn concurrent_runtime_enforces_session_tool_budget_with_batched_calls() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let runtime = JobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(EvidenceOnlyModel))),
            tools,
            policy: Arc::new(ReviewerPolicy::new()),
            limits,
            review_revision_id: change.head_revision_id.clone(),
            events: RuntimeEventDispatcher::none(),
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions(vec![SessionSpec {
            scope: test_scope_with_budget("session", 4, 2),
        }]));
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.tool_calls, 2);
        assert_eq!(report.tool_counts.read_diff, 1);
        assert_eq!(report.tool_counts.read_file, 1);
        assert_eq!(report.tool_counts.search_text, 0);
        assert_eq!(report.counters.tool_errors, 1);
        let search_metrics = &report.tool_metrics[&ToolMetricKey::builtin(ToolName::SearchText)];
        assert_eq!(search_metrics.calls, 1);
        assert_eq!(search_metrics.errors, 1);
        assert!(!report.benchmark_valid);
    }

    #[test]
    fn concurrent_runtime_reports_cancelled_sessions() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let runtime = JobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(MockReviewModel::new(
                "README.md".to_string(),
                "needle".to_string(),
            )))),
            tools,
            policy: Arc::new(ReviewerPolicy::new()),
            limits,
            review_revision_id: change.head_revision_id.clone(),
            events: RuntimeEventDispatcher::none(),
        };
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions_with_cancel(
            vec![SessionSpec {
                scope: test_scope("session"),
            }],
            cancel,
        ));
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.model_calls, 0);
        assert_eq!(report.tool_calls, 0);
        assert_eq!(report.findings, 0);
        assert!(!report.benchmark_valid);
    }

    #[test]
    fn concurrent_runtime_cancellation_during_model_call_stops_before_tools() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let cancel = tokio_util::sync::CancellationToken::new();
        let model_calls = Arc::new(AtomicUsize::new(0));
        let event_sink = Arc::new(crate::reviewer::runtime_events::InMemoryEventSink::default());
        let runtime_event_sink: Arc<dyn crate::reviewer::runtime_events::EventSink> =
            event_sink.clone();
        let runtime = JobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(CancellingModel {
                parent_cancel: cancel.clone(),
                calls: Arc::clone(&model_calls),
            }))),
            tools,
            policy: Arc::new(ReviewerPolicy::new()),
            limits,
            review_revision_id: change.head_revision_id.clone(),
            events: RuntimeEventDispatcher::new(Some(runtime_event_sink), None),
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let report = tokio.block_on(runtime.run_sessions_with_cancel(
            vec![SessionSpec {
                scope: test_scope("session"),
            }],
            cancel,
        ));

        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.model_calls, 1);
        assert_eq!(report.model_metrics.calls, 1);
        assert_eq!(report.model_metrics.successes, 0);
        assert_eq!(report.model_metrics.errors, 1);
        assert_eq!(report.model_metrics.retries, 0);
        assert_eq!(report.tool_calls, 0);
        assert_eq!(report.counters.tool_errors, 0);
        assert!(!report.benchmark_valid);

        let records = event_sink.records();
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    &record.event,
                    crate::reviewer::runtime_events::RuntimeEvent::ModelStarted { .. }
                ))
                .count(),
            1
        );
        assert!(!records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::runtime_events::RuntimeEvent::ModelCompleted { .. }
        )));
        assert!(!records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::runtime_events::RuntimeEvent::ToolBatchStarted { .. }
        )));
        assert!(records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::runtime_events::RuntimeEvent::SessionFinished { status, .. }
                if status == "cancelled"
        )));
    }

    #[test]
    fn concurrent_runtime_cancellation_during_jsonrpc_tool_marks_provider_cancelled() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let cancel = tokio_util::sync::CancellationToken::new();
        let model_calls = Arc::new(AtomicUsize::new(0));
        let transport_calls = Arc::new(AtomicUsize::new(0));
        let tool_id = ToolId::parse("external_cancellable_tool").unwrap();
        let provider_id = ToolProviderId::parse("jsonrpc_cancellable_provider").unwrap();
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_jsonrpc_tool(
                provider_id.clone(),
                tool_id.clone(),
                "External tool used to prove cancellation propagation.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                CustomToolOptions {
                    cacheable: false,
                    effects: ToolEffects::default(),
                    provider_resources: Vec::new(),
                },
                Arc::new(CancellingJsonRpcTransport {
                    provider_id: provider_id.clone(),
                    tool_id: tool_id.clone(),
                    parent_cancel: cancel.clone(),
                    calls: Arc::clone(&transport_calls),
                }),
            )
            .unwrap();
        let tools = Arc::new(
            ToolEngine::with_registry(snapshot.clone(), Arc::clone(&limits), Arc::new(registry))
                .unwrap(),
        );
        let event_sink = Arc::new(crate::reviewer::runtime_events::InMemoryEventSink::default());
        let runtime_event_sink: Arc<dyn crate::reviewer::runtime_events::EventSink> =
            event_sink.clone();
        let runtime = JobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(SingleExternalToolModel {
                tool_id: tool_id.clone(),
                calls: Arc::clone(&model_calls),
            }))),
            tools,
            policy: Arc::new(ReviewerPolicy::new()),
            limits,
            review_revision_id: change.head_revision_id.clone(),
            events: RuntimeEventDispatcher::new(Some(runtime_event_sink), None),
        };
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.grant_tool(
            tool_id.clone(),
            ToolGrant {
                allow: true,
                max_calls: None,
                effects_allowed: ToolEffects::default(),
            },
        );
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let report = tokio.block_on(runtime.run_sessions_with_cancel(
            vec![SessionSpec {
                scope: test_scope_with_capabilities("session", capabilities),
            }],
            cancel,
        ));

        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport_calls.load(Ordering::SeqCst), 1);
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.model_calls, 1);
        assert_eq!(report.tool_calls, 0);
        assert_eq!(report.counters.tool_errors, 1);
        let tool_metrics = &report.tool_metrics[&ToolMetricKey::new(&provider_id, &tool_id)];
        assert_eq!(tool_metrics.calls, 1);
        assert_eq!(tool_metrics.errors, 1);
        assert_eq!(tool_metrics.cancellations, 1);
        assert_eq!(tool_metrics.timeouts, 0);
        let provider_health = report
            .provider_health
            .iter()
            .find(|snapshot| snapshot.provider_id == provider_id)
            .expect("provider health");
        assert_eq!(provider_health.state, ToolProviderHealthState::Degraded);
        assert_eq!(provider_health.calls, 1);
        assert_eq!(provider_health.errors, 1);
        assert_eq!(provider_health.cancellations, 1);

        let records = event_sink.records();
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    &record.event,
                    crate::reviewer::runtime_events::RuntimeEvent::ModelStarted { .. }
                ))
                .count(),
            1
        );
        assert!(records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::runtime_events::RuntimeEvent::ToolCallCompleted {
                error_code: Some(ToolErrorCode::Cancelled),
                ..
            }
        )));
        assert!(records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::runtime_events::RuntimeEvent::SessionFinished { status, .. }
                if status == "cancelled"
        )));
    }

    #[test]
    fn concurrent_runtime_cancellation_after_tool_result_skips_transcript_append() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let cancel = tokio_util::sync::CancellationToken::new();
        let tool_id = ToolId::parse("cancel_after_success_tool").unwrap();
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let model_calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::review_defaults().unwrap();
        registry
            .register_custom(
                tool_id.clone(),
                "Custom tool that cancels the run immediately before returning success.",
                serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
                false,
                Arc::new(CancelAfterSuccessCustomTool {
                    parent_cancel: cancel.clone(),
                    calls: Arc::clone(&tool_calls),
                }),
            )
            .unwrap();
        let tools = Arc::new(
            ToolEngine::with_registry(snapshot.clone(), Arc::clone(&limits), Arc::new(registry))
                .unwrap(),
        );
        let event_sink = Arc::new(crate::reviewer::runtime_events::InMemoryEventSink::default());
        let runtime_event_sink: Arc<dyn crate::reviewer::runtime_events::EventSink> =
            event_sink.clone();
        let runtime = JobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(
                CancelAfterToolResultModel {
                    tool_id: tool_id.clone(),
                    calls: Arc::clone(&model_calls),
                },
            ))),
            tools,
            policy: Arc::new(ReviewerPolicy::new()),
            limits,
            review_revision_id: change.head_revision_id.clone(),
            events: RuntimeEventDispatcher::new(Some(runtime_event_sink), None),
        };
        let mut capabilities = trusted_custom_capabilities();
        capabilities.grant_tool(tool_id.clone(), ToolGrant::allow_custom_read_only());
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let report = tokio.block_on(runtime.run_sessions_with_cancel(
            vec![SessionSpec {
                scope: test_scope_with_capabilities("session", capabilities),
            }],
            cancel,
        ));

        assert_eq!(model_calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.model_calls, 1);
        assert_eq!(report.tool_calls, 0);
        assert_eq!(report.counters.tool_errors, 0);
        let custom_metrics = &report.tool_metrics[&ToolMetricKey::in_process(&tool_id)];
        assert_eq!(custom_metrics.calls, 1);
        assert_eq!(custom_metrics.successes, 1);

        let records = event_sink.records();
        assert!(records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::runtime_events::RuntimeEvent::ModelCompleted {
                tool_call_count: 1,
                ..
            }
        )));
        assert!(!records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::runtime_events::RuntimeEvent::ToolCallCompleted { .. }
        )));
        assert!(records.iter().any(|record| matches!(
            &record.event,
            crate::reviewer::runtime_events::RuntimeEvent::SessionFinished { status, .. }
                if status == "cancelled"
        )));
    }

    #[test]
    fn concurrent_runtime_retries_retryable_provider_error() {
        let report = run_with_model(Arc::new(FailThenMockModel::new(
            1,
            ModelFailure::RetryableProvider,
        )));
        assert_eq!(report.completed_sessions, 1);
        assert_eq!(report.findings, 1);
        assert_eq!(
            report.model_calls, 3,
            "first turn should retry once, then terminal turn should run once"
        );
        assert_eq!(report.model_metrics.calls, 3);
        assert_eq!(report.model_metrics.successes, 2);
        assert_eq!(report.model_metrics.errors, 0);
        assert_eq!(report.model_metrics.retries, 1);
        assert_eq!(report.model_metrics.total_tokens, report.total_tokens);
        assert!(report.model_metrics.latency_ms >= report.model_metrics.max_latency_ms);
        assert!(report.model_metrics.max_latency_ms > 0);
        assert!(report.benchmark_valid);
    }

    #[test]
    fn concurrent_runtime_retries_timeout() {
        let report = run_with_model(Arc::new(FailThenMockModel::new(1, ModelFailure::Timeout)));
        assert_eq!(report.completed_sessions, 1);
        assert_eq!(report.findings, 1);
        assert_eq!(report.model_calls, 3);
        assert_eq!(report.model_metrics.calls, 3);
        assert_eq!(report.model_metrics.successes, 2);
        assert_eq!(report.model_metrics.errors, 0);
        assert_eq!(report.model_metrics.retries, 1);
        assert!(report.model_metrics.latency_ms >= report.model_metrics.max_latency_ms);
        assert!(report.model_metrics.max_latency_ms > 0);
        assert!(report.benchmark_valid);
    }

    #[test]
    fn concurrent_runtime_reports_model_cost_estimates() {
        let report = run_with_model(Arc::new(CostedMockModel {
            inner: MockReviewModel::new("README.md".to_string(), "needle".to_string()),
        }));
        assert_eq!(report.completed_sessions, 1);
        assert_eq!(
            report.model_metrics.costed_calls,
            report.model_metrics.successes
        );
        assert_eq!(report.model_metrics.unpriced_calls, 0);
        assert_eq!(
            report.model_metrics.estimated_input_cost_micro_usd,
            report.model_metrics.input_tokens * 2
        );
        assert_eq!(
            report.model_metrics.estimated_output_cost_micro_usd,
            report.model_metrics.output_tokens * 3
        );
        assert_eq!(
            report.model_metrics.estimated_total_cost_micro_usd,
            report.model_metrics.estimated_input_cost_micro_usd
                + report.model_metrics.estimated_output_cost_micro_usd
        );
    }

    #[test]
    fn real_provider_canary_gate_is_opt_in() {
        let config = OpenAiProviderCanaryConfig::from_env(DEFAULT_MODEL);
        let expect_live = config.enabled && std::env::var("OPENAI_API_KEY").is_ok();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let reports = runtime.block_on(run_openai_provider_canaries(
            config,
            Arc::new(EnvCredentialResolver),
        ));
        let protocols = reports
            .iter()
            .map(|report| report.protocol)
            .collect::<Vec<_>>();
        assert_eq!(protocols.as_slice(), openai_provider_canary_protocols());
        let evidence = ModelProviderCanaryEvidence::from_reports(reports);
        if expect_live {
            assert!(
                evidence.require_passed().is_ok(),
                "credentialed real-provider canaries failed: {evidence:#?}"
            );
        } else {
            assert!(
                evidence.gate.skipped == openai_provider_canary_protocols().len()
                    && !evidence.gate.valid,
                "uncredentialed real-provider canaries should skip safely: {evidence:#?}"
            );
        }
    }

    #[test]
    fn concurrent_runtime_does_not_retry_non_retryable_provider_error() {
        let report = run_with_model(Arc::new(FailThenMockModel::new(
            1,
            ModelFailure::NonRetryableProvider,
        )));
        assert_eq!(report.completed_sessions, 0);
        assert_eq!(report.model_calls, 1);
        assert_eq!(report.model_metrics.calls, 1);
        assert_eq!(report.model_metrics.successes, 0);
        assert_eq!(report.model_metrics.errors, 1);
        assert_eq!(report.model_metrics.retries, 0);
        assert!(report.model_metrics.latency_ms >= report.model_metrics.max_latency_ms);
        assert!(report.model_metrics.max_latency_ms > 0);
        assert_eq!(report.tool_calls, 0);
        assert_eq!(report.findings, 0);
        assert!(!report.benchmark_valid);
    }

    #[test]
    fn concurrent_runtime_marks_provider_error_session_failed() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let output = Arc::new(std::sync::Mutex::new(Vec::new()));
        let emitter = Arc::new(EventEmitter {
            run_id: "run-1".to_string(),
            attempt: 0,
            redaction_policy_id: "test-redaction".to_string(),
            state: std::sync::Mutex::new(EventEmitterState {
                seq: 0,
                writer: Box::new(SharedWriter(Arc::clone(&output))),
            }),
        });
        let runtime = JobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(Arc::new(FailThenMockModel::new(
                1,
                ModelFailure::NonRetryableProvider,
            )))),
            tools,
            policy: Arc::new(ReviewerPolicy::new()),
            limits,
            review_revision_id: change.head_revision_id.clone(),
            events: RuntimeEventDispatcher::new(None, Some(emitter)),
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = tokio.block_on(runtime.run_sessions(vec![SessionSpec {
            scope: test_scope("session"),
        }]));
        assert_eq!(report.completed_sessions, 0);

        let bytes = output.lock().unwrap().clone();
        let text = String::from_utf8(bytes).unwrap();
        let events = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let session_finished = events
            .iter()
            .find(|event| event["eventType"].as_str() == Some("session_finished"))
            .expect("missing session_finished");
        assert_eq!(session_finished["payload"]["state"], "failed");
        assert!(events.iter().any(|event| {
            event["eventType"].as_str() == Some("error")
                && event["payload"]["retrying"].as_bool() == Some(false)
        }));
    }

    #[derive(Debug)]
    struct EchoCustomTool;

    #[derive(Debug)]
    struct PrematureTerminalModel;

    #[derive(Debug)]
    struct EvidenceOnlyModel;

    #[derive(Debug)]
    struct PublicFacadeModel {
        path: String,
        query: String,
    }

    #[derive(Debug)]
    struct PublicCustomToolModel(crate::reviewer::ids::ToolId);

    #[derive(Debug)]
    struct PublicJsonRpcReviewTool {
        provider_id: crate::reviewer::tool_adapters::ToolProviderId,
        tool_id: String,
        expected_provider_resources: Vec<crate::reviewer::tool_adapters::ProviderResourceId>,
        calls: Arc<AtomicUsize>,
    }

    struct LoopbackJsonRpcToolServer {
        endpoint: String,
        handle: std::thread::JoinHandle<serde_json::Value>,
    }

    impl LoopbackJsonRpcToolServer {
        fn spawn() -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let handle = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let request_bytes = read_http_request(&mut stream);
                let (headers, body) = split_http_body(&request_bytes);
                let content_length = http_content_length(headers);
                let request: serde_json::Value =
                    serde_json::from_slice(&body[..content_length]).unwrap();
                let result = serde_json::to_value(JsonRpcToolResponse {
                    data: Some(serde_json::json!({
                        "wire": "ok",
                        "value": request["params"]["arguments"]["value"].clone()
                    })),
                    artifact: None,
                    limits: LimitInfo::default(),
                })
                .unwrap();
                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request["id"].clone(),
                    "result": result
                });
                let response_body = serde_json::to_vec(&response).unwrap();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response_body.len()
                )
                .unwrap();
                stream.write_all(&response_body).unwrap();
                request
            });
            Self { endpoint, handle }
        }

        fn endpoint(&self) -> String {
            self.endpoint.clone()
        }

        fn join(self) -> serde_json::Value {
            self.handle.join().expect("loopback JSON-RPC server")
        }
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> Vec<u8> {
        let mut request_bytes = Vec::new();
        loop {
            let mut chunk = [0u8; 4096];
            let bytes_read = stream.read(&mut chunk).unwrap();
            if bytes_read == 0 {
                break;
            }
            request_bytes.extend_from_slice(&chunk[..bytes_read]);
            if let Some((headers, body)) = try_split_http_body(&request_bytes) {
                let content_length = http_content_length(headers);
                if body.len() >= content_length {
                    break;
                }
            }
        }
        request_bytes
    }

    fn split_http_body(request_bytes: &[u8]) -> (&str, &[u8]) {
        try_split_http_body(request_bytes).expect("complete HTTP request")
    }

    fn try_split_http_body(request_bytes: &[u8]) -> Option<(&str, &[u8])> {
        let body_start = request_bytes
            .windows(b"\r\n\r\n".len())
            .position(|window| window == b"\r\n\r\n")?
            + b"\r\n\r\n".len();
        let headers = std::str::from_utf8(&request_bytes[..body_start]).ok()?;
        Some((headers, &request_bytes[body_start..]))
    }

    fn http_content_length(headers: &str) -> usize {
        headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    return value.trim().parse::<usize>().ok();
                }
                None
            })
            .expect("HTTP content-length")
    }

    #[derive(Debug)]
    struct CancellingModel {
        parent_cancel: tokio_util::sync::CancellationToken,
        calls: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct SingleExternalToolModel {
        tool_id: ToolId,
        calls: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct CancelAfterToolResultModel {
        tool_id: ToolId,
        calls: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct FailThenMockModel {
        failures_left: AtomicUsize,
        failure: ModelFailure,
        inner: MockReviewModel,
    }

    impl FailThenMockModel {
        fn new(failures: usize, failure: ModelFailure) -> Self {
            Self {
                failures_left: AtomicUsize::new(failures),
                failure,
                inner: MockReviewModel::new("README.md".to_string(), "needle".to_string()),
            }
        }
    }

    #[derive(Debug, Copy, Clone)]
    enum ModelFailure {
        RetryableProvider,
        NonRetryableProvider,
        Timeout,
    }

    impl ModelFailure {
        fn error(self) -> RuntimeError {
            match self {
                Self::RetryableProvider => RuntimeError::Provider {
                    status: Some(429),
                    retryable: true,
                },
                Self::NonRetryableProvider => RuntimeError::Provider {
                    status: Some(400),
                    retryable: false,
                },
                Self::Timeout => RuntimeError::Timeout,
            }
        }
    }

    #[async_trait]
    impl ConcurrentModelClient for FailThenMockModel {
        async fn complete(
            &self,
            scope: &SessionScope,
            transcript: &[ConversationItem],
            turn_id: TurnId,
            cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            if self
                .failures_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    if left > 0 {
                        Some(left - 1)
                    } else {
                        None
                    }
                })
                .is_ok()
            {
                return Err(self.failure.error());
            }
            self.inner
                .complete(scope, transcript, turn_id, cancel)
                .await
        }
    }

    #[async_trait]
    impl crate::reviewer::ReviewModel for CancellingModel {
        async fn complete_review(
            &self,
            _request: crate::reviewer::ReviewModelRequest,
            cancel: crate::reviewer::Cancellation,
        ) -> crate::reviewer::runtime::RuntimeResult<crate::reviewer::ReviewModelTurn> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.parent_cancel.cancel();
            cancel.cancelled().await;
            Err(RuntimeError::Cancelled)
        }
    }

    #[async_trait]
    impl ConcurrentModelClient for CancellingModel {
        async fn complete(
            &self,
            _scope: &SessionScope,
            _transcript: &[ConversationItem],
            _turn_id: TurnId,
            cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.parent_cancel.cancel();
            cancel.cancelled().await;
            Err(RuntimeError::Cancelled)
        }
    }

    #[async_trait]
    impl ConcurrentModelClient for SingleExternalToolModel {
        async fn complete(
            &self,
            scope: &SessionScope,
            _transcript: &[ConversationItem],
            turn_id: TurnId,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ModelTurn::ToolCalls {
                usage: TokenUsage {
                    input_tokens: 1,
                    output_tokens: 1,
                    total_tokens: 2,
                },
                calls: vec![ModelToolCall {
                    call_id: ToolCallId(format!("{}-{}-external", scope.id.0, turn_id.0)),
                    index: 0,
                    name: self.tool_id.clone(),
                    raw_arguments: "{}".to_string(),
                }],
            })
        }
    }

    #[async_trait]
    impl ConcurrentModelClient for CancelAfterToolResultModel {
        async fn complete(
            &self,
            scope: &SessionScope,
            _transcript: &[ConversationItem],
            turn_id: TurnId,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ModelTurn::ToolCalls {
                usage: TokenUsage {
                    input_tokens: 1,
                    output_tokens: 1,
                    total_tokens: 2,
                },
                calls: vec![ModelToolCall {
                    call_id: ToolCallId(format!(
                        "{}-{}-cancel-after-success",
                        scope.id.0, turn_id.0
                    )),
                    index: 0,
                    name: self.tool_id.clone(),
                    raw_arguments: "{}".to_string(),
                }],
            })
        }
    }

    struct CostedMockModel {
        inner: MockReviewModel,
    }

    #[async_trait]
    impl ConcurrentModelClient for CostedMockModel {
        async fn complete(
            &self,
            scope: &SessionScope,
            transcript: &[ConversationItem],
            turn_id: TurnId,
            cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            self.inner
                .complete(scope, transcript, turn_id, cancel)
                .await
        }

        fn estimate_cost(&self, usage: &TokenUsage) -> Option<ModelCostEstimate> {
            let input_cost_micro_usd = usage.input_tokens * 2;
            let output_cost_micro_usd = usage.output_tokens * 3;
            Some(ModelCostEstimate {
                input_cost_micro_usd,
                output_cost_micro_usd,
                total_cost_micro_usd: input_cost_micro_usd + output_cost_micro_usd,
            })
        }
    }

    #[async_trait]
    impl crate::reviewer::ReviewModel for PublicFacadeModel {
        async fn complete_review(
            &self,
            request: crate::reviewer::ReviewModelRequest,
            _cancel: crate::reviewer::Cancellation,
        ) -> crate::reviewer::runtime::RuntimeResult<crate::reviewer::ReviewModelTurn> {
            let usage = TokenUsage {
                input_tokens: request.transcript_item_count() as u64,
                output_tokens: 1,
                total_tokens: request.transcript_item_count() as u64 + 1,
            };
            if request.tool_result_count() == 0 {
                return Ok(crate::reviewer::ReviewModelTurn::ToolCalls {
                    usage,
                    calls: vec![
                        reviewer_call(&request, "diff", "read_diff", serde_json::json!({})),
                        reviewer_call(
                            &request,
                            "file",
                            "read_file",
                            serde_json::json!({ "path": self.path }),
                        ),
                        reviewer_call(
                            &request,
                            "search",
                            "search_text",
                            serde_json::json!({ "query": self.query }),
                        ),
                    ],
                });
            }
            Ok(crate::reviewer::ReviewModelTurn::ToolCalls {
                usage,
                calls: vec![reviewer_call(
                    &request,
                    "finding",
                    "record_finding",
                    serde_json::json!({
                        "title": "public facade finding",
                        "claim": "public facade gathered diff, file, and search evidence"
                    }),
                )],
            })
        }
    }

    #[async_trait]
    impl crate::reviewer::ReviewModel for PublicCustomToolModel {
        async fn complete_review(
            &self,
            request: crate::reviewer::ReviewModelRequest,
            _cancel: crate::reviewer::Cancellation,
        ) -> crate::reviewer::runtime::RuntimeResult<crate::reviewer::ReviewModelTurn> {
            let usage = TokenUsage {
                input_tokens: request.transcript_item_count() as u64,
                output_tokens: 1,
                total_tokens: request.transcript_item_count() as u64 + 1,
            };
            if request.tool_result_count() > 0 {
                return Ok(crate::reviewer::ReviewModelTurn::Text {
                    content: "custom tool completed".to_string(),
                    usage,
                });
            }
            Ok(crate::reviewer::ReviewModelTurn::ToolCalls {
                usage,
                calls: vec![crate::reviewer::ReviewToolCall::new(
                    self.0.as_str(),
                    serde_json::json!({ "value": "ok" }),
                )
                .with_call_id(request.tool_call_id("custom"))],
            })
        }
    }

    #[async_trait]
    impl crate::reviewer::tool_adapters::JsonRpcToolTransport for PublicJsonRpcReviewTool {
        async fn call(
            &self,
            request: crate::reviewer::tool_adapters::JsonRpcToolRequest,
            _cancel: crate::reviewer::Cancellation,
        ) -> crate::reviewer::runtime::RuntimeResult<
            crate::reviewer::tool_adapters::JsonRpcToolResponse,
        > {
            assert_eq!(request.provider_id, self.provider_id);
            assert_eq!(request.tool_id.as_str(), self.tool_id);
            assert_eq!(request.provider_resources, self.expected_provider_resources);
            assert_eq!(request.arguments["value"], "ok");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::reviewer::tool_adapters::JsonRpcToolResponse {
                data: Some(serde_json::json!({
                    "provider": request.provider_id.as_str(),
                    "tool": request.tool_id.as_str(),
                    "value": request.arguments["value"]
                })),
                artifact: None,
                limits: crate::reviewer::metrics::LimitInfo::default(),
            })
        }
    }

    #[async_trait]
    impl ConcurrentModelClient for EvidenceOnlyModel {
        async fn complete(
            &self,
            scope: &SessionScope,
            _transcript: &[ConversationItem],
            turn_id: TurnId,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            let session_id = &scope.id;
            Ok(ModelTurn::ToolCalls {
                usage: TokenUsage {
                    input_tokens: 1,
                    output_tokens: 1,
                    total_tokens: 2,
                },
                calls: vec![
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-diff", session_id.0, turn_id.0)),
                        index: 0,
                        name: ToolId::from(ToolName::ReadDiff),
                        raw_arguments: "{}".to_string(),
                    },
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-file", session_id.0, turn_id.0)),
                        index: 1,
                        name: ToolId::from(ToolName::ReadFile),
                        raw_arguments: serde_json::json!({ "path": "README.md" }).to_string(),
                    },
                    ModelToolCall {
                        call_id: ToolCallId(format!("{}-{}-search", session_id.0, turn_id.0)),
                        index: 2,
                        name: ToolId::from(ToolName::SearchText),
                        raw_arguments: serde_json::json!({ "query": "needle" }).to_string(),
                    },
                ],
            })
        }
    }

    #[async_trait]
    impl ConcurrentModelClient for PrematureTerminalModel {
        async fn complete(
            &self,
            scope: &SessionScope,
            _transcript: &[ConversationItem],
            turn_id: TurnId,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<ModelTurn> {
            let session_id = &scope.id;
            Ok(ModelTurn::ToolCalls {
                usage: TokenUsage {
                    input_tokens: 1,
                    output_tokens: 1,
                    total_tokens: 2,
                },
                calls: vec![ModelToolCall {
                    call_id: ToolCallId(format!("{}-{}-finding", session_id.0, turn_id.0)),
                    index: 0,
                    name: ToolId::from(ToolName::RecordFinding),
                    raw_arguments: serde_json::json!({
                        "title": "premature finding",
                        "claim": "terminal call before evidence"
                    })
                    .to_string(),
                }],
            })
        }
    }

    #[derive(Clone)]
    struct SharedWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl crate::reviewer::ReviewToolHandler for EchoCustomTool {
        async fn execute_review_tool(
            &self,
            context: crate::reviewer::ReviewToolContext,
            args: serde_json::Value,
            _cancel: crate::reviewer::Cancellation,
        ) -> crate::reviewer::runtime::RuntimeResult<crate::reviewer::ReviewToolOutput> {
            Ok(crate::reviewer::ReviewToolOutput {
                data: Some(serde_json::json!({
                    "tool": context.tool_id,
                    "session": context.session_id,
                    "value": args["value"],
                    "secret": "AKIA1234567890ABCDEF"
                })),
                artifact: Some(crate::reviewer::ReviewToolArtifact {
                    key: "host_custom_check".to_string(),
                    content: "artifact AKIA1234567890ABCDEF".to_string(),
                }),
            })
        }
    }

    struct ResourceScopedReviewTool {
        expected_provider_resources: Vec<ProviderResourceId>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl crate::reviewer::ReviewToolHandler for ResourceScopedReviewTool {
        async fn execute_review_tool(
            &self,
            context: crate::reviewer::ReviewToolContext,
            _args: serde_json::Value,
            _cancel: crate::reviewer::Cancellation,
        ) -> crate::reviewer::runtime::RuntimeResult<crate::reviewer::ReviewToolOutput> {
            assert_eq!(context.provider_resources, self.expected_provider_resources);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::reviewer::ReviewToolOutput {
                data: Some(serde_json::json!({
                    "resources": context
                        .provider_resources
                        .iter()
                        .map(|resource| resource.as_str())
                        .collect::<Vec<_>>()
                })),
                artifact: None,
            })
        }
    }

    #[async_trait]
    impl CustomToolHandler for EchoCustomTool {
        async fn execute(
            &self,
            context: CustomToolContext,
            args: serde_json::Value,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> crate::runtime::contracts::RuntimeResult<CustomToolOutput> {
            Ok(CustomToolOutput {
                data: Some(serde_json::json!({
                    "tool": context.tool_id.as_str(),
                    "session": context.session_id.0,
                    "value": args["value"],
                    "secret": "AKIA1234567890ABCDEF"
                })),
                artifact: Some(CustomToolArtifact {
                    key: ArtifactKey("host_custom_check".to_string()),
                    content: "artifact AKIA1234567890ABCDEF".to_string(),
                }),
                limits: LimitInfo::default(),
            })
        }
    }

    struct CancelAfterSuccessCustomTool {
        parent_cancel: tokio_util::sync::CancellationToken,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CustomToolHandler for CancelAfterSuccessCustomTool {
        async fn execute(
            &self,
            context: CustomToolContext,
            _args: serde_json::Value,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> crate::runtime::contracts::RuntimeResult<CustomToolOutput> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.parent_cancel.cancel();
            Ok(CustomToolOutput {
                data: Some(serde_json::json!({
                    "tool": context.tool_id.as_str(),
                    "cancelled": true
                })),
                artifact: None,
                limits: LimitInfo::default(),
            })
        }
    }

    struct SlowCustomTool;

    #[async_trait]
    impl CustomToolHandler for SlowCustomTool {
        async fn execute(
            &self,
            _context: CustomToolContext,
            _args: serde_json::Value,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> crate::runtime::contracts::RuntimeResult<CustomToolOutput> {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(CustomToolOutput::default())
        }
    }

    struct EchoJsonRpcTransport {
        provider_id: ToolProviderId,
        tool_id: ToolId,
        calls: Arc<AtomicUsize>,
    }

    struct StaticJsonRpcTransport {
        provider_id: ToolProviderId,
        tool_id: ToolId,
        calls: Arc<AtomicUsize>,
        response: JsonRpcToolResponse,
    }

    struct ResourceCheckingJsonRpcTransport {
        provider_id: ToolProviderId,
        tool_id: ToolId,
        calls: Arc<AtomicUsize>,
        expected_provider_resources: Vec<ProviderResourceId>,
        response: JsonRpcToolResponse,
    }

    struct CancellingJsonRpcTransport {
        provider_id: ToolProviderId,
        tool_id: ToolId,
        parent_cancel: tokio_util::sync::CancellationToken,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl JsonRpcToolTransport for EchoJsonRpcTransport {
        async fn call(
            &self,
            request: JsonRpcToolRequest,
            cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<JsonRpcToolResponse> {
            assert!(!cancel.is_cancelled());
            assert_eq!(request.provider_id, self.provider_id);
            assert_eq!(request.tool_id, self.tool_id);
            assert_eq!(request.arguments["value"], "ok");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(JsonRpcToolResponse {
                data: Some(serde_json::json!({
                    "value": request.arguments["value"],
                    "secret": "AKIA1234567890ABCDEF"
                })),
                artifact: Some(CustomToolArtifact {
                    key: ArtifactKey("external_jsonrpc_artifact".to_string()),
                    content: "external AKIA1234567890ABCDEF".to_string(),
                }),
                limits: LimitInfo::default(),
            })
        }
    }

    #[async_trait]
    impl JsonRpcToolTransport for StaticJsonRpcTransport {
        async fn call(
            &self,
            request: JsonRpcToolRequest,
            cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<JsonRpcToolResponse> {
            assert!(!cancel.is_cancelled());
            assert_eq!(request.provider_id, self.provider_id);
            assert_eq!(request.tool_id, self.tool_id);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.response.clone())
        }
    }

    #[async_trait]
    impl JsonRpcToolTransport for ResourceCheckingJsonRpcTransport {
        async fn call(
            &self,
            request: JsonRpcToolRequest,
            cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<JsonRpcToolResponse> {
            assert!(!cancel.is_cancelled());
            assert_eq!(request.provider_id, self.provider_id);
            assert_eq!(request.tool_id, self.tool_id);
            assert_eq!(request.provider_resources, self.expected_provider_resources);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.response.clone())
        }
    }

    #[async_trait]
    impl JsonRpcToolTransport for CancellingJsonRpcTransport {
        async fn call(
            &self,
            request: JsonRpcToolRequest,
            cancel: tokio_util::sync::CancellationToken,
        ) -> RuntimeResult<JsonRpcToolResponse> {
            assert_eq!(request.provider_id, self.provider_id);
            assert_eq!(request.tool_id, self.tool_id);
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.parent_cancel.cancel();
            cancel.cancelled().await;
            Err(RuntimeError::Cancelled)
        }
    }

    struct CountingSlowCustomTool {
        active: Arc<AtomicUsize>,
        max_seen: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CustomToolHandler for CountingSlowCustomTool {
        async fn execute(
            &self,
            _context: CustomToolContext,
            _args: serde_json::Value,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> crate::runtime::contracts::RuntimeResult<CustomToolOutput> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_seen.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(CustomToolOutput::default())
        }
    }

    struct PanicCustomTool;

    #[async_trait]
    impl CustomToolHandler for PanicCustomTool {
        async fn execute(
            &self,
            _context: CustomToolContext,
            _args: serde_json::Value,
            _cancel: tokio_util::sync::CancellationToken,
        ) -> crate::runtime::contracts::RuntimeResult<CustomToolOutput> {
            panic!("intentional custom tool panic")
        }
    }

    fn reviewer_call(
        request: &crate::reviewer::ReviewModelRequest,
        suffix: &str,
        tool_id: &str,
        arguments: serde_json::Value,
    ) -> crate::reviewer::ReviewToolCall {
        crate::reviewer::ReviewToolCall::new(tool_id, arguments)
            .with_call_id(request.tool_call_id(suffix))
    }

    fn public_budget() -> crate::reviewer::AgentBudget {
        crate::reviewer::AgentBudget {
            max_turns: 4,
            max_tool_calls: 8,
            max_prompt_tokens: 32_000,
            max_output_tokens: 512,
        }
    }

    fn test_scope(id: &str) -> SessionScope {
        test_scope_with_capabilities(id, CapabilitySet::review_read_only())
    }

    fn search_call(call_id: &str) -> ModelToolCall {
        ModelToolCall {
            call_id: ToolCallId(call_id.to_string()),
            index: 0,
            name: ToolId::from(ToolName::SearchText),
            raw_arguments: r#"{"query":"needle"}"#.to_string(),
        }
    }

    async fn wait_for_inflight_tool(engine: &ToolEngine) {
        for _ in 0..50 {
            if engine.inflight_tool_count_for_test() > 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for in-flight tool cell");
    }

    async fn wait_for_search_dedupe_waiter(engine: &ToolEngine) {
        for _ in 0..50 {
            if engine.snapshot_counters().search_dedupe_waiters > 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for search dedupe waiter");
    }

    fn trusted_custom_capabilities() -> CapabilitySet {
        let mut capabilities = CapabilitySet::review_read_only();
        capabilities.runtime_authority.host_read = true;
        capabilities
    }

    fn minimal_report() -> ConcurrentRunReport {
        ConcurrentRunReport {
            runtime: "concurrent",
            sessions: 1,
            completed_sessions: 1,
            model_calls: 1,
            tool_calls: 1,
            tool_counts: ToolCounts::default(),
            findings: 1,
            publishable_findings: 1,
            elapsed_ms: 1,
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            artifacts: 0,
            artifact_bytes: 0,
            counters: ConcurrentCounters::default(),
            tool_metrics: Default::default(),
            provider_health: Vec::new(),
            snapshot_metrics: Vec::new(),
            model_metrics: Default::default(),
            terminal_diagnostics: Vec::new(),
            benchmark_valid: true,
            benchmark_failures: Vec::new(),
        }
    }

    fn run_with_model(model: Arc<dyn ConcurrentModelClient>) -> ConcurrentRunReport {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("README.md"), "needle\n").unwrap();
        let change = test_change_with_file("README.md");
        let policy = PathPolicyV1::bench(64, 20);
        let snapshot = RepoSnapshot::build(temp.path(), &policy, &change).unwrap();
        let limits = Arc::new(RuntimeLimits::standard(1, 64 * 1024, 20));
        let tools = Arc::new(ToolEngine::new(Arc::clone(&snapshot), Arc::clone(&limits)).unwrap());
        let runtime = JobRuntime {
            snapshot,
            model_router: Arc::new(StaticModelRouter::new(model)),
            tools,
            policy: Arc::new(ReviewerPolicy::new()),
            limits,
            review_revision_id: change.head_revision_id.clone(),
            events: RuntimeEventDispatcher::none(),
        };
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio.block_on(runtime.run_sessions(vec![SessionSpec {
            scope: test_scope("session"),
        }]))
    }

    fn test_scope_with_budget(id: &str, max_turns: usize, max_tool_calls: usize) -> SessionScope {
        let mut scope = test_scope(id);
        scope.budget.max_turns = max_turns;
        scope.budget.max_tool_calls = max_tool_calls;
        scope
    }

    fn all_builtin_tools() -> impl Iterator<Item = ToolName> {
        ToolName::review_read_only_tools().iter().copied()
    }

    fn builtin_args(tool: ToolName) -> String {
        match tool {
            ToolName::ListChangedFiles | ToolName::ReadDiff | ToolName::ListFiles => {
                "{}".to_string()
            }
            ToolName::ReadFile | ToolName::ReadBaseFile | ToolName::ReadHeadFile => {
                serde_json::json!({ "path": "README.md" }).to_string()
            }
            ToolName::SearchText => serde_json::json!({ "query": "needle" }).to_string(),
            ToolName::FindRelatedFiles | ToolName::FindTestsForFile | ToolName::ListImports => {
                serde_json::json!({ "path": "src/lib.rs" }).to_string()
            }
            ToolName::RecordFinding => serde_json::json!({
                "title": "benchmark finding",
                "claim": "claim"
            })
            .to_string(),
            ToolName::ChallengeFinding => serde_json::json!({
                "finding_id": "finding-1",
                "rationale": "challenge"
            })
            .to_string(),
            ToolName::Finish => serde_json::json!({ "reason": "done" }).to_string(),
        }
    }

    fn test_scope_with_capabilities(id: &str, capabilities: CapabilitySet) -> SessionScope {
        SessionScope {
            id: SessionId(id.to_string()),
            role: Role::Generalist,
            objective: "test review scope".to_string(),
            snapshot_id: None,
            model_profile_id: Some("test-model".to_string()),
            capabilities,
            budget: AgentBudget {
                max_turns: 4,
                max_tool_calls: 8,
                max_prompt_tokens: 32_000,
                max_output_tokens: 512,
            },
        }
    }

    fn bench_args(repo: &Path, terminal_policy: BenchTerminalPolicy) -> BenchArgs {
        BenchArgs {
            repo: repo.to_path_buf(),
            sessions: 3,
            max_active: 3,
            max_turns: 10,
            max_tool_calls: 14,
            hold_ms: 0,
            max_file_kb: 200,
            max_search_matches: 120,
            model: DEFAULT_MODEL.to_string(),
            max_output_tokens: 128,
            terminal_policy,
        }
    }

    fn test_repo(path: &Path) -> RepoContext {
        RepoContext::new(path.to_path_buf(), PathPolicyV1::bench(64, 10)).unwrap()
    }

    fn test_change_with_file(path: &str) -> ChangeScopeV1 {
        ChangeScopeV1 {
            kind: ChangeKind::LocalDiff,
            change_id: "test".to_string(),
            source_ref: "head".to_string(),
            target_ref: "base".to_string(),
            base_revision_id: "base".to_string(),
            head_revision_id: "head".to_string(),
            merge_base_revision_id: None,
            changed_files_manifest_ref: None,
            diff_manifest_ref: None,
            snapshot_mode: SnapshotMode::WorktreeHead,
            rename_detection: RenameDetection::None,
            changed_files: vec![ChangedFileEntryV1 {
                status: ChangedFileStatus::Modified,
                old_path: Some(PathBuf::from(path)),
                new_path: Some(PathBuf::from(path)),
                old_content_hash: None,
                new_content_hash: None,
                is_binary: false,
                is_generated: false,
            }],
        }
    }
}
