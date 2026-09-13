<?php
declare(strict_types=1);

if (PHP_SAPI !== 'cli') {
    fwrite(STDERR, "CLI only.\n");
    exit(1);
}

$config = require __DIR__ . '/consorcio-v2-config.php';
require_once __DIR__ . '/consorcio-v2-lib.php';
require_once __DIR__ . '/consorcio-v2-release-gate.php';
date_default_timezone_set((string)$config['project']['timezone']);

function v2_record_publication_attempt(array $config, array $payload, bool $successful = false): void
{
    $state = rtrim((string)$config['paths']['state'], '/');
    v2_atomic_write_json($state . '/last_publication_attempt.json', $payload);
    if ($successful) {
        v2_atomic_write_json($state . '/last_publication_success.json', $payload);
    }
}

$force = in_array('--force', $argv, true);
$dryRun = in_array('--dry-run', $argv, true);

v2_ensure_dirs([
    $config['paths']['releases'],
    $config['paths']['tmp'],
    $config['paths']['state'],
    $config['paths']['logs'],
    $config['paths']['locks'],
]);

$lock = v2_acquire_lock((string)$config['paths']['lock']);
if ($lock === null) {
    fwrite(STDERR, "Outro ciclo V2 está em andamento.\n");
    exit(20);
}

$stage = null;
$releaseRoot = null;
$previousRoot = null;
$swapped = false;
$manifestSha = null;
$sourceCommit = null;

try {
    $sourceCommit = v2_resolve_source_commit($config);
    $manifestUrl = v2_raw_url($config, $sourceCommit, (string)$config['source']['manifest_path']);
    $metaRaw = v2_fetch($manifestUrl, $config['source']);
    $remoteMeta = v2_decode_json($metaRaw, $manifestUrl);
    v2_validate_backend_release_meta($remoteMeta, $config);
    $entries = v2_manifest_entries($remoteMeta);
    $manifestSha = hash('sha256', $metaRaw);

    $quarantineFile = rtrim((string)$config['paths']['state'], '/') . '/rejected_releases.json';
    $quarantine = is_file($quarantineFile) ? v2_read_json($quarantineFile) : ['items' => []];
    $rejected = is_array($quarantine['items'] ?? null) ? $quarantine['items'] : [];
    if (!$force && isset($rejected[$manifestSha])) {
        throw new RuntimeException('Manifesto está em quarentena após rejeição anterior; use --force somente após corrigir a causa.');
    }

    $current = (string)$config['paths']['current'];
    if (is_link($current)) {
        try {
            $currentRoot = v2_resolve_current_root($current);
            $currentResult = v2_validate_release_backend($currentRoot, $config);
            if (hash_equals($manifestSha, (string)$currentResult['manifest_sha256']) && !$force) {
                $payload = v2_validation_payload('success', $currentRoot, $manifestSha, $config);
                $payload['validated_artifacts'] = $currentResult['validated_artifacts'];
                $payload['source_commit'] = $sourceCommit;
                $payload['release_fingerprint'] = $currentResult['meta']['backend_release']['release_fingerprint'] ?? null;
                $payload['reason'] = 'remote_manifest_unchanged_current_healthy';
                v2_record_validation($config, $payload);

                $publication = [
                    'attempted_at' => gmdate('c'),
                    'result' => 'no_change',
                    'release_id' => basename($currentRoot),
                    'release_root' => $currentRoot,
                    'source_commit' => $sourceCommit,
                    'manifest_sha256' => $manifestSha,
                    'release_fingerprint' => $currentResult['meta']['backend_release']['release_fingerprint'] ?? null,
                    'reason' => 'remote_manifest_unchanged_current_healthy',
                ];
                v2_record_publication_attempt($config, $publication, true);
                v2_log($config, 'publish-v2', 'Sem mudança; current revalidado.', $publication);
                fwrite(STDOUT, json_encode($publication, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL);
                exit(10);
            }
        } catch (Throwable $currentError) {
            v2_log($config, 'publish-v2', 'Current existente está inválido; iniciando reparo da mesma geração ou atualização.', [
                'error' => $currentError->getMessage(),
                'source_commit' => $sourceCommit,
                'manifest_sha256' => $manifestSha,
            ]);
        }
    }

    if ($dryRun) {
        $publication = [
            'attempted_at' => gmdate('c'),
            'result' => 'dry_run',
            'source_commit' => $sourceCommit,
            'manifest_sha256' => $manifestSha,
            'release_fingerprint' => $remoteMeta['backend_release']['release_fingerprint'] ?? null,
            'artifacts' => count($entries),
        ];
        v2_record_publication_attempt($config, $publication, false);
        fwrite(STDOUT, json_encode($publication, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL);
        exit(0);
    }

    $stage = rtrim((string)$config['paths']['tmp'], '/') . '/stage-' . getmypid() . '-' . bin2hex(random_bytes(5));
    v2_ensure_dirs([$stage . '/global', $stage . '/seo']);
    if (file_put_contents($stage . '/global/meta.json', $metaRaw, LOCK_EX) === false) {
        throw new RuntimeException('Falha ao gravar meta.json no staging.');
    }

    $distPrefix = trim((string)$config['source']['dist_prefix'], '/');
    foreach ($entries as $relative => $entry) {
        $url = v2_raw_url($config, $sourceCommit, $distPrefix . '/' . $relative);
        $body = v2_fetch($url, $config['source']);
        if (strlen($body) !== $entry['size_bytes']) {
            throw new RuntimeException("Tamanho remoto divergente antes da publicação: {$relative}");
        }
        $hash = hash('sha256', $body);
        if (!hash_equals($entry['sha256'], $hash)) {
            throw new RuntimeException("SHA-256 remoto divergente antes da publicação: {$relative}");
        }
        $target = $stage . '/' . $relative;
        v2_ensure_dirs([dirname($target)]);
        if (file_put_contents($target, $body, LOCK_EX) === false) {
            throw new RuntimeException("Falha ao gravar staging: {$relative}");
        }
    }

    $stageValidation = v2_validate_release_backend($stage, $config);
    if (!hash_equals($manifestSha, (string)$stageValidation['manifest_sha256'])) {
        throw new RuntimeException('Assinatura do manifesto no staging divergiu do remoto.');
    }

    $releaseId = gmdate('Ymd\THis\Z') . '_' . substr($sourceCommit, 0, 8) . '_' . substr($manifestSha, 0, 8);
    $releaseRoot = rtrim((string)$config['paths']['releases'], '/') . '/' . $releaseId;
    if (file_exists($releaseRoot)) {
        throw new RuntimeException("Release já existe: {$releaseRoot}");
    }
    if (!@rename($stage, $releaseRoot)) {
        throw new RuntimeException('Falha ao promover staging para release.');
    }
    $stage = null;

    if (is_link($current)) {
        $previousRoot = v2_resolve_current_root($current);
    }
    v2_atomic_symlink_swap($current, $releaseRoot);
    $swapped = true;

    $post = v2_validate_release_backend(v2_resolve_current_root($current), $config);
    if (!hash_equals($manifestSha, (string)$post['manifest_sha256'])) {
        throw new RuntimeException('Validação pós-swap não corresponde ao manifesto remoto.');
    }

    $currentState = [
        'published_at' => gmdate('c'),
        'release_id' => basename($releaseRoot),
        'release_root' => $releaseRoot,
        'source_commit' => $sourceCommit,
        'manifest_sha256' => $manifestSha,
        'pipeline_version' => $post['meta']['pipeline_version'] ?? null,
        'source_fingerprint' => $post['meta']['source_fingerprint'] ?? null,
        'release_fingerprint' => $post['meta']['backend_release']['release_fingerprint'] ?? null,
        'degraded_sources' => $post['meta']['freshness']['degraded_sources'] ?? [],
    ];
    v2_atomic_write_json(rtrim((string)$config['paths']['state'], '/') . '/current_release.json', $currentState);

    $validationState = v2_validation_payload('success', $releaseRoot, $manifestSha, $config);
    $validationState['validated_artifacts'] = $post['validated_artifacts'];
    $validationState['source_commit'] = $sourceCommit;
    $validationState['release_fingerprint'] = $post['meta']['backend_release']['release_fingerprint'] ?? null;
    v2_record_validation($config, $validationState);

    $publication = [
        'attempted_at' => gmdate('c'),
        'result' => 'success',
        'release_id' => basename($releaseRoot),
        'release_root' => $releaseRoot,
        'source_commit' => $sourceCommit,
        'manifest_sha256' => $manifestSha,
        'release_fingerprint' => $post['meta']['backend_release']['release_fingerprint'] ?? null,
        'degraded_sources' => $post['meta']['freshness']['degraded_sources'] ?? [],
    ];
    v2_record_publication_attempt($config, $publication, true);

    if (isset($rejected[$manifestSha])) {
        unset($rejected[$manifestSha]);
        v2_atomic_write_json($quarantineFile, ['items' => $rejected]);
    }

    v2_prune_releases($config, $releaseRoot);
    v2_log($config, 'publish-v2', 'Release V2 publicada.', $currentState);
    fwrite(STDOUT, json_encode($currentState, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL);
    exit(0);
} catch (Throwable $e) {
    $rollbackRestored = false;
    if ($swapped && is_string($previousRoot) && is_dir($previousRoot)) {
        try {
            $rollbackValidation = v2_validate_release_backend($previousRoot, $config);
            v2_atomic_symlink_swap((string)$config['paths']['current'], $previousRoot);
            $rollbackRestored = true;

            $restoredValidation = v2_validation_payload(
                'success',
                $previousRoot,
                (string)$rollbackValidation['manifest_sha256'],
                $config
            );
            $restoredValidation['validated_artifacts'] = $rollbackValidation['validated_artifacts'];
            $restoredValidation['release_fingerprint'] = $rollbackValidation['meta']['backend_release']['release_fingerprint'] ?? null;
            $restoredValidation['reason'] = 'automatic_rollback_after_publication_failure';
            v2_record_validation($config, $restoredValidation);
        } catch (Throwable $rollbackError) {
            v2_log($config, 'publish-v2', 'Rollback automático falhou.', [
                'publish_error' => $e->getMessage(),
                'rollback_error' => $rollbackError->getMessage(),
            ]);
        }
    }

    if (is_string($manifestSha)) {
        $quarantineFile = rtrim((string)$config['paths']['state'], '/') . '/rejected_releases.json';
        $quarantine = is_file($quarantineFile) ? v2_read_json($quarantineFile) : ['items' => []];
        $items = is_array($quarantine['items'] ?? null) ? $quarantine['items'] : [];
        $items[$manifestSha] = [
            'rejected_at' => gmdate('c'),
            'source_commit' => $sourceCommit,
            'error' => $e->getMessage(),
        ];
        v2_atomic_write_json($quarantineFile, ['items' => $items]);
    }

    $activeRoot = null;
    try {
        if (is_link((string)$config['paths']['current'])) {
            $activeRoot = v2_resolve_current_root((string)$config['paths']['current']);
        }
    } catch (Throwable $_ignored) {
    }

    $failure = [
        'attempted_at' => gmdate('c'),
        'result' => 'failure',
        'active_release_id' => $activeRoot ? basename($activeRoot) : null,
        'active_release_root' => $activeRoot,
        'target_release_root' => $releaseRoot,
        'target_manifest_sha256' => $manifestSha,
        'target_source_commit' => $sourceCommit,
        'automatic_rollback_restored' => $rollbackRestored,
        'error' => $e->getMessage(),
    ];
    v2_record_publication_attempt($config, $failure, false);
    v2_log($config, 'publish-v2', 'Publicação rejeitada.', $failure);
    fwrite(STDERR, $e->getMessage() . PHP_EOL);
    exit(50);
} finally {
    if (is_string($stage) && is_dir($stage)) {
        v2_recursive_delete($stage);
    }
    v2_release_lock($lock);
}
