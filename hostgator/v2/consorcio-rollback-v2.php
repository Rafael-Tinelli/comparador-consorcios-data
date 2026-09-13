<?php
declare(strict_types=1);

if (PHP_SAPI !== 'cli') {
    fwrite(STDERR, "CLI only.\n");
    exit(1);
}

$config = require __DIR__ . '/consorcio-v2-config.php';
require_once __DIR__ . '/consorcio-v2-lib.php';
date_default_timezone_set((string)$config['project']['timezone']);

$requested = null;
foreach ($argv as $arg) {
    if (str_starts_with($arg, '--to=')) {
        $requested = trim(substr($arg, 5));
    }
}

v2_ensure_dirs([
    $config['paths']['state'],
    $config['paths']['logs'],
    $config['paths']['locks'],
]);

$lock = v2_acquire_lock((string)$config['paths']['lock']);
if ($lock === null) {
    fwrite(STDERR, "Outro ciclo V2 está em andamento.\n");
    exit(20);
}

try {
    $currentLink = (string)$config['paths']['current'];
    $currentRoot = is_link($currentLink) ? v2_resolve_current_root($currentLink) : null;
    $releasesDir = rtrim((string)$config['paths']['releases'], '/');

    if (is_string($requested) && $requested !== '') {
        if (basename($requested) !== $requested || preg_match('/^[A-Za-z0-9._-]+$/', $requested) !== 1) {
            throw new RuntimeException('Release informada em --to é inválida.');
        }
        $target = $releasesDir . '/' . $requested;
    } else {
        $candidates = [];
        foreach (glob($releasesDir . '/*', GLOB_ONLYDIR) ?: [] as $path) {
            if ($currentRoot !== null && realpath($path) === realpath($currentRoot)) {
                continue;
            }
            $candidates[] = ['path' => $path, 'mtime' => filemtime($path) ?: 0];
        }
        usort($candidates, fn(array $a, array $b): int => $b['mtime'] <=> $a['mtime']);
        if (!$candidates) {
            throw new RuntimeException('Nenhuma release anterior disponível para rollback.');
        }
        $target = $candidates[0]['path'];
    }

    $pre = v2_validate_release($target, $config);
    v2_atomic_symlink_swap($currentLink, $target);
    $active = v2_resolve_current_root($currentLink);
    $post = v2_validate_release($active, $config);
    if (!hash_equals((string)$pre['manifest_sha256'], (string)$post['manifest_sha256'])) {
        throw new RuntimeException('Manifesto mudou durante rollback.');
    }

    $state = [
        'rolled_back_at' => gmdate('c'),
        'release_id' => basename($active),
        'release_root' => $active,
        'manifest_sha256' => $post['manifest_sha256'],
        'pipeline_version' => $post['meta']['pipeline_version'] ?? null,
        'source_fingerprint' => $post['meta']['source_fingerprint'] ?? null,
        'reason' => 'manual_rollback',
    ];
    v2_atomic_write_json(rtrim((string)$config['paths']['state'], '/') . '/current_release.json', $state);

    $validation = v2_validation_payload('success', $active, (string)$post['manifest_sha256'], $config);
    $validation['validated_artifacts'] = $post['validated_artifacts'];
    $validation['reason'] = 'manual_rollback';
    v2_record_validation($config, $validation);
    v2_log($config, 'rollback-v2', 'Rollback concluído.', $state);
    fwrite(STDOUT, json_encode($state, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL);
    exit(0);
} catch (Throwable $e) {
    v2_log($config, 'rollback-v2', 'Rollback rejeitado.', ['error' => $e->getMessage()]);
    fwrite(STDERR, $e->getMessage() . PHP_EOL);
    exit(50);
} finally {
    v2_release_lock($lock);
}
