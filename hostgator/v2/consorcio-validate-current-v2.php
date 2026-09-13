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

v2_ensure_dirs([
    $config['paths']['state'],
    $config['paths']['logs'],
    $config['paths']['locks'],
]);

$lock = v2_acquire_lock((string)$config['paths']['lock']);
if ($lock === null) {
    fwrite(STDERR, "Validação adiada: publicação/rollback em andamento.\n");
    exit(20);
}

$root = null;
$manifestSha = null;
try {
    $root = v2_resolve_current_root((string)$config['paths']['current']);
    $result = v2_validate_release_backend($root, $config);
    $manifestSha = $result['manifest_sha256'];
    $payload = v2_validation_payload('success', $root, $manifestSha, $config);
    $payload['validated_artifacts'] = $result['validated_artifacts'];
    $payload['backend_release_contract'] = $result['meta']['backend_release']['contract'] ?? null;
    $payload['release_fingerprint'] = $result['meta']['backend_release']['release_fingerprint'] ?? null;
    v2_record_validation($config, $payload);
    v2_log($config, 'validate-v2', 'Validação concluída.', $payload);
    fwrite(STDOUT, json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL);
    exit(0);
} catch (Throwable $e) {
    $payload = v2_validation_payload('failure', $root, $manifestSha, $config, [$e->getMessage()]);
    v2_record_validation($config, $payload);
    v2_log($config, 'validate-v2', 'Validação rejeitada.', $payload);
    fwrite(STDERR, $e->getMessage() . PHP_EOL);
    exit(50);
} finally {
    v2_release_lock($lock);
}
