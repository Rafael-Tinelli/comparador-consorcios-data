<?php
declare(strict_types=1);

if (PHP_SAPI !== 'cli') {
    exit(1);
}

$repoRoot = dirname(__DIR__);
require_once $repoRoot . '/hostgator/v2/consorcio-v2-lib.php';
require_once $repoRoot . '/hostgator/v2/consorcio-v2-release-gate.php';
$config = require $repoRoot . '/hostgator/v2/consorcio-v2-config.php';

$fixture = $argv[1] ?? null;
if (!is_string($fixture) || !is_dir($fixture)) {
    fwrite(STDERR, "Uso: php tests/test_hostgator_v2.php <release-root-v2>\n");
    exit(2);
}

$tmpBase = sys_get_temp_dir() . '/consorcio-v2-php-' . getmypid() . '-' . bin2hex(random_bytes(4));
v2_ensure_dirs([$tmpBase]);
$config['paths']['state'] = $tmpBase . '/state';
$config['paths']['logs'] = $tmpBase . '/logs';
$config['paths']['locks'] = $tmpBase . '/locks';
$config['paths']['lock'] = $tmpBase . '/locks/publication.lock';
$config['paths']['current'] = $tmpBase . '/current-v2';
$config['paths']['releases'] = $tmpBase . '/releases';
$config['paths']['tmp'] = $tmpBase . '/tmp';

function t_assert(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException('ASSERT: ' . $message);
    }
}

function t_copy_tree(string $src, string $dst): void
{
    v2_recursive_delete($dst);
    v2_ensure_dirs([$dst]);
    $it = new RecursiveIteratorIterator(
        new RecursiveDirectoryIterator($src, FilesystemIterator::SKIP_DOTS),
        RecursiveIteratorIterator::SELF_FIRST
    );
    foreach ($it as $item) {
        $target = $dst . '/' . $it->getSubPathName();
        if ($item->isDir()) {
            v2_ensure_dirs([$target]);
        } else {
            v2_ensure_dirs([dirname($target)]);
            if (!copy($item->getPathname(), $target)) {
                throw new RuntimeException("Falha ao copiar fixture: {$target}");
            }
        }
    }
}

function t_expect_failure(callable $fn, string $contains): void
{
    try {
        $fn();
    } catch (Throwable $e) {
        t_assert(str_contains($e->getMessage(), $contains), "falha deveria conter '{$contains}', veio '{$e->getMessage()}'");
        return;
    }
    throw new RuntimeException("ASSERT: esperava falha contendo '{$contains}'.");
}

function t_rewrite_meta(string $release, callable $mutator): void
{
    $path = rtrim($release, '/') . '/global/meta.json';
    $meta = v2_read_json($path);
    $mutator($meta);
    v2_atomic_write_json($path, $meta);
}

try {
    $baseline = v2_validate_release_backend($fixture, $config);
    t_assert(($baseline['validated_artifacts'] ?? 0) === 10, 'baseline deve validar 10 artefatos não-meta');
    t_assert(($baseline['meta']['backend_release']['contract'] ?? null) === 'comparador-v2-release.v1', 'baseline deve carregar contrato de release V2');
    t_assert(($baseline['meta']['freshness']['source_state_matches_consumed_bytes'] ?? null) === true, 'baseline deve confirmar estado ligado aos bytes consumidos');

    $missing = $tmpBase . '/missing-core';
    t_copy_tree($fixture, $missing);
    unlink($missing . '/global/administradoras.json');
    t_expect_failure(fn() => v2_validate_release_backend($missing, $config), 'Artefato declarado ausente');

    $tampered = $tmpBase . '/tampered';
    t_copy_tree($fixture, $tampered);
    file_put_contents($tampered . '/global/administradoras.json', " \n", FILE_APPEND);
    t_expect_failure(fn() => v2_validate_release_backend($tampered, $config), 'Tamanho divergente');

    $invalid = $tmpBase . '/invalid-json';
    t_copy_tree($fixture, $invalid);
    file_put_contents($invalid . '/seo/site.json', "{broken\n");
    t_expect_failure(fn() => v2_validate_release_backend($invalid, $config), 'Tamanho divergente');

    $extra = $tmpBase . '/extra-json';
    t_copy_tree($fixture, $extra);
    file_put_contents($extra . '/seo/nao-declarado.json', "{}\n");
    t_expect_failure(fn() => v2_validate_release_backend($extra, $config), 'Inventário físico diverge do manifesto');

    $noReleaseContract = $tmpBase . '/no-release-contract';
    t_copy_tree($fixture, $noReleaseContract);
    t_rewrite_meta($noReleaseContract, function (array &$meta): void {
        unset($meta['backend_release']);
    });
    t_expect_failure(fn() => v2_validate_release_backend($noReleaseContract, $config), 'backend_release');

    $noSourceStatus = $tmpBase . '/no-source-status';
    t_copy_tree($fixture, $noSourceStatus);
    t_rewrite_meta($noSourceStatus, function (array &$meta): void {
        unset($meta['source_status']);
    });
    t_expect_failure(fn() => v2_validate_release_backend($noSourceStatus, $config), 'source_status');

    $missingRequiredSource = $tmpBase . '/missing-required-source';
    t_copy_tree($fixture, $missingRequiredSource);
    t_rewrite_meta($missingRequiredSource, function (array &$meta): void {
        unset($meta['source_status']['bc_consorciobd']);
    });
    t_expect_failure(fn() => v2_validate_release_backend($missingRequiredSource, $config), 'source_status obrigatório ausente: bc_consorciobd');

    $noByteBinding = $tmpBase . '/no-byte-binding';
    t_copy_tree($fixture, $noByteBinding);
    t_rewrite_meta($noByteBinding, function (array &$meta): void {
        $meta['freshness']['source_state_matches_consumed_bytes'] = false;
    });
    t_expect_failure(fn() => v2_validate_release_backend($noByteBinding, $config), 'source_state_matches_consumed_bytes=true');

    $badDegraded = $tmpBase . '/bad-degraded';
    t_copy_tree($fixture, $badDegraded);
    t_rewrite_meta($badDegraded, function (array &$meta): void {
        $meta['source_status']['bc_filiais']['last_check_status'] = 'failure';
        $meta['freshness']['degraded_sources'] = [];
    });
    t_expect_failure(fn() => v2_validate_release_backend($badDegraded, $config), 'degraded_sources diverge');

    $badCompetence = $tmpBase . '/bad-competence';
    t_copy_tree($fixture, $badCompetence);
    t_rewrite_meta($badCompetence, function (array &$meta): void {
        $meta['source_status']['bc_consorciobd']['competence']['value'] = '202604';
    });
    t_expect_failure(fn() => v2_validate_release_backend($badCompetence, $config), 'Competência ConsorcioBD diverge');

    $stateSuccess = v2_validation_payload('success', $fixture, $baseline['manifest_sha256'], $config);
    v2_record_validation($config, $stateSuccess);
    $stateFailure = v2_validation_payload('failure', $fixture, $baseline['manifest_sha256'], $config, ['fixture failure']);
    v2_record_validation($config, $stateFailure);
    $attempt = v2_read_json($config['paths']['state'] . '/last_validation_attempt.json');
    $success = v2_read_json($config['paths']['state'] . '/last_validation_success.json');
    t_assert($attempt['result'] === 'failure', 'última tentativa deve registrar falha');
    t_assert($success['result'] === 'success', 'último sucesso deve permanecer sucesso anterior');

    $lock1 = v2_acquire_lock($config['paths']['lock']);
    t_assert(is_resource($lock1), 'primeiro lock deve ser obtido');
    $lock2 = v2_acquire_lock($config['paths']['lock']);
    t_assert($lock2 === null, 'segundo lock concorrente deve falhar');
    v2_release_lock($lock1);

    $releaseA = $tmpBase . '/releases/a';
    $releaseB = $tmpBase . '/releases/b';
    t_copy_tree($fixture, $releaseA);
    t_copy_tree($fixture, $releaseB);
    v2_atomic_symlink_swap($config['paths']['current'], $releaseA);
    t_assert(v2_resolve_current_root($config['paths']['current']) === realpath($releaseA), 'current deve apontar para A');
    v2_atomic_symlink_swap($config['paths']['current'], $releaseB);
    t_assert(v2_resolve_current_root($config['paths']['current']) === realpath($releaseB), 'current deve trocar atomicamente para B');

    echo json_encode([
        'status' => 'PASS',
        'validated_artifacts' => $baseline['validated_artifacts'],
        'negative_cases' => 10,
        'backend_release_gate' => true,
        'source_status_gate' => true,
        'required_source_gate' => true,
        'source_byte_binding_gate' => true,
        'competence_gate' => true,
        'degraded_state_gate' => true,
        'state_attempt_success_split' => true,
        'lock_concurrency' => true,
        'symlink_swap' => true,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT) . PHP_EOL;
    v2_recursive_delete($tmpBase);
    exit(0);
} catch (Throwable $e) {
    fwrite(STDERR, $e->getMessage() . PHP_EOL);
    v2_recursive_delete($tmpBase);
    exit(1);
}
