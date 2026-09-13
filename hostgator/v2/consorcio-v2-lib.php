<?php
declare(strict_types=1);

function v2_decode_json(string $raw, string $label): array
{
    $data = json_decode($raw, true);
    if (!is_array($data) || json_last_error() !== JSON_ERROR_NONE) {
        throw new RuntimeException("JSON inválido em {$label}: " . json_last_error_msg());
    }
    return $data;
}

function v2_read_json(string $path): array
{
    if (!is_file($path)) {
        throw new RuntimeException("Arquivo ausente: {$path}");
    }
    $raw = file_get_contents($path);
    if ($raw === false) {
        throw new RuntimeException("Falha ao ler: {$path}");
    }
    return v2_decode_json($raw, $path);
}

function v2_ensure_dirs(array $dirs): void
{
    foreach ($dirs as $dir) {
        if (!is_dir($dir) && !@mkdir($dir, 0775, true) && !is_dir($dir)) {
            throw new RuntimeException("Não foi possível criar diretório: {$dir}");
        }
    }
}

function v2_atomic_write_json(string $path, array $payload): void
{
    v2_ensure_dirs([dirname($path)]);
    $json = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    if ($json === false) {
        throw new RuntimeException("Falha ao serializar estado: {$path}");
    }
    $tmp = $path . '.tmp.' . getmypid() . '.' . bin2hex(random_bytes(4));
    if (file_put_contents($tmp, $json . PHP_EOL, LOCK_EX) === false) {
        throw new RuntimeException("Falha ao escrever estado temporário: {$tmp}");
    }
    if (!@rename($tmp, $path)) {
        @unlink($tmp);
        throw new RuntimeException("Falha ao publicar estado: {$path}");
    }
}

function v2_safe_filename(string $name): bool
{
    return $name !== ''
        && basename($name) === $name
        && preg_match('/^[A-Za-z0-9._-]+\.json$/', $name) === 1;
}

function v2_manifest_entries(array $meta): array
{
    if (!isset($meta['artifacts']) || !is_array($meta['artifacts'])) {
        throw new RuntimeException('meta.json sem artifacts.');
    }
    $entries = [];
    foreach (['global', 'seo'] as $family) {
        $rows = $meta['artifacts'][$family] ?? null;
        if (!is_array($rows)) {
            throw new RuntimeException("meta.json sem artifacts.{$family} válido.");
        }
        foreach ($rows as $row) {
            if (!is_array($row)) {
                throw new RuntimeException("Entrada inválida em artifacts.{$family}.");
            }
            $file = $row['file'] ?? null;
            $sha = $row['sha256'] ?? null;
            $size = $row['size_bytes'] ?? null;
            if (!is_string($file) || !v2_safe_filename($file)) {
                throw new RuntimeException("Nome de artefato inválido em {$family}.");
            }
            if (!is_string($sha) || preg_match('/^[a-f0-9]{64}$/', $sha) !== 1) {
                throw new RuntimeException("SHA-256 inválido para {$family}/{$file}.");
            }
            if (!is_int($size) || $size <= 0) {
                throw new RuntimeException("size_bytes inválido para {$family}/{$file}.");
            }
            $relative = $family . '/' . $file;
            if (isset($entries[$relative])) {
                throw new RuntimeException("Artefato duplicado no manifesto: {$relative}");
            }
            $entries[$relative] = [
                'family' => $family,
                'file' => $file,
                'sha256' => $sha,
                'size_bytes' => $size,
            ];
        }
    }
    return $entries;
}

function v2_payload_contract(array $payload): ?string
{
    if (isset($payload['metadata']) && is_array($payload['metadata']) && isset($payload['metadata']['contract'])) {
        return is_string($payload['metadata']['contract']) ? $payload['metadata']['contract'] : null;
    }
    return isset($payload['contract']) && is_string($payload['contract']) ? $payload['contract'] : null;
}

function v2_list_json_files(string $releaseRoot): array
{
    $files = [];
    foreach (['global', 'seo'] as $family) {
        $dir = rtrim($releaseRoot, '/') . '/' . $family;
        if (!is_dir($dir)) {
            continue;
        }
        foreach (glob($dir . '/*.json') ?: [] as $path) {
            $files[] = $family . '/' . basename($path);
        }
    }
    sort($files);
    return $files;
}

function v2_validate_admin_semantics(array $payload, array $config): void
{
    $items = $payload['items'] ?? null;
    if (!is_array($items)) {
        throw new RuntimeException('administradoras.json sem items.');
    }
    $minimum = (int)($config['validation']['minimum_administradoras'] ?? 100);
    if (count($items) < $minimum) {
        throw new RuntimeException("administradoras abaixo do mínimo: " . count($items));
    }
    $roots = [];
    foreach ($items as $index => $item) {
        if (!is_array($item)) {
            throw new RuntimeException("administradoras.items[{$index}] inválido.");
        }
        $root = $item['cnpj_root'] ?? null;
        if (!is_string($root) || preg_match('/^\d{8}$/', $root) !== 1) {
            throw new RuntimeException("cnpj_root inválido em administradoras.items[{$index}].");
        }
        if (isset($roots[$root])) {
            throw new RuntimeException("cnpj_root duplicado em administradoras: {$root}");
        }
        $roots[$root] = true;
        if (array_key_exists('scores', $item) || array_key_exists('score', $item) || array_key_exists('nota_geral', $item)) {
            throw new RuntimeException("Contrato V2 não permite score geral em administradoras: {$root}");
        }
        $publishable = $item['comparabilidade']['ranking_geral_publicavel'] ?? null;
        if ($publishable !== false) {
            throw new RuntimeException("ranking_geral_publicavel deve ser false em {$root}");
        }
    }
}

function v2_validate_release(string $releaseRoot, array $config): array
{
    $releaseRoot = rtrim($releaseRoot, '/');
    $metaPath = $releaseRoot . '/global/meta.json';
    if (!is_file($metaPath) || filesize($metaPath) <= 0) {
        throw new RuntimeException("meta.json ausente ou vazio: {$metaPath}");
    }
    $metaRaw = file_get_contents($metaPath);
    if ($metaRaw === false) {
        throw new RuntimeException('Falha ao ler meta.json.');
    }
    $meta = v2_decode_json($metaRaw, $metaPath);

    foreach (['pipeline_version', 'source_fingerprint', 'contracts', 'counts', 'quality', 'artifacts'] as $field) {
        if (!array_key_exists($field, $meta)) {
            throw new RuntimeException("meta.json sem campo obrigatório: {$field}");
        }
    }
    if (($meta['methodology']['general_score'] ?? null) !== false) {
        throw new RuntimeException('meta.json não confirma general_score=false.');
    }

    $entries = v2_manifest_entries($meta);
    $requiredContracts = (array)($config['required_contracts'] ?? []);
    foreach ($requiredContracts as $relative => $contract) {
        if (!isset($entries[$relative])) {
            throw new RuntimeException("Artefato obrigatório fora do manifesto: {$relative}");
        }
    }

    $decoded = [];
    foreach ($entries as $relative => $entry) {
        $path = $releaseRoot . '/' . $relative;
        if (!is_file($path)) {
            throw new RuntimeException("Artefato declarado ausente: {$relative}");
        }
        $size = filesize($path);
        if ($size === false || $size <= 0) {
            throw new RuntimeException("Artefato vazio: {$relative}");
        }
        if (!empty($config['validation']['require_size_match']) && $size !== $entry['size_bytes']) {
            throw new RuntimeException("Tamanho divergente em {$relative}: esperado {$entry['size_bytes']}, obtido {$size}");
        }
        if (!empty($config['validation']['require_hash_match'])) {
            $hash = hash_file('sha256', $path);
            if (!is_string($hash) || !hash_equals($entry['sha256'], $hash)) {
                throw new RuntimeException("SHA-256 divergente em {$relative}");
            }
        }
        $raw = file_get_contents($path);
        if ($raw === false) {
            throw new RuntimeException("Falha ao ler {$relative}");
        }
        $payload = v2_decode_json($raw, $relative);
        $decoded[$relative] = $payload;
        if (isset($requiredContracts[$relative])) {
            $actual = v2_payload_contract($payload);
            if ($actual !== $requiredContracts[$relative]) {
                throw new RuntimeException("Contrato divergente em {$relative}: esperado {$requiredContracts[$relative]}, obtido " . var_export($actual, true));
            }
        }
    }

    if (!empty($config['validation']['reject_undeclared_json'])) {
        $expected = array_keys($entries);
        $expected[] = 'global/meta.json';
        sort($expected);
        $physical = v2_list_json_files($releaseRoot);
        if ($physical !== $expected) {
            $extra = array_values(array_diff($physical, $expected));
            $missing = array_values(array_diff($expected, $physical));
            throw new RuntimeException('Inventário físico diverge do manifesto. extras=' . json_encode($extra) . ' missing=' . json_encode($missing));
        }
    }

    if (!isset($decoded['global/administradoras.json'])) {
        throw new RuntimeException('administradoras.json não validado.');
    }
    v2_validate_admin_semantics($decoded['global/administradoras.json'], $config);

    foreach ((array)($meta['counts'] ?? []) as $key => $expectedCount) {
        $relative = 'global/' . $key . '.json';
        if (!isset($decoded[$relative]) || !is_int($expectedCount)) {
            continue;
        }
        $items = $decoded[$relative]['items'] ?? null;
        if (is_array($items) && count($items) !== $expectedCount) {
            throw new RuntimeException("Contagem divergente em {$relative}: meta={$expectedCount}, arquivo=" . count($items));
        }
    }

    return [
        'meta' => $meta,
        'manifest_sha256' => hash('sha256', $metaRaw),
        'validated_artifacts' => count($entries),
        'files' => array_keys($entries),
    ];
}

function v2_acquire_lock(string $path)
{
    v2_ensure_dirs([dirname($path)]);
    $handle = fopen($path, 'c+');
    if ($handle === false) {
        throw new RuntimeException("Falha ao abrir lock: {$path}");
    }
    if (!flock($handle, LOCK_EX | LOCK_NB)) {
        fclose($handle);
        return null;
    }
    ftruncate($handle, 0);
    fwrite($handle, (string)getmypid());
    fflush($handle);
    return $handle;
}

function v2_release_lock($handle): void
{
    if (is_resource($handle)) {
        @flock($handle, LOCK_UN);
        @fclose($handle);
    }
}

function v2_resolve_current_root(string $current): string
{
    if (!is_link($current)) {
        throw new RuntimeException("current-v2 deve ser symlink: {$current}");
    }
    $target = readlink($current);
    if ($target === false || $target === '') {
        throw new RuntimeException("Falha ao ler symlink: {$current}");
    }
    if ($target[0] !== '/') {
        $target = dirname($current) . '/' . $target;
    }
    $real = realpath($target);
    if ($real === false || !is_dir($real)) {
        throw new RuntimeException("Target de current-v2 inválido: {$target}");
    }
    return $real;
}

function v2_atomic_symlink_swap(string $link, string $target): void
{
    $realTarget = realpath($target);
    if ($realTarget === false || !is_dir($realTarget)) {
        throw new RuntimeException("Release alvo inválida: {$target}");
    }
    if (file_exists($link) && !is_link($link)) {
        throw new RuntimeException("Recusa substituir current que não seja symlink: {$link}");
    }
    v2_ensure_dirs([dirname($link)]);
    $tmp = $link . '.next.' . getmypid() . '.' . bin2hex(random_bytes(4));
    if (!symlink($realTarget, $tmp)) {
        throw new RuntimeException("Falha ao criar symlink temporário: {$tmp}");
    }
    if (!@rename($tmp, $link)) {
        @unlink($tmp);
        throw new RuntimeException("Falha no swap atômico de {$link}");
    }
}

function v2_validation_payload(string $result, ?string $releaseRoot, ?string $manifestSha, array $config, array $errors = []): array
{
    return [
        'validated_at' => gmdate('c'),
        'result' => $result,
        'release_id' => $releaseRoot ? basename($releaseRoot) : null,
        'release_root' => $releaseRoot,
        'manifest_sha256' => $manifestSha,
        'validator_version' => (string)($config['project']['validator_version'] ?? 'unknown'),
        'errors' => array_values($errors),
    ];
}

function v2_record_validation(array $config, array $payload): void
{
    $state = rtrim((string)$config['paths']['state'], '/');
    v2_atomic_write_json($state . '/last_validation_attempt.json', $payload);
    if (($payload['result'] ?? null) === 'success') {
        v2_atomic_write_json($state . '/last_validation_success.json', $payload);
    }
}

function v2_log(array $config, string $channel, string $message, array $context = []): void
{
    $dir = rtrim((string)$config['paths']['logs'], '/');
    v2_ensure_dirs([$dir]);
    $line = '[' . gmdate('c') . '] ' . strtoupper($channel) . ' ' . $message;
    if ($context) {
        $line .= ' ' . json_encode($context, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }
    $line .= PHP_EOL;
    file_put_contents($dir . '/' . $channel . '.log', $line, FILE_APPEND | LOCK_EX);
}

function v2_fetch(string $url, array $source): string
{
    $timeout = (int)($source['timeout_seconds'] ?? 60);
    $retry = max(1, (int)($source['retry_count'] ?? 3));
    $userAgent = (string)($source['user_agent'] ?? 'sanida-consorcio-v2');
    $last = 'unknown';
    for ($attempt = 1; $attempt <= $retry; $attempt++) {
        $ch = curl_init($url);
        if ($ch === false) {
            throw new RuntimeException('Falha ao inicializar cURL.');
        }
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_CONNECTTIMEOUT => min(15, $timeout),
            CURLOPT_TIMEOUT => $timeout,
            CURLOPT_USERAGENT => $userAgent,
            CURLOPT_HTTPHEADER => ['Accept: application/vnd.github+json'],
        ]);
        $body = curl_exec($ch);
        $error = curl_error($ch);
        $status = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);
        if (is_string($body) && $status >= 200 && $status < 300) {
            return $body;
        }
        $last = "HTTP {$status} {$error}";
        if ($attempt < $retry) {
            usleep(250000 * $attempt);
        }
    }
    throw new RuntimeException("Falha ao baixar {$url}: {$last}");
}

function v2_resolve_source_commit(array $config): string
{
    $source = $config['source'];
    $repo = rawurlencode((string)$source['repository']);
    $repo = str_replace('%2F', '/', $repo);
    $ref = rawurlencode((string)$source['ref']);
    $url = rtrim((string)$source['github_api_base'], '/') . '/repos/' . $repo . '/commits/' . $ref;
    $payload = v2_decode_json(v2_fetch($url, $source), $url);
    $sha = $payload['sha'] ?? null;
    if (!is_string($sha) || preg_match('/^[a-f0-9]{40}$/', $sha) !== 1) {
        throw new RuntimeException('GitHub não retornou commit SHA válido para a ref.');
    }
    return $sha;
}

function v2_raw_url(array $config, string $commitSha, string $relativePath): string
{
    $source = $config['source'];
    return rtrim((string)$source['raw_base'], '/') . '/' . (string)$source['repository'] . '/' . $commitSha . '/' . ltrim($relativePath, '/');
}

function v2_recursive_delete(string $path): void
{
    if (is_link($path) || is_file($path)) {
        @unlink($path);
        return;
    }
    if (!is_dir($path)) {
        return;
    }
    $iterator = new RecursiveIteratorIterator(
        new RecursiveDirectoryIterator($path, FilesystemIterator::SKIP_DOTS),
        RecursiveIteratorIterator::CHILD_FIRST
    );
    foreach ($iterator as $item) {
        if ($item->isDir() && !$item->isLink()) {
            @rmdir($item->getPathname());
        } else {
            @unlink($item->getPathname());
        }
    }
    @rmdir($path);
}

function v2_prune_releases(array $config, string $currentRoot): void
{
    $dir = rtrim((string)$config['paths']['releases'], '/');
    $keep = max(2, (int)($config['retention']['keep_releases'] ?? 10));
    if (!is_dir($dir)) {
        return;
    }
    $rows = [];
    foreach (glob($dir . '/*', GLOB_ONLYDIR) ?: [] as $path) {
        $rows[] = ['path' => $path, 'mtime' => filemtime($path) ?: 0];
    }
    usort($rows, fn(array $a, array $b): int => $b['mtime'] <=> $a['mtime']);
    $kept = 0;
    foreach ($rows as $row) {
        if (realpath($row['path']) === realpath($currentRoot)) {
            continue;
        }
        $kept++;
        if ($kept >= $keep) {
            v2_recursive_delete($row['path']);
        }
    }
}
