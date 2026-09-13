<?php
declare(strict_types=1);

$baseDir = getenv('CONSORCIO_DATA_BASE');
if (!is_string($baseDir) || $baseDir === '') {
    // Quando instalado como <base>/bin-v2/consorcio-v2-config.php.
    $baseDir = dirname(__DIR__);
}
$baseDir = rtrim($baseDir, '/');

return [
    'version' => '2.0.0',
    'project' => [
        'name' => 'comparador-consorcios-data',
        'timezone' => 'America/Sao_Paulo',
        'validator_version' => '2.0.0',
    ],
    'source' => [
        'repository' => 'Rafael-Tinelli/comparador-consorcios-data',
        'ref' => 'main',
        'github_api_base' => 'https://api.github.com',
        'raw_base' => 'https://raw.githubusercontent.com',
        'manifest_path' => 'data/dist/global/meta.json',
        'dist_prefix' => 'data/dist',
        'user_agent' => 'sanida-consorcio-v2-publisher/2.0',
        'timeout_seconds' => 60,
        'retry_count' => 3,
    ],
    'paths' => [
        'base' => $baseDir,
        'releases' => $baseDir . '/releases-v2',
        'current' => $baseDir . '/current-v2',
        'tmp' => $baseDir . '/_tmp-v2',
        'state' => $baseDir . '/state-v2',
        'logs' => $baseDir . '/logs-v2',
        'locks' => $baseDir . '/locks-v2',
        'lock' => $baseDir . '/locks-v2/publication.lock',
    ],
    'required_contracts' => [
        'global/instituicoes.json' => 'instituicoes.v2',
        'global/administradoras.json' => 'administradoras.v2',
        'global/produtos.json' => 'produtos.v2',
        'global/rankings.json' => 'rankings.v2',
        'global/segmentos.json' => 'segmentos.v2',
        'global/comparacoes.json' => 'comparacoes.v2',
        'global/ofertas.json' => 'ofertas.v2',
        'seo/defaults.json' => 'seo.defaults.v2',
        'seo/routes.json' => 'seo.routes.v2',
        'seo/site.json' => 'seo.site.v2',
    ],
    'validation' => [
        'minimum_administradoras' => 100,
        'reject_undeclared_json' => true,
        'require_hash_match' => true,
        'require_size_match' => true,
    ],
    'retention' => [
        'keep_releases' => 10,
    ],
];
