<?php
declare(strict_types=1);

function v2_validate_backend_release_meta(array $meta, array $config): void
{
    $requiredReleaseContract = $config['validation']['require_backend_release_contract'] ?? null;
    if (is_string($requiredReleaseContract) && $requiredReleaseContract !== '') {
        $backendRelease = $meta['backend_release'] ?? null;
        if (!is_array($backendRelease)) {
            throw new RuntimeException('meta.json sem backend_release.');
        }
        if (($backendRelease['contract'] ?? null) !== $requiredReleaseContract) {
            throw new RuntimeException('Contrato backend_release divergente.');
        }
        if (($backendRelease['publication_eligible'] ?? null) !== true) {
            throw new RuntimeException('Release V2 não está marcada como elegível para publicação.');
        }
        $fingerprint = $backendRelease['release_fingerprint'] ?? null;
        if (!is_string($fingerprint) || preg_match('/^[a-f0-9]{64}$/', $fingerprint) !== 1) {
            throw new RuntimeException('backend_release.release_fingerprint inválido.');
        }
    }

    if (!empty($config['validation']['require_source_status'])) {
        $sourceStatus = $meta['source_status'] ?? null;
        if (!is_array($sourceStatus) || $sourceStatus === []) {
            throw new RuntimeException('meta.json sem source_status.');
        }
        foreach ($sourceStatus as $source => $state) {
            if (!is_string($source) || $source === '' || !is_array($state)) {
                throw new RuntimeException('Entrada inválida em source_status.');
            }
            foreach (['last_checked_at', 'last_successful_check_at', 'content_sha256', 'competence'] as $field) {
                if (!array_key_exists($field, $state) || $state[$field] === null || $state[$field] === '') {
                    throw new RuntimeException("source_status.{$source}.{$field} ausente.");
                }
            }
            if (!is_array($state['competence']) || !array_key_exists('kind', $state['competence']) || !array_key_exists('value', $state['competence'])) {
                throw new RuntimeException("source_status.{$source}.competence inválida.");
            }
            $hash = $state['content_sha256'];
            if (!is_string($hash) || preg_match('/^[a-f0-9]{64}$/', $hash) !== 1) {
                throw new RuntimeException("source_status.{$source}.content_sha256 inválido.");
            }
        }

        $freshness = $meta['freshness'] ?? null;
        if (!is_array($freshness) || ($freshness['all_required_states_present'] ?? null) !== true) {
            throw new RuntimeException('meta.json não confirma all_required_states_present=true.');
        }
    }
}

function v2_validate_release_backend(string $releaseRoot, array $config): array
{
    $result = v2_validate_release($releaseRoot, $config);
    $meta = $result['meta'] ?? null;
    if (!is_array($meta)) {
        throw new RuntimeException('Validador não retornou meta.json decodificado.');
    }
    v2_validate_backend_release_meta($meta, $config);
    return $result;
}
