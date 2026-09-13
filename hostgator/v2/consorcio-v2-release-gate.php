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

        $requiredSources = $config['validation']['required_source_status'] ?? [];
        if (!is_array($requiredSources) || $requiredSources === []) {
            throw new RuntimeException('Configuração sem required_source_status.');
        }
        foreach ($requiredSources as $requiredSource) {
            if (!is_string($requiredSource) || $requiredSource === '') {
                throw new RuntimeException('required_source_status contém identificador inválido.');
            }
            if (!array_key_exists($requiredSource, $sourceStatus) || !is_array($sourceStatus[$requiredSource])) {
                throw new RuntimeException("source_status obrigatório ausente: {$requiredSource}.");
            }
        }

        $expectedDegraded = [];
        foreach ($sourceStatus as $source => $state) {
            if (!is_string($source) || $source === '' || !is_array($state)) {
                throw new RuntimeException('Entrada inválida em source_status.');
            }
            $checkStatus = $state['last_check_status'] ?? null;
            if (!in_array($checkStatus, ['success', 'failure'], true)) {
                throw new RuntimeException("source_status.{$source}.last_check_status inválido.");
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
            if ($checkStatus !== 'success' && in_array($source, $requiredSources, true)) {
                $expectedDegraded[] = $source;
            }
        }

        $freshness = $meta['freshness'] ?? null;
        if (!is_array($freshness) || ($freshness['all_required_states_present'] ?? null) !== true) {
            throw new RuntimeException('meta.json não confirma all_required_states_present=true.');
        }
        if (!empty($config['validation']['require_source_state_matches_consumed_bytes'])
            && ($freshness['source_state_matches_consumed_bytes'] ?? null) !== true) {
            throw new RuntimeException('meta.json não confirma source_state_matches_consumed_bytes=true.');
        }
        $degraded = $freshness['degraded_sources'] ?? null;
        if (!is_array($degraded)) {
            throw new RuntimeException('freshness.degraded_sources inválido.');
        }
        sort($expectedDegraded, SORT_STRING);
        $actualDegraded = array_values(array_filter($degraded, 'is_string'));
        sort($actualDegraded, SORT_STRING);
        if ($actualDegraded !== $expectedDegraded) {
            throw new RuntimeException('freshness.degraded_sources diverge do status das fontes obrigatórias.');
        }

        $monthlyPeriod = $meta['source_periods']['consorciobd_mensal'] ?? null;
        $monthlyCompetence = $sourceStatus['bc_consorciobd']['competence']['value'] ?? null;
        if ($monthlyPeriod !== null && $monthlyCompetence !== $monthlyPeriod) {
            throw new RuntimeException('Competência ConsorcioBD diverge entre source_status e source_periods.');
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
