<?php

use Cbox\Signals\ApiActivity\ApiActivitySettings;
use Cbox\Signals\ApiActivity\ApiActor;
use Cbox\Signals\ApiActivity\ApiCall;
use Cbox\Signals\McpActivity\Catalog;
use Cbox\Signals\McpActivity\Operation;
use Cbox\Signals\McpActivity\Settings;
use Opis\JsonSchema\Errors\ErrorFormatter;
use Opis\JsonSchema\Validator;

/** The 2.6.0 schema and the cross-field rules of cbox-signals bin/p0/validate-contracts.py (api_activity). */
function apiActivityErrors(array $record): array
{
    $wire = json_decode(json_encode($record, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), flags: JSON_THROW_ON_ERROR);
    $schema = json_decode(file_get_contents(__DIR__.'/Contracts/v2.6/event.schema.json'), flags: JSON_THROW_ON_ERROR);
    $validator = new Validator;
    $validator->setMaxErrors(10);
    $result = $validator->validate($wire, $schema);
    if (! $result->isValid()) {
        return ['schema: '.json_encode((new ErrorFormatter)->format($result->error()))];
    }
    $errors = [];
    $p = $record['data'];
    $start = strtotime($p['window_start']);
    $seconds = strtotime($p['window_end']) - $start;
    if ($seconds < 60 || $seconds > 3600 || $start % $seconds !== 0) {
        return ['window'];
    }
    if ($record['occurred_at'] !== $p['window_end']) {
        $errors[] = 'occurs at window end';
    }
    $requests = [];
    $seen = [];
    $references = 0;
    foreach ($p['operations'] as $o) {
        $r = $o['responses'];
        $answered = $r['success'] + $r['redirect'];
        $requests[] = $answered + $r['client_error'] + $r['server_error'];
        $key = $o['method'].' '.$o['route'];
        if (isset($seen[$key])) {
            $errors[] = 'operation listed twice';
        }
        $seen[$key] = true;
        if (end($requests) === 0) {
            $errors[] = 'operation without requests';
        }
        if ($r['client_error'] < $r['denied'] + $r['not_found'] + $r['rate_limited']) {
            $errors[] = 'sub-classes exceed client errors';
        }
        $q = $o['parameters'];
        if ($q === null) {
            continue;
        }
        $refs = $q['references'];
        $references += count($refs);
        for ($i = 1; $i < count($refs); $i++) {
            if (strcmp($refs[$i - 1], $refs[$i]) >= 0) {
                $errors[] = 'references not sorted and unique';
            }
        }
        if (count($refs) + $q['overflow'] > $answered) {
            $errors[] = 'more parameter sets than answered requests';
        }
        if ($q['conflicts'] > $answered) {
            $errors[] = 'more conflicts than answered requests';
        }
    }
    if ($references > 768) {
        $errors[] = 'too many references in one window';
    }
    $listed = array_fill(0, count($p['operations']), 0);
    $previous = null;
    foreach ($p['sequence'] as $s) {
        if ($s['operation'] >= count($p['operations'])) {
            $errors[] = 'unlisted operation in sequence';

            continue;
        }
        if ($previous !== null && $s['operation'] === $previous['operation']) {
            $errors[] = 'neighbouring runs of one operation';
        }
        if ($previous !== null && $s['offset_ms'] < $previous['offset_ms']) {
            $errors[] = 'sequence out of order';
        }
        if ($s['offset_ms'] >= $seconds * 1000) {
            $errors[] = 'sequence outside window';
        }
        $listed[$s['operation']] += $s['repeat'];
        $previous = $s;
    }
    foreach ($listed as $i => $n) {
        if ($n > $requests[$i]) {
            $errors[] = 'sequence has more calls than the operation';
        }
    }
    $c = $p['context'];
    if (($c['device_id'] === null) !== ($c['device_evidence'] === 'missing')) {
        $errors[] = 'device evidence mismatch';
    }

    return $errors;
}

/** The 2.0.0 coverage schema and the cross-field rules of signalsd internal/canonical/validate.go (coverage). */
function coverageErrors(array $frame): array
{
    $wire = json_decode(json_encode($frame, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), flags: JSON_THROW_ON_ERROR);
    $schema = json_decode(file_get_contents(__DIR__.'/Contracts/v2/coverage.schema.json'), flags: JSON_THROW_ON_ERROR);
    $result = (new Validator)->validate($wire, $schema);
    if (! $result->isValid()) {
        return ['schema: '.json_encode((new ErrorFormatter)->format($result->error()))];
    }
    $errors = [];
    $time = $frame['event_time'];
    if (strtotime($time['from']) > strtotime($time['through'])) {
        $errors[] = 'from after through';
    }
    if (($time['status'] === 'complete') !== ($time['reasons'] === [])) {
        $errors[] = 'complete exactly when there are no reasons';
    }
    if ((int) $frame['commit_through'] > (int) $frame['position']['sequence']) {
        $errors[] = 'commit beyond the position';
    }
    if (array_diff($frame['observed_families'], $frame['expected_families']) !== []) {
        $errors[] = 'observed family not expected';
    }

    return $errors;
}

const WINDOW_START = 1793872800; // 2026-11-05T10:00:00Z, a multiple of 600

function mcpSettings(int $capacity = 10000): Settings
{
    return new Settings(ApiActivitySettings::testing(), new Catalog('catalog-1', [new Operation('pages', 'listPages'), new Operation('pages', 'getPage', 'page-v1', ['page_id'])]), capacity: $capacity);
}

function settings(int $maxActors = 500): ApiActivitySettings
{
    return new ApiActivitySettings(true, 'secret-reference-key', 'key-1', 'status-api', 'cbox.id', 600, 120, $maxActors,
        ['project_id' => 'project', 'environment_id' => 'production', 'audience' => 'environment'], 'status-api-activity', '0', 'g1');
}

function call(ApiActivitySettings $settings, int $offsetMs, string $route, int $status = 200, array $parameters = [], ?ApiActor $actor = null, string $method = 'GET', int $start = WINDOW_START): ApiCall
{
    return ApiCall::of($settings, $actor ?? new ApiActor(user: 'u1', organization: 'o1', token: 't1', ip: '192.0.2.42'), $start * 1000 + $offsetMs, $method, $route, $status, $parameters, false);
}
