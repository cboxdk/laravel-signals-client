<?php

use Cbox\Signals\Data\Entity;
use Cbox\Signals\Data\EvaluationRequest;
use Cbox\Signals\Data\Event;
use Cbox\Signals\Data\EventBatch;
use Cbox\Signals\Data\MetricBatch;
use Cbox\Signals\Data\MetricKind;
use Cbox\Signals\Data\MetricSample;
use Cbox\Signals\Data\ReplayRequest;
use Cbox\Signals\Exceptions\SignalsFailure;
use Cbox\Signals\HttpSignals;
use Cbox\Signals\Testing\FakeSignals;
use Illuminate\Http\Client\Factory;

it('transmits stable occurrence and batch identities', function (): void {
    $http = new Factory;
    $http->fake(['https://signals.test/*' => Factory::response(['operation_id' => 'op_1', 'accepted_count' => 1, 'accepted_at' => '2026-11-04T12:00:00Z'], 202)]);
    $client = new HttpSignals($http, 'https://signals.test', 'test-only-key');
    $receipt = $client->events(new EventBatch('customer', [new Event('event_1', 'document.checked', 1, new DateTimeImmutable('2026-11-04T12:00:00Z'), ['allowed' => false])]), 'batch_1');
    expect($receipt->acceptedCount)->toBe(1);
    $http->assertSent(fn ($request) => $request->hasHeader('Idempotency-Key', 'batch_1') && $request['events'][0]['id'] === 'event_1');
});
it('keeps unavailable and insufficient coverage distinct', function (): void {
    $http = new Factory;
    $http->fake(['*' => Factory::response(['evaluation_id' => 'ev_1', 'status' => 'insufficient_data', 'signals' => [], 'coverage' => ['identity' => 'warming_up'], 'data_as_of' => null, 'config_version' => 1])]);
    $client = new HttpSignals($http, 'https://signals.test', 'test-only-key');
    expect($client->evaluate(new EvaluationRequest('attempt_1', new Entity('customer', 'user', 'one')))->status->value)->toBe('insufficient_data');
    $http = new Factory;
    $http->fake(['*' => Factory::response(['code' => 'quota_exhausted'], 429)]);
    $client = new HttpSignals($http, 'https://signals.test', 'test-only-key');
    expect(fn () => $client->evaluate(new EvaluationRequest('attempt_2', new Entity('customer', 'user', 'one'))))->toThrow(SignalsFailure::class);
});
it('refuses malformed success and missing configuration', function (): void {
    $http = new Factory;
    $http->fake(['*' => Factory::response(['ok' => true])]);
    expect(fn () => (new HttpSignals($http, 'https://signals.test', 'key'))->evaluate(new EvaluationRequest('one', new Entity('x', 'user', 'one'))))->toThrow(SignalsFailure::class);
    expect(fn () => (new HttpSignals($http, '', ''))->evaluate(new EvaluationRequest('one', new Entity('x', 'user', 'one'))))->toThrow(SignalsFailure::class);
});

it('uses durable metric and replay contracts with stable request keys', function (): void {
    $http = new Factory;
    $http->fake(['*' => Factory::response(['operation_id' => 'op_metric', 'accepted_count' => 1, 'accepted_at' => '2026-11-04T12:00:00Z'], 202)]);
    $client = new HttpSignals($http, 'https://signals.test', 'test-only-key');
    $at = new DateTimeImmutable('2026-11-04T12:00:00.123456Z');
    $sample = new MetricSample('memory-1', 'runtime.memory', 128, 'bytes', MetricKind::Gauge, $at, new Entity('shop', 'instance', 'worker'));
    expect($client->metrics(new MetricBatch('runtime', [$sample]), 'metrics-stable')->operationId)->toBe('op_metric');
    $http->assertSent(fn ($request) => $request->url() === 'https://signals.test/v1/metrics:batch' && $request->hasHeader('Idempotency-Key', 'metrics-stable') && str_contains($request->body(), '"dimensions":{}') && str_contains($request->body(), '123456'));
    $client->replay(new ReplayRequest($at->modify('-1 hour'), $at, 1, 1, 'memory'), 'replay-stable');
    $http->assertSent(fn ($request) => $request->url() === 'https://signals.test/v1/replays' && $request->hasHeader('Idempotency-Key', 'replay-stable') && $request['configuration_revision'] === 1);
});

it('never invents successful results in its test fake', function (): void {
    $fake = new FakeSignals;
    $request = new EvaluationRequest('attempt', new Entity('shop', 'user', 'person'));
    expect(fn () => $fake->evaluate($request))->toThrow(LogicException::class);
    $fake->queueEvaluation(new SignalsFailure('quota_exhausted', 429));
    expect(fn () => $fake->evaluate($request))->toThrow(SignalsFailure::class);
    expect($fake->evaluationCalls)->toHaveCount(2);
});
