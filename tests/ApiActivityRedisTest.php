<?php

use Cbox\Signals\ApiActivity\ApiActivitySettings;
use Cbox\Signals\ApiActivity\ApiActor;
use Cbox\Signals\ApiActivity\ApiContext;
use Cbox\Signals\ApiActivity\RedisApiActivityLog;
use Cbox\Signals\Contracts\SourceRecords;
use Cbox\Signals\Data\SourceReceipt;
use Cbox\Signals\HttpSourceRecords;
use Cbox\Signals\Support\JsonTransport;
use Illuminate\Http\Client\Factory;
use Illuminate\Http\Client\Request as HttpRequest;
use Illuminate\Support\Facades\Redis;
use Orchestra\Testbench\TestCase;

uses(TestCase::class);

// Counting runs in Redis, never in the database. These tests need a real Redis:
// set SIGNALS_TEST_REDIS=redis://host:6379/15 (the database is flushed).

beforeEach(function (): void {
    $dsn = getenv('SIGNALS_TEST_REDIS');
    if (! is_string($dsn) || $dsn === '') {
        $this->markTestSkipped('SIGNALS_TEST_REDIS is not set.');
    }
    $url = parse_url($dsn);
    config()->set('database.redis.client', 'phpredis');
    // The log sends raw commands under its own prefix, so read Redis here without Laravel's.
    config()->set('database.redis.options.prefix', '');
    config()->set('database.redis.default', ['host' => $url['host'] ?? '127.0.0.1', 'port' => $url['port'] ?? 6379, 'database' => (int) ltrim($url['path'] ?? '/15', '/')]);
    Redis::connection()->flushdb();
});

function redisSettings(int $maxActors = 500, int $tracked = 2000, int $shards = 4): ApiActivitySettings
{
    return new ApiActivitySettings(true, 'secret-reference-key', 'key-1', 'status-api', 'cbox.id', 600, 120, $maxActors,
        ['project_id' => 'project', 'environment_id' => 'production', 'audience' => 'environment'], 'status-api-activity', '0', 'g1',
        shards: $shards, trackedActors: $tracked);
}

/** A source that keeps what it accepted, and answers with the queued failures first. */
function memorySource(array $failures = []): object
{
    return new class($failures) implements SourceRecords
    {
        public array $pages = [];

        public function __construct(public array $failures) {}

        public function send(string $source, array $records): SourceReceipt
        {
            $failure = array_shift($this->failures);
            if ($failure !== null) {
                throw $failure;
            }
            $this->pages[] = $records;

            return new SourceReceipt(count($records), 0, new DateTimeImmutable);
        }

        public function records(): array
        {
            return array_merge(...($this->pages ?: [[]]));
        }
    };
}

function answered(array $records): int
{
    $sum = 0;
    foreach ($records as $record) {
        foreach ($record['data']['operations'] as $o) {
            $sum += $o['responses']['success'] + $o['responses']['redirect'] + $o['responses']['client_error'] + $o['responses']['server_error'];
        }
        $sum += $record['data']['omitted_requests'];
    }

    return $sum;
}

/** @return array{0:resource,1:array<int,resource>} */
function redisWorker(string ...$arguments): array
{
    $process = proc_open([PHP_BINARY, __DIR__.'/Integration/api-activity-redis.php', ...$arguments], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);

    return [$process, $pipes];
}

function redisFinish(array $worker): string
{
    [$process, $pipes] = $worker;
    $output = stream_get_contents($pipes[1]).stream_get_contents($pipes[2]);
    expect(proc_close($process))->toBe(0, $output);

    return trim($output);
}

it('aggregates calls in Redis into the same records the builder makes, and closes only after the grace', function (): void {
    $s = redisSettings();
    $log = new RedisApiActivityLog($this->app->make('redis'), $s);
    $actor = new ApiActor(user: 'u1', organization: 'o1', token: 't1', ip: '192.0.2.42');
    $log->record(call($s, 1000, '/v1/components', 200, [], $actor));
    foreach ([['a', 200], ['b', 200], ['a', 200], ['c', 404]] as $i => [$id, $status]) {
        $log->record(call($s, 2000 + $i, '/v1/incidents/{incident}', $status, ['incident' => $id], $actor));
    }
    $log->record(call($s, 3000, '/v1/incidents', 201, [], $actor, 'POST'));
    $log->record(call($s, 1000, '/v1/next', 200, [], $actor, 'GET', WINDOW_START + 600));

    $source = memorySource();
    expect($log->flush($source, WINDOW_START + 600 + 119))->toBe(['parts' => 0, 'records' => 0]);
    expect($log->flush($source, WINDOW_START + 720))->toBe(['parts' => 1, 'records' => 1]);
    $record = $source->records()[0];
    expect(apiActivityErrors($record))->toBe([]);
    $data = $record['data'];
    expect(array_column($data['operations'], 'route'))->toBe(['/v1/components', '/v1/incidents/{incident}', '/v1/incidents'])
        ->and($data['operations'][1]['responses'])->toMatchArray(['success' => 3, 'client_error' => 1, 'not_found' => 1])
        ->and($data['operations'][1]['parameters']['references'])->toHaveCount(2)
        ->and(array_map(fn ($r) => [$r['operation'], $r['repeat']], $data['sequence']))->toBe([[0, 1], [1, 4], [2, 1]])
        ->and($record['position']['sequence'])->toBe('1')
        ->and(json_encode($record))->not->toContain('192.0.2.42');
    // Only the part counter of the closed window is left (it expires); its counts are gone.
    expect(array_values(array_filter(Redis::connection()->keys('*{'.WINDOW_START.':*'), fn ($k) => ! str_ends_with($k, '}:m'))))->toBe([]);
});

it('counts a call that arrives after its window closed as a later record, and never twice', function (): void {
    $s = redisSettings();
    $log = new RedisApiActivityLog($this->app->make('redis'), $s);
    $log->record(call($s, 1000, '/v1/a'));
    $source = memorySource();
    $log->flush($source, WINDOW_START + 720);
    $log->record(call($s, 2000, '/v1/a')); // a slow server
    $log->flush($source, WINDOW_START + 800);
    $log->flush($source, WINDOW_START + 900);
    $records = $source->records();
    expect($records)->toHaveCount(2)->and(answered($records))->toBe(2)
        ->and($records[0]['id'])->not->toBe($records[1]['id'])
        ->and(array_column(array_column($records, 'position'), 'sequence'))->toBe(['1', '2']);
});

it('retries a page with the same records and positions, and keeps only a refused record aside', function (): void {
    $s = redisSettings(shards: 1);
    $log = new RedisApiActivityLog($this->app->make('redis'), $s);
    foreach (['u1', 'u2', 'u3'] as $i => $user) {
        $log->record(call($s, 1000 + $i, '/v1/a', 200, [], new ApiActor(user: $user)));
    }
    $http = new Factory;
    $pages = [];
    $replies = [
        $http->response(['code' => 'project_backpressure'], 503),
        $http->response(['code' => 'invalid_canonical_record'], 422),
        $http->response(['accepted' => 1, 'duplicates' => 0, 'accepted_at' => '2026-11-05T10:12:00Z']),
        $http->response(['code' => 'invalid_canonical_record'], 422),
        $http->response(['accepted' => 1, 'duplicates' => 0, 'accepted_at' => '2026-11-05T10:12:00Z']),
    ];
    $http->fake(function (HttpRequest $request) use (&$pages, &$replies) {
        $pages[] = [$request->url(), $request['records']];

        return array_shift($replies);
    });
    $source = new HttpSourceRecords(new JsonTransport($http, 'https://signals.test'), 'write-key');

    expect($log->flush($source, WINDOW_START + 720))->toBe(['parts' => 0, 'records' => 0]); // 503: wait
    expect($log->flush($source, WINDOW_START + 721))->toBe(['parts' => 0, 'records' => 0]); // not due yet
    expect($pages)->toHaveCount(1);
    expect($log->flush($source, WINDOW_START + 800))->toBe(['parts' => 1, 'records' => 2]);
    expect($pages[0][0])->toBe('https://signals.test/v2/sources/status-api-activity/records:batch')
        ->and($pages[1][1])->toBe($pages[0][1]) // identical bytes on retry: same ids, positions and recorded_at
        ->and(array_map(fn ($p) => array_column(array_column($p[1], 'position'), 'sequence'), $pages))->toBe([['1', '2', '3'], ['1', '2', '3'], ['1'], ['2'], ['3']]);
    $failed = Redis::connection()->lrange('signals:api:failed', 0, -1);
    expect($failed)->toHaveCount(1)->and(json_decode($failed[0], true)['record']['position']['sequence'])->toBe('2');
});

it('stays bounded in memory when an attack comes from 20000 addresses', function (): void {
    $s = redisSettings(maxActors: 100, tracked: 400);
    $log = new RedisApiActivityLog($this->app->make('redis'), $s);
    $before = (int) Redis::connection()->info('memory')['used_memory'];
    for ($i = 0; $i < 20000; $i++) {
        $actor = new ApiActor(ip: '10.'.intdiv($i, 65536).'.'.intdiv($i % 65536, 256).'.'.($i % 256), context: new ApiContext(asn: 64500 + $i % 200, networkClass: 'hosting', enrichmentVersion: 'geo-1'));
        $log->record(call($s, $i * 20, '/v1/incidents/{incident}', 404, ['incident' => (string) $i], $actor));
    }
    $used = (int) Redis::connection()->info('memory')['used_memory'] - $before;
    $source = memorySource();
    $log->flush($source, WINDOW_START + 720);
    $records = $source->records();
    foreach ($records as $record) {
        expect(apiActivityErrors($record))->toBe([], $record['id']);
    }
    $remainders = array_filter($records, fn ($r) => $r['data']['grouping'] === 'remainder');
    expect(answered($records))->toBe(20000)
        ->and(count($records))->toBeLessThanOrEqual(100 + 4 * 16)
        // HyperLogLog counts: about 19600 folded actors, within 2 %.
        ->and(abs(array_sum(array_map(fn ($r) => $r['data']['actors'], $remainders)) - 19600))->toBeLessThan(400)
        // 400 tracked actors and 64 networks per shard, not 20000 actors.
        ->and($used)->toBeLessThan(4 * 1024 * 1024);
});

it('counts every call exactly once while four servers write and four schedulers flush at once', function (): void {
    $file = tempnam(sys_get_temp_dir(), 'api-activity');
    $workers = [];
    foreach (range(1, 4) as $i) {
        $workers[] = redisWorker('write', (string) $i, '2500');
        $workers[] = redisWorker('flush', '40', $file);
    }
    $outputs = array_map(redisFinish(...), $workers);
    redisFinish(redisWorker('flush', '1', $file)); // what arrived after the last round

    $records = array_map(fn (string $line): array => json_decode($line, true), array_filter(explode("\n", file_get_contents($file))));
    unlink($file);
    $ids = array_column($records, 'id');
    $sequences = array_map(intval(...), array_column(array_column($records, 'position'), 'sequence'));
    sort($sequences);
    expect(answered($records))->toBe(10000)
        ->and(array_unique($ids))->toHaveCount(count($ids))
        ->and($sequences)->toBe(range(1, count($records)))
        ->and(array_values(array_filter(Redis::connection()->keys('signals:api:{*'), fn ($k) => ! str_ends_with($k, '}:m'))))->toBe([]);
    foreach ($records as $record) {
        expect(apiActivityErrors($record))->toBe([], $record['id']);
    }
    foreach (array_filter($outputs, fn ($o) => str_starts_with($o, '{')) as $line) {
        expect(json_decode($line, true)['p99_ms'])->toBeLessThan(5);
    }
});
