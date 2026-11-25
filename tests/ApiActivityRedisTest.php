<?php

use Cbox\Signals\ApiActivity\ApiActivitySettings;
use Cbox\Signals\ApiActivity\ApiActor;
use Cbox\Signals\ApiActivity\ApiContext;
use Cbox\Signals\ApiActivity\RedisApiActivityLog;
use Cbox\Signals\Contracts\SourceRecords;
use Cbox\Signals\Data\SourceReceipt;
use Cbox\Signals\Exceptions\SignalsFailure;
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

function redisSettings(int $maxActors = 500, int $tracked = 2000, int $shards = 4, int $grace = 60): ApiActivitySettings
{
    return new ApiActivitySettings(true, 'secret-reference-key', 'key-1', 'status-api', 'cbox.id', 600, $grace, $maxActors,
        ['project_id' => 'project', 'environment_id' => 'production', 'audience' => 'environment'], 'status-api-activity', '0', 'g1',
        shards: $shards, trackedActors: $tracked);
}

/**
 * A source that keeps what it accepted, and answers with the queued failures first
 * (`$failures` for records, `$frameFailures` for coverage frames). It accepts a coverage
 * frame only under signalsd's rules (internal/store/canonical.go acceptCanonicalCoverage,
 * internal/canonical/validate.go), and tells which windows the accepted frames make
 * complete and whether the source reads fresh, as internal/shadow/coverage.go and
 * internal/store/canonical_status.go do. Set `now` to the time Signals receives a call.
 */
function memorySource(array $failures = [], array $frameFailures = []): object
{
    return new class($failures, $frameFailures) implements SourceRecords
    {
        public array $pages = [];

        /** Every frame sent, accepted or not. */
        public array $attempts = [];

        /** @var list<array{0:int,1:array}> accepted frames with the time each was received */
        public array $frames = [];

        public int $now = 0;

        /** The highest position accepted, as rawHighest reads it. */
        public int $highest = 0;

        /** @var array<int,int> when each window first became complete */
        public array $completedAt = [];

        public function __construct(public array $failures, public array $frameFailures) {}

        public function send(string $source, array $records): SourceReceipt
        {
            if ($records[0]['kind'] === 'coverage') {
                $this->attempts[] = $records[0];
                $failure = array_shift($this->frameFailures);
                if ($failure !== null) {
                    throw $failure;
                }
                $this->accept($records[0]);

                return new SourceReceipt(1, 0, new DateTimeImmutable);
            }
            $failure = array_shift($this->failures);
            if ($failure !== null) {
                throw $failure;
            }
            $this->pages[] = $records;
            foreach ($records as $record) {
                $this->highest = max($this->highest, (int) $record['position']['sequence']);
            }

            return new SourceReceipt(count($records), 0, new DateTimeImmutable);
        }

        private function accept(array $frame): void
        {
            expect(coverageErrors($frame))->toBe([]);
            $emitted = strtotime($frame['emitted_at']);
            $through = strtotime($frame['event_time']['through']);
            $commit = (int) $frame['commit_through'];
            expect($emitted)->toBeLessThanOrEqual($this->now + 30)->toBeGreaterThanOrEqual($this->now - 120)
                ->and($through)->toBeLessThanOrEqual($emitted + 30)
                ->and($frame['expected_families'])->toBe(['api.activity'])
                ->and($commit)->toBeLessThanOrEqual($this->highest);
            if ($frame['completeness'] === 'complete') {
                expect($commit)->toBe($this->highest, 'a complete frame commits through the highest accepted position');
            }
            if ($this->frames !== []) {
                expect($emitted)->toBeGreaterThan(strtotime(end($this->frames)[1]['emitted_at']));
            }
            $this->frames[] = [$this->now, $frame];
            foreach (array_keys($this->windows()) as $start) {
                $this->completedAt[$start] ??= $this->now;
            }
        }

        /** Windows of 600 s the frames received so far make complete, in the order Signals received them. */
        public function windows(): array
        {
            $spans = [];
            $complete = [];
            foreach ($this->frames as [, $frame]) {
                $from = strtotime($frame['event_time']['from']);
                $through = strtotime($frame['event_time']['through']);
                $cells = $through > $from ? range(intdiv($from, 600) * 600, intdiv($through - 1, 600) * 600, 600) : [];
                foreach ($cells as $start) {
                    if ($frame['completeness'] !== 'complete') {
                        unset($spans[$start], $complete[$start]); // withdrawn

                        continue;
                    }
                    $spans[$start][] = [max($from, $start), min($through, $start + 600)];
                    usort($spans[$start], fn ($a, $b) => $a[0] <=> $b[0]);
                    $cursor = $start;
                    foreach ($spans[$start] as [$a, $b]) {
                        if ($a > $cursor) {
                            break;
                        }
                        $cursor = max($cursor, $b);
                    }
                    if ($cursor >= $start + 600) {
                        $complete[$start] = true;
                    }
                }
            }

            return $complete;
        }

        public function complete(int $start): bool
        {
            return isset($this->windows()[$start]);
        }

        /** The source status at `$at`: complete, partial or stale. */
        public function status(int $at): string
        {
            [$received, $frame] = end($this->frames);
            $age = max($at - $received, $at - strtotime($frame['emitted_at']), $at - strtotime($frame['event_time']['through']), $frame['event_time']['lag_seconds']);

            return $age > min(120, 2 * $frame['event_time']['expected_cadence_seconds']) ? 'stale' : $frame['completeness'];
        }

        public function through(): string
        {
            return end($this->frames)[1]['event_time']['through'];
        }

        public function records(): array
        {
            return array_merge(...($this->pages ?: [[]]));
        }
    };
}

/** Run the scheduled command's flush at `$at`, with Signals receiving at the same time. */
function flushAt(RedisApiActivityLog $log, object $source, int $at): array
{
    $source->now = $at;

    return $log->flush($source, $at);
}

/** Run the scheduler every minute from `$from` through `$through`. */
function everyMinute(RedisApiActivityLog $log, object $source, int $from, int $through): void
{
    for ($at = $from; $at <= $through; $at += 60) {
        flushAt($log, $source, $at);
    }
}

function iso(int $at): string
{
    return gmdate('Y-m-d\\TH:i:s\\Z', $at);
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
    expect(flushAt($log, $source, WINDOW_START + 600 + 59))->toBe(['parts' => 0, 'records' => 0, 'coverage' => 'complete']);
    expect(flushAt($log, $source, WINDOW_START + 660))->toBe(['parts' => 1, 'records' => 1, 'coverage' => 'complete']);
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
    flushAt($log, $source, WINDOW_START + 720);
    $log->record(call($s, 2000, '/v1/a')); // a slow server
    flushAt($log, $source, WINDOW_START + 800);
    flushAt($log, $source, WINDOW_START + 900);
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
    $http->fake(function (HttpRequest $request) use ($http, &$pages, &$replies) {
        if ($request['records'][0]['kind'] === 'coverage') {
            return $http->response(['accepted' => 1, 'duplicates' => 0, 'accepted_at' => '2026-11-05T10:12:00Z']);
        }
        $pages[] = [$request->url(), $request['records']];

        return array_shift($replies);
    });
    $source = new HttpSourceRecords(new JsonTransport($http, 'https://signals.test'), 'write-key');

    expect($log->flush($source, WINDOW_START + 720))->toMatchArray(['parts' => 0, 'records' => 0]); // 503: wait
    expect($log->flush($source, WINDOW_START + 721))->toMatchArray(['parts' => 0, 'records' => 0]); // not due yet
    expect($pages)->toHaveCount(1);
    expect($log->flush($source, WINDOW_START + 800))->toMatchArray(['parts' => 1, 'records' => 2]);
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
    flushAt($log, $source, WINDOW_START + 720);
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

it('sends a complete coverage frame every minute while the API is idle, so idle windows become complete and the source stays fresh', function (): void {
    $log = new RedisApiActivityLog($this->app->make('redis'), redisSettings());
    $source = memorySource();
    for ($at = WINDOW_START + 2; $at <= WINDOW_START + 1262; $at += 60) {
        expect(flushAt($log, $source, $at)['coverage'])->toBe('complete')
            ->and($source->status($at + 58))->toBe('complete'); // still fresh just before the next run
    }
    $frames = array_column($source->frames, 1);
    expect($frames)->toHaveCount(22)
        // The first frame starts the claim: the window before it is never claimed.
        ->and($frames[0]['event_time'])->toMatchArray(['from' => iso(WINDOW_START - 1), 'through' => iso(WINDOW_START - 1), 'lag_seconds' => 3])
        ->and($frames[1]['event_time']['through'])->toBe(iso(WINDOW_START + 62))
        // In the window's grace `through` waits a second short of its end ...
        ->and($frames[10]['event_time']['through'])->toBe(iso(WINDOW_START + 599))
        // ... and passes it once the window is sealed with no calls.
        ->and($frames[11]['event_time'])->toMatchArray(['from' => iso(WINDOW_START - 1), 'through' => iso(WINDOW_START + 662), 'lag_seconds' => 0])
        ->and(array_unique(array_column($frames, 'commit_through')))->toBe(['0'])
        ->and(array_unique(array_column(array_column($frames, 'position'), 'sequence')))->toBe(['0'])
        ->and($source->completedAt)->toBe([WINDOW_START => WINDOW_START + 662, WINDOW_START + 600 => WINDOW_START + 1262]);
});

it('reads stale for a moment every window when the grace is longer than a minute', function (): void {
    $log = new RedisApiActivityLog($this->app->make('redis'), redisSettings(grace: 120));
    $source = memorySource();
    everyMinute($log, $source, WINDOW_START + 2, WINDOW_START + 662);
    // `through` cannot pass the window's end before its grace is over, at the run at +722.
    expect($source->through())->toBe(iso(WINDOW_START + 599))->and($source->status(WINDOW_START + 720))->toBe('stale');
});

it('claims a window with calls only after Signals accepted its records', function (): void {
    $s = redisSettings();
    $log = new RedisApiActivityLog($this->app->make('redis'), $s);
    $source = memorySource();
    everyMinute($log, $source, WINDOW_START + 2, WINDOW_START + 302);
    $log->record(call($s, 1000, '/v1/a'));
    $log->record(call($s, 400000, '/v1/b', 200, [], new ApiActor(user: 'u2')));
    everyMinute($log, $source, WINDOW_START + 362, WINDOW_START + 602);
    expect($source->records())->toBe([])->and($source->through())->toBe(iso(WINDOW_START + 599))->and($source->complete(WINDOW_START))->toBeFalse();

    expect(flushAt($log, $source, WINDOW_START + 662))->toBe(['parts' => 2, 'records' => 2, 'coverage' => 'complete']);
    $frame = end($source->frames)[1];
    expect($frame['commit_through'])->toBe('2')->and($frame['position']['sequence'])->toBe('2')
        ->and($frame['event_time']['through'])->toBe(iso(WINDOW_START + 662))
        ->and($source->completedAt[WINDOW_START])->toBe(WINDOW_START + 662)
        ->and($source->status(WINDOW_START + 720))->toBe('complete');
});

it('holds through while a window cannot be sent, so the source turns stale, and catches up when it is accepted', function (): void {
    $s = redisSettings();
    $log = new RedisApiActivityLog($this->app->make('redis'), $s);
    $unavailable = new SignalsFailure('unavailable', 503);
    $source = memorySource([$unavailable, $unavailable]);
    everyMinute($log, $source, WINDOW_START + 2, WINDOW_START + 302);
    $log->record(call($s, 1000, '/v1/a'));
    everyMinute($log, $source, WINDOW_START + 362, WINDOW_START + 722); // fails at +662 and +722

    expect($source->records())->toBe([])
        ->and($source->through())->toBe(iso(WINDOW_START + 599))
        ->and($source->complete(WINDOW_START))->toBeFalse()
        ->and($source->status(WINDOW_START + 662 + 58))->toBe('stale')
        ->and($source->status(WINDOW_START + 722 + 58))->toBe('stale');

    flushAt($log, $source, WINDOW_START + 782);
    $throughs = array_map(strtotime(...), array_column(array_column($source->attempts, 'event_time'), 'through'));
    expect($source->records())->toHaveCount(1)
        ->and($source->through())->toBe(iso(WINDOW_START + 782))
        ->and($source->completedAt[WINDOW_START])->toBe(WINDOW_START + 782)
        ->and($source->status(WINDOW_START + 782 + 58))->toBe('complete');
    $sorted = $throughs;
    sort($sorted);
    expect($throughs)->toBe($sorted);
});

it('keeps a window complete when a late part is accepted, and withdraws it while a late part cannot be sent', function (): void {
    $s = redisSettings();
    $log = new RedisApiActivityLog($this->app->make('redis'), $s);
    $source = memorySource([null, null, new SignalsFailure('unavailable', 503)]);
    everyMinute($log, $source, WINDOW_START + 2, WINDOW_START + 302);
    $log->record(call($s, 1000, '/v1/a'));
    everyMinute($log, $source, WINDOW_START + 362, WINDOW_START + 662);
    expect($source->complete(WINDOW_START))->toBeTrue();

    $log->record(call($s, 2000, '/v1/a')); // a slow request, counted after the window was sealed
    expect(flushAt($log, $source, WINDOW_START + 722))->toBe(['parts' => 1, 'records' => 1, 'coverage' => 'complete']);
    expect($source->complete(WINDOW_START))->toBeTrue()->and($source->through())->toBe(iso(WINDOW_START + 722));

    $log->record(call($s, 3000, '/v1/a')); // another, and this time Signals is unavailable
    expect(flushAt($log, $source, WINDOW_START + 782))->toBe(['parts' => 0, 'records' => 0, 'coverage' => 'partial']);
    $frame = end($source->frames)[1];
    expect($frame['event_time'])->toMatchArray(['status' => 'partial', 'from' => iso(WINDOW_START), 'through' => iso(WINDOW_START + 722), 'reasons' => ['late_records_pending']])
        ->and($frame['completeness'])->toBe('partial')
        ->and($source->complete(WINDOW_START))->toBeFalse()
        ->and($source->status(WINDOW_START + 782 + 58))->toBe('partial');

    expect(flushAt($log, $source, WINDOW_START + 842))->toBe(['parts' => 1, 'records' => 1, 'coverage' => 'complete']);
    $frame = end($source->frames)[1];
    expect($frame['event_time'])->toMatchArray(['from' => iso(WINDOW_START - 1), 'through' => iso(WINDOW_START + 842)])
        ->and($frame['commit_through'])->toBe('3')
        ->and($source->complete(WINDOW_START))->toBeTrue()
        ->and(answered($source->records()))->toBe(3)
        ->and(array_column(array_column($source->records(), 'position'), 'sequence'))->toBe(['1', '2', '3']);
});

it('never moves through back after a frame that may have arrived, nor sends two frames for one second', function (): void {
    $s = redisSettings();
    $log = new RedisApiActivityLog($this->app->make('redis'), $s);
    $unavailable = new SignalsFailure('unavailable', 503);
    $source = memorySource([null, $unavailable], [...array_fill(0, 12, null), $unavailable]);
    everyMinute($log, $source, WINDOW_START + 2, WINDOW_START + 302);
    $log->record(call($s, 1000, '/v1/a'));
    everyMinute($log, $source, WINDOW_START + 362, WINDOW_START + 662);

    // The frame claiming +722 times out: Signals may have stored it.
    expect(flushAt($log, $source, WINDOW_START + 722)['coverage'])->toBeNull();
    $log->record(call($s, 2000, '/v1/a')); // a late part that cannot be sent at once
    expect(flushAt($log, $source, WINDOW_START + 782)['coverage'])->toBe('partial')
        ->and(flushAt($log, $source, WINDOW_START + 782))->toBe(['parts' => 0, 'records' => 0, 'coverage' => null]);
    expect(end($source->attempts)['event_time']['through'])->toBe(iso(WINDOW_START + 722));
    everyMinute($log, $source, WINDOW_START + 842, WINDOW_START + 902);

    $throughs = array_map(fn ($f) => strtotime($f['event_time']['through']), $source->attempts);
    $emitted = array_map(fn ($f) => strtotime($f['emitted_at']), $source->attempts);
    $sorted = $throughs;
    sort($sorted);
    expect($throughs)->toBe($sorted)
        ->and(array_unique($emitted))->toHaveCount(count($emitted))
        ->and($source->through())->toBe(iso(WINDOW_START + 902))
        ->and($source->complete(WINDOW_START))->toBeTrue()
        // The late part kept the position it reserved before its first attempt.
        ->and(array_column(array_column($source->records(), 'position'), 'sequence'))->toBe(['1', '2']);
});

it('makes a window with a refused record partial for good, and commits only accepted positions', function (): void {
    $s = redisSettings(shards: 1);
    $log = new RedisApiActivityLog($this->app->make('redis'), $s);
    $invalid = new SignalsFailure('invalid_canonical_record', 422);
    $source = memorySource([$invalid, null, $invalid]);
    everyMinute($log, $source, WINDOW_START + 2, WINDOW_START + 302);
    $log->record(call($s, 1000, '/v1/a'));
    $log->record(call($s, 2000, '/v1/a', 200, [], new ApiActor(user: 'u2')));
    everyMinute($log, $source, WINDOW_START + 362, WINDOW_START + 602);

    expect(flushAt($log, $source, WINDOW_START + 662))->toBe(['parts' => 1, 'records' => 1, 'coverage' => 'partial']);
    $frame = end($source->frames)[1];
    $refused = json_decode(Redis::connection()->lrange('signals:api:failed', 0, -1)[0], true)['record']['position']['sequence'];
    expect($frame)->toMatchArray(['completeness' => 'partial', 'omitted_records' => 1, 'reason_codes' => ['records_refused'], 'commit_through' => (string) $source->highest])
        ->and($frame['commit_through'])->not->toBe($refused)
        ->and($frame['event_time']['from'])->toBe(iso(WINDOW_START));

    $log->record(call($s, 1000, '/v1/a', 200, [], null, 'GET', WINDOW_START + 600));
    everyMinute($log, $source, WINDOW_START + 722, WINDOW_START + 1262);
    expect(end($source->frames)[1]['event_time']['from'])->toBe(iso(WINDOW_START + 600))
        ->and($source->complete(WINDOW_START))->toBeFalse()
        ->and($source->complete(WINDOW_START + 600))->toBeTrue()
        ->and($source->status(WINDOW_START + 1262 + 58))->toBe('complete');
});

it('takes the positions 0.1.0 reserved as its first commit when it is upgraded', function (): void {
    Redis::connection()->set('signals:api:positions:g1:0', '7');
    $log = new RedisApiActivityLog($this->app->make('redis'), redisSettings());
    $source = memorySource();
    $source->highest = 7;
    expect(flushAt($log, $source, WINDOW_START + 2)['coverage'])->toBe('complete')
        ->and(end($source->frames)[1]['commit_through'])->toBe('7');
});

it('claims no commit after Signals refused a frame, until a record is accepted again', function (): void {
    $s = redisSettings();
    $log = new RedisApiActivityLog($this->app->make('redis'), $s);
    $source = memorySource([], [new SignalsFailure('invalid_canonical_record', 422)]);
    expect(flushAt($log, $source, WINDOW_START + 2)['coverage'])->toBeNull();
    expect(flushAt($log, $source, WINDOW_START + 62)['coverage'])->toBe('partial')
        ->and(end($source->frames)[1])->toMatchArray(['commit_through' => '0', 'reason_codes' => ['commit_unknown']]);
    $log->record(call($s, 1000, '/v1/a', 200, [], null, 'GET', WINDOW_START + 60));
    everyMinute($log, $source, WINDOW_START + 122, WINDOW_START + 722);
    expect(end($source->frames)[1])->toMatchArray(['commit_through' => '1', 'completeness' => 'complete']);
});
