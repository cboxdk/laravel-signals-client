<?php

use Cbox\Signals\ApiActivity\ApiActivitySettings;
use Cbox\Signals\ApiActivity\ApiActor;
use Cbox\Signals\Contracts\SourceRecords;
use Cbox\Signals\Data\Entity;
use Cbox\Signals\Data\SourceReceipt;
use Cbox\Signals\Exceptions\SignalsFailure;
use Cbox\Signals\McpActivity\Attempt;
use Cbox\Signals\McpActivity\Catalog;
use Cbox\Signals\McpActivity\DatabaseMcpActivityBuffers;
use Cbox\Signals\McpActivity\Operation;
use Cbox\Signals\McpActivity\Outcome;
use Cbox\Signals\McpActivity\RedisMcpActivityLog;
use Cbox\Signals\McpActivity\Settings;
use Illuminate\Support\Facades\Redis;
use Orchestra\Testbench\TestCase;

uses(TestCase::class);

beforeEach(function () {
    if (! getenv('SIGNALS_TEST_REDIS')) {
        $this->markTestSkipped('SIGNALS_TEST_REDIS is not set.');
    }
    $url = parse_url(getenv('SIGNALS_TEST_REDIS'));
    config()->set('database.redis.client', 'phpredis');
    config()->set('database.redis.options.prefix', '');
    config()->set('database.redis.default', ['host' => $url['host'], 'port' => $url['port'], 'database' => (int) ltrim($url['path'], '/')]);
    Redis::connection()->flushdb();
    config()->set('database.default', 'testing');
    config()->set('database.connections.testing', ['driver' => 'sqlite', 'database' => ':memory:']);
    (require __DIR__.'/../database/migrations/2026_11_26_000001_create_signals_mcp_buffers.php')->up();
    $this->buffers = new DatabaseMcpActivityBuffers(app('db')->connection(), app('redis'));
});

it('expires once, survives a sender restart beyond lookback, and retries the identical sealed window', function () {
    $s = mcpSettings(2);
    $log = new RedisMcpActivityLog(app('redis'), $s, $this->buffers);
    $start = intdiv(time() - 7200, 600) * 600;
    $capture = fn ($user) => Attempt::capture($s, new ApiActor(user: $user), $start * 1000 + 1, 'pages', 'getPage', ['page_id' => 'page-one']);
    $id = $log->start($capture('u1'));
    $second = $log->start($capture('u2'));
    expect($log->start($capture('u3')))->toBeNull()->and($log->status()['refused'])->toBe(1);
    $log->finish($id, Outcome::Unknown);
    $log->finish($id, Outcome::Success, true);
    $source = new class implements SourceRecords
    {
        public array $records = [];

        public bool $loseResponse = true;

        public function send(string $source, array $records): SourceReceipt
        {
            $this->records[] = $records[0];
            if ($this->loseResponse) {
                $this->loseResponse = false;
                throw new RuntimeException('synthetic response lost after acceptance');
            }

            return new SourceReceipt(0, 1, new DateTimeImmutable);
        }
    };
    expect(fn () => $log->flush($source))->toThrow(RuntimeException::class);
    $again = new RedisMcpActivityLog(app('redis'), $s, $this->buffers);
    expect($again->flush($source))->toBe(2)->and($source->records[0])->toBe($source->records[1]);
    $records = array_values(array_filter(array_slice($source->records, 1), fn ($r) => $r['kind'] === 'event'));
    expect($records)->toHaveCount(2);
    foreach ($records as $record) {
        expect($record['data']['total_attempts'])->toBe(1)->and($record['data']['operations'][0]['outcomes']['unknown'])->toBe(1)
            ->and($record['data']['operations'][0]['replayed'])->toBe(0)
            ->and($record['data']['window_start'])->toBe(gmdate('Y-m-d\TH:i:s\Z', $start));
    }
    expect($records[0]['data']['subject']['id'])->not->toBe($records[1]['data']['subject']['id']);
    $again->finish($second, Outcome::Denied);
    expect($again->flush($source))->toBe(0)->and($again->status())->toMatchArray(['pending' => 0, 'sealed' => 0, 'refused' => 1, 'expired_attempts' => 0, 'expired_records' => 0, 'coverage' => 'partial']);
    $frame = array_values(array_filter($source->records, fn ($r) => $r['kind'] === 'coverage'))[0];
    expect(coverageErrors($frame))->toBe([])->and($frame['commit_through'])->toBe('2')->and($frame['reason_codes'])->toContain('refused', 'unknown_outcomes');
    $changed = new RedisMcpActivityLog(app('redis'), new Settings($s->source, new Catalog('new-catalog', [])), $this->buffers);
    expect(fn () => $changed->flush($source))->toThrow(UnexpectedValueException::class);
    if ($path = getenv('MCP_RECORD_OUTPUT')) {
        file_put_contents($path, json_encode([...$records, $frame], JSON_THROW_ON_ERROR));
    }
});

it('does not expire before deadline and terminal completion wins only once', function () {
    $s = mcpSettings();
    $log = new RedisMcpActivityLog(app('redis'), $s, $this->buffers);
    $a = Attempt::capture($s, new ApiActor(user: 'u1'), (int) floor(microtime(true) * 1000), 'pages', 'listPages', []);
    $id = $log->start($a);
    $log->finish($id, Outcome::Unknown);
    expect(Redis::connection()->hget($s->prefix().':outcomes', $id))->toBeFalse();
    $log->finish($id, Outcome::Denied);
    $log->finish($id, Outcome::Success);
    expect(Redis::connection()->hget($s->prefix().':outcomes', $id))->toBe('denied:0');

    // Reusing a revision/key id must not silently mix old and new meanings.
    $changedCatalog = new Settings($s->source, new Catalog('catalog-1', [new Operation('pages', 'getPage', 'page-v1', ['other_id'])]));
    $changedKey = new Settings(new ApiActivitySettings(...array_replace(get_object_vars($s->source), ['referenceKey' => 'different-secret'])), $s->catalog);
    $changedWindow = new Settings(new ApiActivitySettings(...array_replace(get_object_vars($s->source), ['window' => 1200])), $s->catalog);
    foreach ([$changedCatalog, $changedKey, $changedWindow] as $changed) {
        $other = new RedisMcpActivityLog(app('redis'), $changed, $this->buffers);
        expect(fn () => $other->start($a))->toThrow(UnexpectedValueException::class);
        expect(fn () => $other->finish($id, Outcome::Success))->toThrow(UnexpectedValueException::class);
    }
});

it('does not deliver expired payloads or reset source positions when buffers expire', function () {
    $base = mcpSettings();
    $s = new Settings($base->source, $base->catalog, retentionSeconds: 7200);
    $log = new RedisMcpActivityLog(app('redis'), $s, $this->buffers);
    $now = time();
    $capture = fn ($user, $start) => Attempt::capture($s, new ApiActor(user: $user), $start * 1000, 'pages', 'listPages', []);
    expect($log->start($capture('already-expired', $now - 7201)))->toBeNull();
    $log->start($capture('expires-soon', $now - 7200 + 3));
    $log->start($capture('retained', $now - 3600));
    $sender = new class implements SourceRecords
    {
        public bool $fail = true;

        public array $received = [];

        public function send(string $source, array $records): SourceReceipt
        {
            if ($this->fail) {
                throw new RuntimeException('synthetic outage');
            }
            $this->received[] = $records[0];

            return new SourceReceipt(1, 0, new DateTimeImmutable);
        }
    };
    expect(fn () => $log->flush($sender))->toThrow(RuntimeException::class);
    expect(Redis::connection()->ttl($s->prefix().':sealed'))->toBeGreaterThan(0);
    sleep(4);
    $sender->fail = false;
    expect($log->flush($sender))->toBe(1)->and($sender->received[0]['data']['subject']['id'])->toBe('retained')
        ->and($sender->received[0]['position']['sequence'])->toBeIn(['1', '2'])
        ->and($log->status()['expired_records'])->toBe(1)->and($log->status()['expired_attempts'])->toBe(1)
        ->and($log->status()['coverage'])->toBe('partial');
    $log->start($capture('next', $now - 3000));
    expect($log->flush($sender))->toBe(1)->and($sender->received[2]['position']['sequence'])->toBe('3');
});

it('reports unknown idle coverage and retries an unacknowledged frame unchanged without charging attempts', function () {
    $log = new RedisMcpActivityLog(app('redis'), mcpSettings(), $this->buffers);
    $sender = new class implements SourceRecords
    {
        public array $received = [];

        public function send(string $source, array $records): SourceReceipt
        {
            $this->received[] = $records[0];

            return new SourceReceipt(0, count($this->received) > 1 ? 1 : 0, new DateTimeImmutable);
        }
    };
    expect(fn () => $log->flush($sender))->toThrow(UnexpectedValueException::class);
    expect($log->flush($sender))->toBe(0)->and($sender->received[0])->toBe($sender->received[1]);
    $frame = $sender->received[1];
    expect(coverageErrors($frame))->toBe([])->and($frame['commit_through'])->toBe('0')
        ->and($frame['completeness'])->toBe('unknown')->and($frame['observed_families'])->toBe([])
        ->and($frame['reason_codes'])->toBe(['collector_fail_open']);
});

it('erases pending and sealed windows across generations and prevents late completion or restored buffers from resurrecting them', function ($type) {
    $s = mcpSettings();
    $later = new Settings(new ApiActivitySettings(...array_replace(get_object_vars($s->source), ['generation' => 'g2', 'referenceKey' => 'rotated-secret'])), $s->catalog);
    $logs = array_map(fn ($settings) => new RedisMcpActivityLog(app('redis'), $settings, $this->buffers), [$s, $later]);
    $actor = new ApiActor(user: 'erased-user', organization: 'erased-org', token: 'erased-token');
    $old = (time() - 7200) * 1000;
    $pending = $logs[0]->start(Attempt::capture($s, $actor, (int) floor(microtime(true) * 1000), 'pages', 'listPages', []));
    $logs[1]->start(Attempt::capture($later, $actor, $old, 'pages', 'listPages', []));
    $logs[1]->start(Attempt::capture($later, new ApiActor(ip: '192.0.2.8'), $old, 'pages', 'listPages', []));
    $logs[1]->start(Attempt::capture($later, new ApiActor(user: 'unrelated-user'), $old, 'pages', 'listPages', []));
    $sender = new class implements SourceRecords
    {
        public bool $fail = true;

        public array $events = [];

        public function send(string $source, array $records): SourceReceipt
        {
            if ($this->fail) {
                throw new RuntimeException('response lost');
            }
            if ($records[0]['kind'] === 'event') {
                $this->events[] = $records[0];
            }

            return new SourceReceipt(1, 0, new DateTimeImmutable);
        }
    };
    expect(fn () => $logs[1]->flush($sender))->toThrow(RuntimeException::class);
    $backup = Redis::connection()->lrange($later->prefix().':sealed', 0, -1);
    $id = ['user' => 'erased-user', 'organization' => 'erased-org', 'token' => 'erased-token'][$type];
    $this->buffers->erase(new Entity($s->source->namespace, $type, $id));
    expect($logs[0]->status()['pending'])->toBe(0)->and($logs[1]->status()['sealed'])->toBe(1);
    $logs[0]->finish($pending, Outcome::Success);
    expect($logs[0]->start(Attempt::capture($s, $actor, $old, 'pages', 'listPages', [])))->toBeNull();
    // Simulate Redis restoration with the SQL tombstone still durable.
    Redis::connection()->del($later->prefix().':sealed');
    Redis::connection()->rpush($later->prefix().':sealed', ...$backup);
    $sender->fail = false;
    $restarted = new RedisMcpActivityLog(app('redis'), $later, new DatabaseMcpActivityBuffers(app('db')->connection(), app('redis')));
    expect($restarted->flush($sender))->toBe(1)->and($sender->events)->toHaveCount(1)
        ->and($sender->events[0]['data']['subject']['id'])->toBe('unrelated-user');
    expect($restarted->status()['coverage'])->toBe('partial');
})->with(['user', 'organization', 'token']);

it('keeps the SQL erasure fence when Redis cleanup fails and retries cleanup while disabled', function () {
    $s = mcpSettings();
    $log = new RedisMcpActivityLog(app('redis'), $s, $this->buffers);
    $actor = new ApiActor(user: 'u-delete');
    $attempt = Attempt::capture($s, $actor, (int) floor(microtime(true) * 1000), 'pages', 'listPages', []);
    $log->start($attempt);
    // Wrong Redis type makes cleanup fail after the tombstone commit.
    Redis::connection()->set($s->prefix().':sealed', 'synthetic fault');
    expect(fn () => $this->buffers->erase(new Entity($s->source->namespace, 'user', 'u-delete')))->toThrow(SignalsFailure::class);
    expect(app('db')->table('signals_mcp_erasures')->count())->toBe(1);
    Redis::connection()->del($s->prefix().':sealed');
    $this->buffers->erase(new Entity($s->source->namespace, 'user', 'u-delete'));
    expect($log->status()['pending'])->toBe(0)->and($log->start($attempt))->toBeNull();
    expect(app('db')->table('signals_mcp_erasures')->count())->toBe(1);
});

it('retracts a coverage commit rejected after engine erasure instead of wedging heartbeats', function () {
    $s = mcpSettings();
    $log = new RedisMcpActivityLog(app('redis'), $s, $this->buffers);
    $log->start(Attempt::capture($s, new ApiActor(user: 'erased-at-consumer'), (time() - 7200) * 1000, 'pages', 'listPages', []));
    $sender = new class implements SourceRecords
    {
        public array $frames = [];

        public function send(string $source, array $records): SourceReceipt
        {
            if ($records[0]['kind'] === 'coverage') {
                $this->frames[] = $records[0];
                if (count($this->frames) === 1) {
                    throw new SignalsFailure('invalid', 422);
                }
            }

            return new SourceReceipt(1, 0, new DateTimeImmutable);
        }
    };
    expect(fn () => $log->flush($sender))->toThrow(SignalsFailure::class);
    sleep(1);
    expect($log->flush($sender))->toBe(0)->and($sender->frames[0]['commit_through'])->toBe('1')
        ->and($sender->frames[1]['commit_through'])->toBe('0')
        ->and($sender->frames[1]['reason_codes'])->toContain('commit_unknown')
        ->and(coverageErrors($sender->frames[1]))->toBe([]);
});
