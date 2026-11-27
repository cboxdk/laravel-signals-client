<?php

use Cbox\Signals\ApiActivity\ApiActor;
use Cbox\Signals\Contracts\SourceRecords;
use Cbox\Signals\Data\Entity;
use Cbox\Signals\Data\SourceReceipt;
use Cbox\Signals\Exceptions\SignalsFailure;
use Cbox\Signals\McpActivity\Attempt;
use Cbox\Signals\McpActivity\DatabaseMcpActivityBuffers;
use Cbox\Signals\McpActivity\DatabaseMcpActivityLog;
use Cbox\Signals\McpActivity\Outcome;
use Cbox\Signals\McpActivity\Settings;
use Illuminate\Database\Events\TransactionCommitted;
use Orchestra\Testbench\TestCase;

uses(TestCase::class);

class ClockedMcpJournal extends DatabaseMcpActivityLog
{
    public int $clock = WINDOW_START * 1000;

    protected function nowMs(): int
    {
        return $this->clock;
    }
}

class McpJournalReceiver implements SourceRecords
{
    public array $records = [];

    public ?Closure $beforeReceipt = null;

    public function send(string $source, array $records): SourceReceipt
    {
        $this->records[] = $records[0];
        if ($this->beforeReceipt !== null) {
            ($this->beforeReceipt)($records[0]);
        }

        return new SourceReceipt(1, 0, new DateTimeImmutable);
    }

    public function events(): array
    {
        return array_values(array_filter($this->records, fn ($r) => $r['kind'] === 'event'));
    }

    public function coverage(): array
    {
        return array_values(array_filter($this->records, fn ($r) => $r['kind'] === 'coverage'));
    }
}

function durableSettings(int $timeout = 300000, bool $confirmed = true, int $capacity = 10000): Settings
{
    $s = mcpSettings();

    return new Settings($s->source, $s->catalog, timeoutMs: $timeout, capacity: $capacity, dispatchProofConfirmed: $confirmed);
}

function durableAttempt(Settings $settings, string $user = 'u1', string $action = 'listPages'): Attempt
{
    // Deliberately wrong caller time: only database admission time is authoritative.
    return Attempt::capture($settings, new ApiActor(user: $user, organization: 'o1', token: 't1'), 0, 'pages', $action, ['page_id' => 'object-one']);
}

beforeEach(function () {
    config()->set('database.default', 'testing');
    config()->set('database.connections.testing', ['driver' => 'sqlite', 'database' => ':memory:']);
    (require __DIR__.'/../database/migrations/2026_11_26_000001_create_signals_mcp_buffers.php')->up();
    $this->db = app('db')->connection();
    $this->settings = durableSettings();
    $this->journal = new ClockedMcpJournal($this->db, $this->settings);
    $this->receiver = new McpJournalReceiver;
});

it('reconciles two actors and different tools before certifying exact attempts, including a zero window', function () {
    foreach ([['u1', 'getPage'], ['u1', 'getPage'], ['u2', 'listPages']] as [$user, $action]) {
        $id = $this->journal->start(durableAttempt($this->settings, $user, $action));
        $this->journal->finish($id, Outcome::Success);
    }
    $this->journal->clock += 600000;
    expect($this->journal->flush($this->receiver))->toBe(2);
    expect(array_column(array_column($this->receiver->events(), 'data'), 'total_attempts'))->toBe([2, 1]);
    $frame = $this->receiver->coverage()[0];
    expect($frame['completeness'])->toBe('complete')->and($frame['commit_through'])->toBe('2')->and(coverageErrors($frame))->toBe([]);
    expect($this->receiver->events()[0]['data']['window_start'])->toBe(gmdate('Y-m-d\TH:i:s\Z', WINDOW_START));
    $this->journal->clock += 600000;
    $this->journal->flush($this->receiver);
    expect($this->receiver->events())->toHaveCount(2)->and($this->receiver->coverage()[1]['completeness'])->toBe('complete');
    expect($this->receiver->coverage()[1]['event_time']['from'])->toBe(gmdate('Y-m-d\TH:i:s\Z', WINDOW_START + 600));
});

it('proves an empty interval without inventing an event but never retroactively proves an unobserved prefix', function () {
    $this->journal->clock += 123;
    $this->journal->status();
    $this->journal->clock = (WINDOW_START + 600) * 1000;
    $this->journal->flush($this->receiver);
    expect($this->receiver->coverage()[0]['reason_codes'])->toContain('started_mid_window');
    $this->journal->clock += 600000;
    $this->journal->flush($this->receiver);
    $frame = $this->receiver->coverage()[1];
    expect($frame['completeness'])->toBe('complete')->and($frame['commit_through'])->toBe('0')->and($frame['observed_families'])->toBe(['mcp.activity'])
        ->and($this->receiver->events())->toBe([])->and(coverageErrors($frame))->toBe([]);
});

it('keeps pending and deadline unknown incomplete without poisoning a subsequent healthy interval', function () {
    $s = durableSettings(timeout: 900000);
    $journal = new ClockedMcpJournal($this->db, $s);
    $id = $journal->start(durableAttempt($s));
    $journal->clock += 600000;
    $journal->flush($this->receiver);
    expect($this->receiver->events())->toBe([])->and($this->receiver->coverage()[0]['reason_codes'])->toContain('attempts_pending');
    $journal->clock += 300000;
    $journal->finish($id, Outcome::Success, true); // Deadline wins, not the late success.
    $journal->flush($this->receiver);
    expect($this->receiver->events()[0]['data']['operations'][0]['outcomes']['unknown'])->toBe(1)
        ->and($this->receiver->coverage()[1]['reason_codes'])->toContain('unknown_outcomes');
    $next = $journal->start(durableAttempt($s, 'u2'));
    $journal->finish($next, Outcome::Denied);
    $journal->clock += 300000;
    $journal->flush($this->receiver);
    expect($this->receiver->coverage()[2]['completeness'])->toBe('complete');
});

it('requires the host inventory assertion and refuses clock rollback, configuration reuse and outer transactions', function () {
    $s = durableSettings(confirmed: false);
    $journal = new ClockedMcpJournal($this->db, $s);
    $journal->status();
    $journal->clock += 600000;
    $journal->flush($this->receiver);
    expect($this->receiver->coverage()[0]['reason_codes'])->toContain('dispatch_inventory_unconfirmed');
    $journal->clock--;
    expect(fn () => $journal->start(durableAttempt($s)))->toThrow(RuntimeException::class);
    expect(fn () => $this->journal->start(durableAttempt($this->settings)))->toThrow(RuntimeException::class);
    $this->db->beginTransaction();
    try {
        expect(fn () => $journal->start(durableAttempt($s)))->toThrow(RuntimeException::class, 'dedicated connection');
    } finally {
        $this->db->rollBack();
    }
});

it('refuses capacity before admission and retries immutable events and coverage after lost receipts', function () {
    $s = durableSettings(capacity: 1);
    $journal = new ClockedMcpJournal($this->db, $s);
    $id = $journal->start(durableAttempt($s));
    expect(fn () => $journal->start(durableAttempt($s, 'u2')))->toThrow(RuntimeException::class, 'full');
    $journal->finish($id, Outcome::Success);
    $journal->clock += 600000;
    $this->receiver->beforeReceipt = function () {
        throw new RuntimeException('lost reply');
    };
    expect(fn () => $journal->flush($this->receiver))->toThrow(RuntimeException::class);
    $again = new ClockedMcpJournal($this->db, $s);
    $again->clock = $journal->clock;
    $this->receiver->beforeReceipt = function ($record) {
        if ($record['kind'] === 'coverage') {
            throw new RuntimeException('lost coverage reply');
        }
    };
    expect(fn () => $again->flush($this->receiver))->toThrow(RuntimeException::class);
    expect($this->receiver->events()[0])->toBe($this->receiver->events()[1]);
    $this->receiver->beforeReceipt = null;
    $again->flush($this->receiver);
    expect($this->receiver->coverage()[0])->toBe($this->receiver->coverage()[1]);
    expect($again->status())->toMatchArray(['pending' => 0, 'sealed' => 0, 'retained_parts' => 1]);
});

it('recovers an uncertain commit only by reading its own durable admission', function () {
    $lost = false;
    $this->db->getEventDispatcher()->listen(TransactionCommitted::class, function () use (&$lost) {
        if (! $lost) {
            $lost = true;
            throw new RuntimeException('synthetic lost COMMIT reply');
        }
    });
    $id = $this->journal->start(durableAttempt($this->settings));
    expect($id)->toBeString();
    $this->journal->finish($id, Outcome::Success);
    $this->journal->clock += 600000;
    $this->journal->flush($this->receiver);
    expect($this->receiver->events())->toHaveCount(1)->and($this->receiver->events()[0]['data']['total_attempts'])->toBe(1)
        ->and($this->receiver->coverage()[0]['completeness'])->toBe('complete');
});

it('does not turn missing journal payload into complete coverage', function () {
    $id = $this->journal->start(durableAttempt($this->settings));
    $this->journal->finish($id, Outcome::Success);
    $this->db->table('signals_mcp_attempts')->delete();
    $this->journal->clock += 600000;
    $this->journal->flush($this->receiver);
    expect($this->receiver->events())->toBe([])->and($this->receiver->coverage()[0]['reason_codes'])->toContain('journal_unreconciled');
});

it('invalidates SQL payloads and acknowledged coverage for erasure and rejects restored payloads', function () {
    $id = $this->journal->start(durableAttempt($this->settings));
    $this->journal->finish($id, Outcome::Success);
    $this->journal->clock += 600000;
    $this->journal->flush($this->receiver);
    $part = (array) $this->db->table('signals_mcp_parts')->first();
    $buffers = new DatabaseMcpActivityBuffers($this->db, app('redis'));
    $buffers->erase(new Entity('testing', 'user', 'u1'));
    expect($this->db->table('signals_mcp_parts')->count())->toBe(0);
    $this->db->table('signals_mcp_parts')->insert($part); // Simulated old payload restore, current tombstones survive.
    $this->journal->clock += 1000;
    $this->journal->flush($this->receiver);
    expect($this->db->table('signals_mcp_parts')->count())->toBe(0)->and($this->receiver->events())->toHaveCount(1)
        ->and($this->receiver->coverage()[1]['reason_codes'])->toContain('erased', 'journal_unreconciled');
    expect($this->receiver->coverage()[1]['omitted_records'])->toBe(1);
    expect(fn () => $this->journal->start(durableAttempt($this->settings)))->toThrow(RuntimeException::class, 'erased');
    $this->journal->finish($id, Outcome::Success);
    expect($this->journal->status()['pending'])->toBe(0);
});

it('delivers new unrelated actors after erasure in the same window without restoring its proof', function () {
    $id = $this->journal->start(durableAttempt($this->settings));
    $this->journal->finish($id, Outcome::Success);
    $buffers = new DatabaseMcpActivityBuffers($this->db, app('redis'));
    $buffers->erase(new Entity('testing', 'user', 'u1'));
    $id = $this->journal->start(durableAttempt($this->settings, 'u2'));
    $this->journal->finish($id, Outcome::Success);
    $this->journal->clock += 600000;
    $this->journal->flush($this->receiver);
    expect($this->receiver->events())->toHaveCount(1)
        ->and($this->receiver->events()[0]['data']['subject']['id'])->toBe('u2')
        ->and($this->receiver->coverage()[0]['completeness'])->toBe('partial')
        ->and($this->receiver->coverage()[0]['reason_codes'])->toContain('erased')
        ->and($this->receiver->coverage()[0]['omitted_records'])->toBe(0)
        ->and($this->journal->status()['pending'])->toBe(0);
    $this->journal->clock = (WINDOW_START + 604801) * 1000;
    $this->journal->flush($this->receiver);
    expect($this->journal->status()['retained_parts'])->toBe(0)
        ->and(end($this->receiver->records)['reason_codes'])->toContain('expired')
        ->and(end($this->receiver->records)['omitted_records'])->toBe(1);
});

it('cannot acknowledge an erased in-flight part or resurrect a stale complete frame', function () {
    $id = $this->journal->start(durableAttempt($this->settings));
    $this->journal->finish($id, Outcome::Success);
    $this->journal->clock += 600000;
    $buffers = new DatabaseMcpActivityBuffers($this->db, app('redis'));
    $this->receiver->beforeReceipt = function ($record) use ($buffers) {
        if ($record['kind'] === 'event') {
            $buffers->erase(new Entity('testing', 'token', 't1'));
        }
    };
    $this->journal->flush($this->receiver);
    expect($this->receiver->coverage()[0]['completeness'])->toBe('partial')
        ->and($this->receiver->coverage()[0]['commit_through'])->toBe('0');
});

it('withdraws expired payloads before retiring proof and retracts a rejected consumer commit', function () {
    $id = $this->journal->start(durableAttempt($this->settings));
    $this->journal->finish($id, Outcome::Success);
    $this->journal->clock += 600000;
    $this->journal->flush($this->receiver);
    $this->receiver->beforeReceipt = function () {
        throw new SignalsFailure('invalid_record', 422);
    };
    $this->journal->clock += 1000;
    expect(fn () => $this->journal->flush($this->receiver))->toThrow(SignalsFailure::class);
    $this->receiver->beforeReceipt = null;
    $this->journal->clock += 1000;
    $this->journal->flush($this->receiver);
    expect(end($this->receiver->records)['reason_codes'])->toContain('commit_unknown');
    $this->journal->clock = (WINDOW_START + 604801) * 1000;
    $this->journal->flush($this->receiver);
    expect($this->db->table('signals_mcp_parts')->count())->toBe(0)->and(end($this->receiver->records)['reason_codes'])->toContain('expired');
});
