<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

use Cbox\Signals\Contracts\McpActivityLog;
use Cbox\Signals\Contracts\SourceRecords;
use Cbox\Signals\Data\Entity;
use Cbox\Signals\Exceptions\SignalsFailure;
use Cbox\Signals\Support\Wire;
use Closure;
use Illuminate\Database\Connection;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Query\Builder;
use RuntimeException;
use stdClass;
use Throwable;

/** Durable admission, terminal outcomes and immutable source outbox. Redis is not evidence. */
class DatabaseMcpActivityLog implements McpActivityLog
{
    private readonly string $source;

    public function __construct(private readonly Connection $db, public readonly Settings $settings)
    {
        $source = $settings->source;
        $this->source = hash('sha256', json_encode([$source->scope, $source->sourceId, $source->partition, $source->generation], JSON_THROW_ON_ERROR));
    }

    public function start(Attempt $attempt): ?string
    {
        $id = bin2hex(random_bytes(16));
        try {
            return $this->guard(function (stdClass $state, int $now, ErasedActors $erased) use ($attempt, $id): string {
                $start = $this->window($now);
                // Only this database clock defines admission time. Request timestamps
                // must not insert into an interval already certified as closed.
                $attempt = new Attempt($now, $attempt->operation, $attempt->actorJson, $attempt->reference);
                if ($erased->contains(Wire::object(json_decode($attempt->actorJson, true, flags: JSON_THROW_ON_ERROR)), $start)) {
                    throw new RuntimeException('MCP actor was erased.');
                }
                if ($this->table('attempts')->count() + $this->table('parts')->count() >= $this->settings->capacity) {
                    throw new RuntimeException('MCP durable journal is full.');
                }
                $payload = $attempt->json();
                if (strlen($payload) > 16384) {
                    throw new RuntimeException('MCP admission metadata exceeds journal limit.');
                }
                $this->ensureWindow($start, self::integer($state->origin_ms));
                $ticket = self::integer($state->ticket) + 1;
                $this->table('attempts')->insert(['id' => $id, 'source' => $this->source, 'start_ms' => $now, 'window_ms' => $start, 'ticket' => $ticket, 'payload' => $payload,
                    ...self::ownership(Wire::object(json_decode($attempt->actorJson, true, flags: JSON_THROW_ON_ERROR)))]);
                $this->table('windows')->where('start_ms', $start)->increment('admitted');
                $this->state()->update(['ticket' => $ticket]);

                return $id;
            });
        } catch (Throwable $error) {
            // A lost COMMIT reply is not permission to dispatch on a guess. Only
            // a durable read of this very admission can authorize execution.
            if ($this->db->transactionLevel() === 0 && $this->table('attempts')->where('id', $id)->exists()) {
                return $id;
            }
            throw $error;
        }
    }

    public function finish(string $id, Outcome $outcome, bool $replayed = false): void
    {
        $this->guard(function (stdClass $state, int $now) use ($id, $outcome, $replayed): void {
            $row = $this->table('attempts')->where('id', $id)->first();
            if ($row === null || $row->outcome !== null) {
                return; // Erased, sealed or already finalized: never recreate it.
            }
            $expired = $now >= self::integer($row->start_ms) + $this->settings->timeoutMs;
            if ($outcome === Outcome::Unknown && ! $expired) {
                return;
            }
            $this->table('attempts')->where('id', $id)->update(['outcome' => $expired ? Outcome::Unknown->value : $outcome->value, 'replayed' => ! $expired && $replayed]);
            $this->table('windows')->where('start_ms', self::integer($row->window_ms))->update(['reported' => false]);
        });
    }

    /** One sender per source, without holding admission's transaction across HTTP. */
    public function flush(SourceRecords $sender): int
    {
        $this->standalone();
        $lock = hexdec(substr($this->source, 0, 15));
        if ($this->db->getDriverName() === 'pgsql') {
            $row = $this->db->selectOne('SELECT pg_try_advisory_lock(?) AS acquired', [$lock], false);
            if (! $row instanceof stdClass || ! $row->acquired) {
                return 0;
            }
        }
        try {
            $this->prepare();
            $sent = 0;
            for ($i = 0; $i < 100; $i++) {
                $part = $this->guard(fn (): ?stdClass => $this->table('parts')->where('acknowledged', false)->orderBy('sequence')->first());
                if ($part === null) {
                    break;
                }
                $payload = Wire::text($part->payload);
                if (! hash_equals(Wire::text($part->payload_hash), hash('sha256', $payload))) {
                    throw new RuntimeException('MCP immutable payload hash mismatch.');
                }
                $receipt = $sender->send($this->settings->source->sourceId, [Wire::object(json_decode($payload, true, flags: JSON_THROW_ON_ERROR))]);
                if ($receipt->accepted + $receipt->duplicates !== 1) {
                    throw new RuntimeException('MCP record was not acknowledged.');
                }
                $this->guard(function () use ($part): void {
                    // Erasure can run while HTTP is in flight. A removed part
                    // cannot gain a receipt or resurrect coverage on its return.
                    if ($this->table('parts')->where('id', $part->id)->update(['acknowledged' => true]) === 1) {
                        $this->state()->update(['committed' => self::integer($part->sequence), 'commit_unknown' => false, 'frame' => null]);
                        $this->table('windows')->where('start_ms', self::integer($part->start_ms))->update(['reported' => false]);
                    }
                });
                $sent++;
            }
            $this->cover($sender);

            return $sent;
        } finally {
            if ($this->db->getDriverName() === 'pgsql') {
                $this->db->selectOne('SELECT pg_advisory_unlock(?)', [$lock], false);
            }
        }
    }

    /** Last acknowledged coverage is finite and historical, not service availability.
     * @return array{pending:int,sealed:int,retained_parts:int,unreported_windows:int,coverage:string,through:?string}
     */
    public function status(): array
    {
        return $this->guard(function (stdClass $state): array {
            $frame = $state->acknowledged_frame === null ? null : Wire::object(json_decode(Wire::text($state->acknowledged_frame), true, flags: JSON_THROW_ON_ERROR));

            return ['pending' => $this->table('attempts')->count(),
                'sealed' => $this->table('parts')->where('acknowledged', false)->count(),
                'retained_parts' => $this->table('parts')->count(),
                'unreported_windows' => $this->table('windows')->where('reported', false)->count(),
                'coverage' => $frame === null ? 'unknown' : Wire::text($frame['completeness']),
                'through' => $frame === null ? null : Wire::text(Wire::object($frame['event_time'])['through'])];
        });
    }

    private function prepare(): void
    {
        $this->guard(function (stdClass $state, int $now): void {
            $width = $this->settings->source->window * 1000;
            $closed = $this->window($now);
            $cutoff = $now - $this->settings->retentionSeconds * 1000;
            // Report withdrawal before retiring even non-identifying manifests.
            $this->table('windows')->where('start_ms', '<', $cutoff)->where('reason', 'expired')->where('reported', true)->delete();
            foreach ($this->table('windows')->where('start_ms', '<', $cutoff)->where(function (Builder $q): void {
                $q->whereNull('reason')->orWhere('reason', '!=', 'expired');
            })->get() as $row) {
                self::invalidate($this->db, $this->source, self::integer($row->start_ms), 'expired');
            }
            $cursor = self::integer($state->materialized_ms);
            // A long outage is a gap, not thousands of invented zero windows.
            if ($cursor < $this->window(max(0, $cutoff))) {
                $cursor = $this->window(max(0, $cutoff));
            }
            for ($i = 0; $i < 100 && $cursor < $closed; $i++, $cursor += $width) {
                $this->ensureWindow($cursor, self::integer($state->origin_ms));
            }
            $this->state()->update(['materialized_ms' => $cursor]);
            $this->table('attempts')->whereNull('outcome')->where('start_ms', '<=', $now - $this->settings->timeoutMs)->update(['outcome' => Outcome::Unknown->value, 'replayed' => false]);
            $sequence = self::integer($state->sequence);
            foreach ($this->table('windows')->where('start_ms', '<', $closed)->whereColumn('admitted', '>', 'sealed')->orderBy('start_ms')->get() as $manifest) {
                $start = self::integer($manifest->start_ms);
                // Erasure permanently invalidates this interval, but must not
                // strand unrelated actors admitted later in the same window.
                if ($manifest->reason === 'expired' || $this->table('attempts')->where('window_ms', $start)->whereNull('outcome')->exists()) {
                    continue;
                }
                /** @var array<string,Window> $windows */
                $windows = [];
                $rows = $this->table('attempts')->where('window_ms', $start)->orderBy('ticket')->get();
                $unknown = false;
                foreach ($rows as $row) {
                    $attempt = Attempt::fromJson(Wire::text($row->payload));
                    $actor = hash('sha256', $attempt->actorJson);
                    $windows[$actor] ??= new Window($this->settings, intdiv($start, 1000), $attempt->actorJson);
                    $outcome = Outcome::from(Wire::text($row->outcome));
                    $unknown = $unknown || $outcome === Outcome::Unknown;
                    $windows[$actor]->add(new FinalizedAttempt($attempt, $outcome, (bool) $row->replayed, self::integer($row->ticket)));
                }
                $omitted = 0;
                foreach ($windows as $window) {
                    $id = 'mcp-'.bin2hex(random_bytes(16));
                    $record = $window->record($id, intdiv($now, 1000));
                    $position = Wire::object($record['position']);
                    $position['sequence'] = (string) ++$sequence;
                    $record['position'] = $position;
                    $payload = json_encode($record, JSON_THROW_ON_ERROR);
                    $data = Wire::object($record['data']);
                    $omitted += Wire::integer($data['omitted_attempts']);
                    $this->table('parts')->insert(['id' => $id, 'source' => $this->source, 'start_ms' => $start, 'sequence' => $sequence, 'attempts' => Wire::integer($data['total_attempts']), 'payload' => $payload, 'payload_hash' => hash('sha256', $payload), ...self::ownership($data)]);
                }
                if ($rows->isNotEmpty()) {
                    $this->table('attempts')->where('window_ms', $start)->delete();
                    $this->table('windows')->where('start_ms', $start)->update(['sealed' => self::integer($manifest->sealed) + $rows->count(), 'reported' => false,
                        'records' => self::integer($manifest->records) + count($windows),
                        'reason' => $manifest->reason ?? ($unknown ? 'unknown_outcomes' : ($omitted > 0 ? 'omitted_attempts' : null))]);
                }
            }
            $this->state()->update(['sequence' => $sequence]);
        });
    }

    private function cover(SourceRecords $sender): void
    {
        $frame = $this->guard(function (stdClass $state, int $now): ?string {
            $seconds = intdiv($now, 1000);
            if ($state->frame !== null) {
                $pending = Wire::object(json_decode(Wire::text($state->frame), true, flags: JSON_THROW_ON_ERROR));
                if (strtotime(Wire::text($pending['emitted_at'])) >= $seconds - 90) {
                    return Wire::text($state->frame);
                }
            }
            if ($seconds <= self::integer($state->emitted)) {
                return null;
            }
            $query = $this->table('windows')->where('start_ms', '<', $this->window($now));
            $row = (clone $query)->where('reported', false)->orderBy('start_ms')->first() ?? $query->orderByDesc('start_ms')->first();
            if ($row === null) {
                return null;
            }
            $start = self::integer($row->start_ms);
            $reasons = [];
            if (! $this->settings->dispatchProofConfirmed) {
                $reasons[] = 'dispatch_inventory_unconfirmed';
            }
            if ($state->commit_unknown) {
                $reasons[] = 'commit_unknown';
            }
            if ($row->reason !== null) {
                $reasons[] = Wire::text($row->reason);
            }
            if ($this->table('attempts')->where('window_ms', $start)->exists()) {
                $reasons[] = 'attempts_pending';
            }
            $parts = $this->table('parts')->where('start_ms', $start);
            if ((clone $parts)->where('acknowledged', false)->exists()) {
                $reasons[] = 'records_pending';
            }
            $retained = $parts->count();
            if (self::integer($row->admitted) !== self::integer($row->sealed) || self::integer($row->sealed) !== self::integer($parts->sum('attempts')) || self::integer($row->records) !== $retained) {
                $reasons[] = 'journal_unreconciled';
            }
            $complete = $reasons === [];
            $source = $this->settings->source;
            $commit = (string) self::integer($state->committed);
            $iso = static fn (int $at): string => gmdate('Y-m-d\TH:i:s\Z', $at);
            $frame = ['contract_version' => '2.0.0', 'kind' => 'coverage', 'scope' => $source->scope,
                'position' => ['source_id' => $source->sourceId, 'partition' => $source->partition, 'generation' => $source->generation, 'sequence' => $commit],
                'emitted_at' => $iso($seconds), 'event_time' => ['status' => $complete ? 'complete' : 'partial', 'from' => $iso(intdiv($start, 1000)),
                    'through' => $iso(intdiv($start, 1000) + $source->window), 'expected_cadence_seconds' => 60, 'lag_seconds' => max(0, $seconds - intdiv($start, 1000) - $source->window), 'reasons' => $reasons],
                'commit_through' => $commit, 'expected_families' => ['mcp.activity'], 'observed_families' => ['mcp.activity'],
                // Count known missing sealed records, never attempts that might
                // have shared a record. Restoring/deleting one part cannot inflate it.
                'omitted_records' => max(0, self::integer($row->records) - $retained), 'completeness' => $complete ? 'complete' : 'partial', 'reason_codes' => $reasons];
            $json = json_encode($frame, JSON_THROW_ON_ERROR);
            $this->state()->update(['frame' => $json, 'emitted' => $seconds]);

            return $json;
        });
        if ($frame === null) {
            return;
        }
        try {
            $receipt = $sender->send($this->settings->source->sourceId, [Wire::object(json_decode($frame, true, flags: JSON_THROW_ON_ERROR))]);
            if ($receipt->accepted + $receipt->duplicates !== 1) {
                throw new RuntimeException('MCP coverage was not acknowledged.');
            }
        } catch (SignalsFailure $failure) {
            if ($failure->status === 422) {
                $this->guard(fn (): int => $this->state()->update(['committed' => 0, 'commit_unknown' => true, 'frame' => null, 'acknowledged_frame' => null]));
            }
            throw $failure;
        }
        $this->guard(function (stdClass $state) use ($frame): void {
            if ($state->frame !== $frame) {
                return; // An erasure invalidated it while the request was in flight.
            }
            $value = Wire::object(json_decode($frame, true, flags: JSON_THROW_ON_ERROR));
            $time = Wire::object($value['event_time']);
            $this->table('windows')->where('start_ms', strtotime(Wire::text($time['from'])) * 1000)->update(['reported' => true]);
            $this->state()->update(['frame' => null, 'acknowledged_frame' => $frame]);
        });
    }

    private function ensureWindow(int $start, int $origin): void
    {
        if ($this->table('windows')->where('start_ms', $start)->exists()) {
            return;
        }
        if ($this->table('windows')->count() >= 20000) {
            throw new RuntimeException('MCP interval proof backlog is full.');
        }
        $this->table('windows')->insert(['source' => $this->source, 'start_ms' => $start, 'reason' => $start < $origin ? 'started_mid_window' : null]);
    }

    /** @template T
     * @param  Closure(stdClass,int,ErasedActors):T  $operation
     * @return T
     */
    private function guard(Closure $operation): mixed
    {
        $this->standalone();
        $this->settings->source->sendable();

        return $this->db->transaction(function () use ($operation): mixed {
            $guard = $this->db->table('signals_mcp_guard')->where('id', 1)->lockForUpdate()->first();
            if ($guard === null) {
                throw new RuntimeException('MCP privacy migration is required.');
            }
            $now = $this->nowMs();
            $identity = hash('sha256', 'sql-admission-v1:'.$this->settings->identity().':'.(int) $this->settings->dispatchProofConfirmed);
            if (! $this->state()->exists()) {
                if ($this->db->table('signals_mcp_sources')->count() >= 128) {
                    throw new RuntimeException('MCP durable source registry is full.');
                }
                $this->state()->insert(['id' => $this->source, 'identity' => $identity, 'window_ms' => $this->settings->source->window * 1000,
                    'origin_ms' => $now, 'clock_ms' => $now, 'materialized_ms' => $this->window($now)]);
            }
            $state = $this->state()->first();
            if ($state === null || $state->identity !== $identity || $now < self::integer($state->clock_ms)) {
                throw new RuntimeException('MCP source identity or clock changed; do not reuse its generation.');
            }
            $this->state()->update(['clock_ms' => $now]);
            $fingerprints = [];
            foreach ($this->db->table('signals_mcp_erasures')->pluck('fingerprint') as $fingerprint) {
                $fingerprints[Wire::text($fingerprint)] = true;
            }
            $erased = new ErasedActors($fingerprints, self::integer($guard->anonymous_before_ms));
            self::purgeErased($this->db, $erased);
            $state = $this->state()->first();
            if ($state === null) {
                throw new RuntimeException('MCP source registry is missing.');
            }

            return $operation($state, $now, $erased);
        });
    }

    private function standalone(): void
    {
        if ($this->db->transactionLevel() !== 0) {
            throw new RuntimeException('MCP admission needs a dedicated connection outside application transactions.');
        }
        if (! in_array($this->db->getDriverName(), ['pgsql', 'sqlite'], true)) {
            throw new RuntimeException('MCP durable admission requires PostgreSQL (SQLite is for local tests).');
        }
    }

    /** Database time, overridable by a deterministic test clock. */
    protected function nowMs(): int
    {
        $sql = $this->db->getDriverName() === 'pgsql'
            ? 'SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS ms'
            : "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) AS ms";
        $row = $this->db->selectOne($sql, [], false);
        if (! $row instanceof stdClass) {
            throw new RuntimeException('MCP database clock is unavailable.');
        }

        return self::integer($row->ms);
    }

    private function window(int $now): int
    {
        $width = $this->settings->source->window * 1000;

        return intdiv($now, $width) * $width;
    }

    private function state(): Builder
    {
        return $this->db->table('signals_mcp_sources')->useWritePdo()->where('id', $this->source);
    }

    private function table(string $name): Builder
    {
        return $this->db->table('signals_mcp_'.$name)->useWritePdo()->where('source', $this->source);
    }

    /** SQL drivers may return integer columns and aggregate counts as strings. */
    private static function integer(mixed $value): int
    {
        if (is_string($value) && ctype_digit($value) && strlen($value) <= 16) {
            $value = (int) $value;
        }
        if (! is_int($value) || $value < 0 || $value > 9007199254740991) {
            throw new RuntimeException('Invalid MCP journal integer.');
        }

        return $value;
    }

    /** Runs under the shared privacy lock, also when restored payloads are encountered. */
    public static function purgeErased(ConnectionInterface $db, ErasedActors $erased): void
    {
        if ($erased->empty()) {
            return;
        }
        $before = self::integer($db->table('signals_mcp_guard')->where('id', 1)->value('anonymous_before_ms'));
        foreach (['attempts', 'parts'] as $table) {
            $name = 'signals_mcp_'.$table;
            $column = $table === 'attempts' ? 'window_ms' : 'start_ms';
            // Ownership fingerprints are committed with each payload. Query the
            // tombstones rather than decoding every retained payload per dispatch.
            $rows = $db->table($name)->whereExists(function (Builder $q) use ($name): void {
                $q->selectRaw('1')->from('signals_mcp_erasures')
                    ->whereColumn('fingerprint', $name.'.subject_hash')
                    ->orWhereColumn('fingerprint', $name.'.organization_hash')
                    ->orWhereColumn('fingerprint', $name.'.token_hash');
            })->orWhere(function (Builder $q) use ($before, $column): void {
                $q->whereNull('subject_hash')->whereNull('organization_hash')->whereNull('token_hash')->where($column, '<=', $before)->whereRaw('? > 0', [(int) ($before > 0)]);
            })->select(['source', $column])->distinct()->get();
            foreach ($rows as $row) {
                self::invalidate($db, Wire::text($row->source), self::integer($row->{$column}), 'erased');
            }
        }
    }

    /** Persistence boundary: these are erasure indexes, never public dimensions.
     * @param  array<string,mixed>  $actor
     * @return array<string,string|null>
     */
    private static function ownership(array $actor): array
    {
        $columns = [];
        foreach (['subject', 'organization', 'token'] as $name) {
            $value = $actor[$name] === null ? null : Wire::object($actor[$name]);
            $columns[$name.'_hash'] = $value === null ? null : ErasedActors::fingerprint(new Entity(Wire::text($value['namespace']), Wire::text($value['type']), Wire::text($value['id'])));
        }

        return $columns;
    }

    private static function invalidate(ConnectionInterface $db, string $source, int $start, string $reason): void
    {
        $db->table('signals_mcp_attempts')->where('source', $source)->where('window_ms', $start)->delete();
        $db->table('signals_mcp_parts')->where('source', $source)->where('start_ms', $start)->delete();
        $db->table('signals_mcp_windows')->where('source', $source)->where('start_ms', $start)->update(['reason' => $reason, 'reported' => false]);
        $db->table('signals_mcp_sources')->where('id', $source)->update(['frame' => null, 'acknowledged_frame' => null, 'committed' => 0, 'commit_unknown' => true]);
    }
}
