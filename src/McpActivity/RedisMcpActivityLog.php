<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

use Cbox\Signals\Contracts\McpActivityBuffers;
use Cbox\Signals\Contracts\McpActivityLog;
use Cbox\Signals\Contracts\SourceRecords;
use Cbox\Signals\Exceptions\SignalsFailure;
use Cbox\Signals\Support\Wire;
use Illuminate\Contracts\Redis\Factory;
use Illuminate\Redis\Connections\PhpRedisConnection;
use UnexpectedValueException;

/**
 * Bounded dispatch journal. No lookback scan: pending attempts stay discoverable
 * until sent, erased or their bounded retention expires. Fail-open dispatch
 * cannot prove source completeness: heartbeats explicitly report unknown or
 * known partial coverage, never infer complete from an empty queue.
 */
class RedisMcpActivityLog implements McpActivityLog
{
    private const string START = <<<'LUA'
local now=tonumber(redis.call('TIME')[1])
local expires=math.min(now,math.floor(tonumber(ARGV[3])/1000))+tonumber(ARGV[5])
if expires<=now then redis.call('HINCRBY',KEYS[3],'expired_attempts',1); return 0 end
if redis.call('HLEN',KEYS[1])+redis.call('LLEN',KEYS[4]) >= tonumber(ARGV[4]) then
 redis.call('HINCRBY',KEYS[3],'refused',1); return 0
end
local ticket=redis.call('HINCRBY',KEYS[3],'ticket',1)
redis.call('HSET',KEYS[1],ARGV[1],ARGV[2])
redis.call('HSET',KEYS[2],ARGV[1],ticket)
redis.call('ZADD',KEYS[5],expires,ARGV[1])
for _,key in ipairs({KEYS[1],KEYS[2],KEYS[5]}) do
 redis.call('EXPIREAT',key,math.max(expires,now+redis.call('TTL',key)))
end
return ticket
LUA;

    private const string FINISH = <<<'LUA'
local raw=redis.call('HGET',KEYS[1],ARGV[1])
if not raw or redis.call('HEXISTS',KEYS[2],ARGV[1]) == 1 then return 0 end
local attempt=cjson.decode(raw)
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
local expired=now >= attempt.started+tonumber(ARGV[4])
if ARGV[2]=='unknown' and not expired then return 0 end
local outcome=ARGV[2]
local replay=ARGV[3]
if expired then outcome='unknown'; replay='0' end
redis.call('HSET',KEYS[2],ARGV[1],outcome..':'..replay)
redis.call('EXPIRE',KEYS[2],redis.call('TTL',KEYS[1]))
if outcome=='unknown' then redis.call('HINCRBY',KEYS[3],'unknown_outcomes',1) end
return 1
LUA;

    private const string PRUNE = <<<'LUA'
local now=tonumber(redis.call('TIME')[1])
for _,id in ipairs(redis.call('ZRANGEBYSCORE',KEYS[6],'-inf',now)) do
 redis.call('HDEL',KEYS[1],id); redis.call('HDEL',KEYS[2],id); redis.call('HDEL',KEYS[3],id)
 redis.call('ZREM',KEYS[6],id); redis.call('HINCRBY',KEYS[4],'expired_attempts',1)
end
if ARGV[1]=='0' then return 1 end
local ttl=redis.call('TTL',KEYS[5])
for i=1,redis.call('LLEN',KEYS[5]) do
 local raw=redis.call('LPOP',KEYS[5])
 if cjson.decode(raw).expires<=now then redis.call('HINCRBY',KEYS[4],'expired_records',1)
 else redis.call('RPUSH',KEYS[5],raw) end
end
if ttl>=0 then redis.call('EXPIRE',KEYS[5],ttl) end
return 1
LUA;

    private const string SEAL = <<<'LUA'
local ids=cjson.decode(ARGV[1])
local now=tonumber(redis.call('TIME')[1])
local expires=now+tonumber(ARGV[3])
for _,id in ipairs(ids) do
 if redis.call('HEXISTS',KEYS[1],id)==0 or redis.call('HEXISTS',KEYS[2],id)==0 then return 0 end
 local deadline=tonumber(redis.call('ZSCORE',KEYS[6],id))
 if not deadline or deadline<=now then return 0 end
 expires=math.min(expires,deadline)
end
local n=redis.call('HINCRBY',KEYS[4],'position',1)
local record=string.gsub(ARGV[2],'"sequence":"__mcp_position__"','"sequence":"'..string.format('%.0f',n)..'"',1)
local ttl=redis.call('TTL',KEYS[5])
redis.call('RPUSH',KEYS[5],'{"expires":'..expires..',"record":'..record..'}')
redis.call('EXPIREAT',KEYS[5],math.max(expires,now+ttl))
for _,id in ipairs(ids) do
 redis.call('HDEL',KEYS[1],id); redis.call('HDEL',KEYS[2],id); redis.call('HDEL',KEYS[3],id)
 redis.call('ZREM',KEYS[6],id)
end
return 1
LUA;

    public function __construct(private readonly Factory $redis, public readonly Settings $settings, private readonly McpActivityBuffers $buffers) {}

    public function start(Attempt $attempt): ?string
    {
        if (! $this->settings->source->enabled) {
            return null;
        }

        return $this->buffers->protect($this->settings, fn (ErasedActors $erased): ?string => $this->register($attempt, $erased));
    }

    private function register(Attempt $attempt, ErasedActors $erased): ?string
    {
        $this->checkIdentity();
        $window = $this->settings->source->window * 1000;
        if ($erased->contains(Wire::object(json_decode($attempt->actorJson, true, flags: JSON_THROW_ON_ERROR)), intdiv($attempt->startedMs, $window) * $window)) {
            $this->command(['HINCRBY', $this->key('state'), 'erased_attempts', '1']);

            return null;
        }
        $this->prune(sealed: false);
        $json = $attempt->json();
        if (strlen($json) > 16384) {
            throw new UnexpectedValueException('MCP dispatch metadata exceeds journal limit.');
        }
        $id = bin2hex(random_bytes(16));
        $n = $this->eval(self::START, ['attempts', 'order', 'state', 'sealed', 'expires'], [$id, $json, (string) $attempt->startedMs, (string) $this->settings->capacity, (string) $this->settings->retentionSeconds]);

        return $n === 0 ? null : $id;
    }

    public function finish(string $id, Outcome $outcome, bool $replayed = false): void
    {
        $this->buffers->protect($this->settings, function () use ($id, $outcome, $replayed): void {
            $this->finalize($id, $outcome, $replayed);
        });
    }

    private function finalize(string $id, Outcome $outcome, bool $replayed = false): void
    {
        $this->checkIdentity();
        $this->eval(self::FINISH, ['attempts', 'outcomes', 'state'], [$id, $outcome->value, $replayed ? '1' : '0', (string) $this->settings->timeoutMs]);
    }

    /** Sends a bounded batch, retries identical bytes after uncertain delivery. */
    public function flush(SourceRecords $sender): int
    {
        $this->settings->source->sendable();

        return $this->buffers->protect($this->settings, fn (): int => $this->deliver($sender));
    }

    private function deliver(SourceRecords $sender): int
    {
        $source = $this->settings->source;
        $source->sendable();
        $this->checkIdentity();
        $this->prune();
        $raw = $this->pairs($this->command(['HGETALL', $this->key('attempts')]));
        $outcomes = $this->pairs($this->command(['HGETALL', $this->key('outcomes')]));
        $order = $this->pairs($this->command(['HGETALL', $this->key('order')]));
        $now = time();
        /** @var array<string,Window> $windows */
        $windows = [];
        /** @var array<string,list<string>> $ids */
        $ids = [];
        foreach ($raw as $id => $json) {
            $attempt = Attempt::fromJson($json);
            if (! isset($outcomes[$id])) {
                $this->finalize($id, Outcome::Unknown);
                $value = $this->command(['HGET', $this->key('outcomes'), $id]);
                if (! is_string($value)) {
                    continue;
                }
                $outcomes[$id] = $value;
            }
            $start = intdiv($attempt->startedMs, $source->window * 1000) * $source->window;
            if ($start + $source->window > $now || ! isset($order[$id])) {
                continue;
            }
            $group = $start.':'.hash('sha256', $attempt->actorJson);
            $windows[$group] ??= new Window($this->settings, $start, $attempt->actorJson);
            [$outcome, $replayed] = explode(':', $outcomes[$id]);
            $windows[$group]->add(new FinalizedAttempt($attempt, Outcome::from($outcome), $replayed === '1', (int) $order[$id]));
            $ids[$group][] = $id;
        }
        foreach ($windows as $group => $window) {
            $record = $window->record('mcp-'.bin2hex(random_bytes(16)), $now);
            $this->eval(self::SEAL, ['attempts', 'outcomes', 'order', 'state', 'sealed', 'expires'], [json_encode($ids[$group], JSON_THROW_ON_ERROR), json_encode($record, JSON_THROW_ON_ERROR), (string) $this->settings->retentionSeconds]);
        }
        $sent = 0;
        for ($i = 0; $i < 100; $i++) {
            $record = $this->command(['LINDEX', $this->key('sealed'), '0']);
            if (! is_string($record)) {
                break;
            }
            $entry = Wire::object(json_decode($record, true, flags: JSON_THROW_ON_ERROR));
            if (Wire::integer($entry['expires']) <= time()) {
                $this->prune();

                continue;
            }
            $receipt = $sender->send($source->sourceId, [Wire::object($entry['record'])]);
            if ($receipt->accepted + $receipt->duplicates !== 1) {
                throw new UnexpectedValueException('MCP source did not acknowledge its record.');
            }
            // A second flusher may have sent the same head. Only its exact head
            // can be popped; it must never remove the next sender's record.
            $this->eval("if redis.call('LINDEX',KEYS[1],0)==ARGV[1] then local r=cjson.decode(ARGV[1]).record; redis.call('LPOP',KEYS[1]); redis.call('HSET',KEYS[2],'commit',r.position.sequence); redis.call('HDEL',KEYS[2],'commit_unknown'); redis.call('HINCRBY',KEYS[2],'omitted_attempts',r.data.omitted_attempts); return 1 end return 0", ['sealed', 'state'], [$record]);
            $sent++;
        }
        $this->cover($sender);

        return $sent;
    }

    /** Expiry counters are lower bounds: Redis TTL can remove dormant payloads first.
     * @return array{pending:int,sealed:int,refused:int,expired_attempts:int,expired_records:int,erased_attempts:int,erased_records:int,coverage:string}
     */
    public function status(): array
    {
        $refused = $this->command(['HGET', $this->key('state'), 'refused']);

        $expiredAttempts = $this->command(['HGET', $this->key('state'), 'expired_attempts']);
        $expiredRecords = $this->command(['HGET', $this->key('state'), 'expired_records']);

        $state = $this->pairs($this->command(['HGETALL', $this->key('state')]));

        return ['pending' => self::integer($this->command(['HLEN', $this->key('attempts')])), 'sealed' => self::integer($this->command(['LLEN', $this->key('sealed')])), 'refused' => $refused === false || $refused === null ? 0 : self::integer($refused), 'expired_attempts' => $expiredAttempts === false || $expiredAttempts === null ? 0 : self::integer($expiredAttempts), 'expired_records' => $expiredRecords === false || $expiredRecords === null ? 0 : self::integer($expiredRecords), 'erased_attempts' => (int) ($state['erased_attempts'] ?? 0), 'erased_records' => (int) ($state['erased_records'] ?? 0), 'coverage' => $state['coverage'] ?? 'unknown'];
    }

    /** Acknowledged progress, not a promise of lossless dispatch registration. */
    private function cover(SourceRecords $sender): void
    {
        $state = $this->pairs($this->command(['HGETALL', $this->key('state')]));
        $now = time();
        if ($now <= max((int) ($state['coverage_emitted'] ?? 0), (int) ($state['coverage_refused_at'] ?? 0))) {
            return;
        }
        $pending = $state['coverage_pending'] ?? null;
        $frame = $pending === null ? null : Wire::object(json_decode($pending, true, flags: JSON_THROW_ON_ERROR));
        // An old unknown/partial frame cannot satisfy the engine's fresh-heartbeat
        // rule. Replace it, without ever upgrading its claim to complete.
        if ($frame !== null && strtotime(Wire::text($frame['emitted_at'])) < $now - 90) {
            $frame = null;
        }
        if ($frame === null) {
            $reasons = ['collector_fail_open'];
            if (isset($state['commit_unknown'])) {
                $reasons[] = 'commit_unknown';
            }
            $partial = false;
            foreach (['refused', 'expired_attempts', 'expired_records', 'erased_attempts', 'erased_records', 'unknown_outcomes', 'omitted_attempts'] as $reason) {
                if ((int) ($state[$reason] ?? 0) > 0) {
                    $reasons[] = $reason;
                    $partial = true;
                }
            }
            $status = $this->status();
            if ($status['pending'] > 0 || $status['sealed'] > 0) {
                $reasons[] = 'records_pending';
            }
            $source = $this->settings->source;
            $commit = $state['commit'] ?? '0';
            $iso = static fn (int $at): string => gmdate('Y-m-d\TH:i:s\Z', $at);
            $frame = ['contract_version' => '2.0.0', 'kind' => 'coverage', 'scope' => $source->scope,
                'position' => ['source_id' => $source->sourceId, 'partition' => $source->partition, 'generation' => $source->generation, 'sequence' => $commit],
                'emitted_at' => $iso($now), 'event_time' => ['status' => $partial ? 'partial' : 'unsupported',
                    'from' => $iso($now - $this->settings->retentionSeconds), 'through' => $iso($now), 'expected_cadence_seconds' => 60, 'lag_seconds' => 0, 'reasons' => $reasons],
                'commit_through' => $commit, 'expected_families' => ['mcp.activity'], 'observed_families' => $commit === '0' ? [] : ['mcp.activity'],
                // Attempts are not records. Only known discarded sealed windows count here.
                'omitted_records' => (int) ($state['expired_records'] ?? 0) + (int) ($state['erased_records'] ?? 0),
                'completeness' => $partial ? 'partial' : 'unknown', 'reason_codes' => $reasons];
            $this->command(['HSET', $this->key('state'), 'coverage_pending', json_encode($frame, JSON_THROW_ON_ERROR)]);
        }
        try {
            $receipt = $sender->send($this->settings->source->sourceId, [$frame]);
        } catch (SignalsFailure $failure) {
            if ($failure->status === 422) {
                // Erasure/retention may remove the highest previously accepted
                // record. Retract the commit claim; retry at a new heartbeat time.
                $this->command(['HSET', $this->key('state'), 'commit', '0', 'commit_unknown', '1', 'coverage_refused_at', (string) $now]);
                $this->command(['HDEL', $this->key('state'), 'coverage_pending']);
            }
            throw $failure;
        }
        if ($receipt->accepted + $receipt->duplicates !== 1) {
            throw new UnexpectedValueException('MCP source did not acknowledge its coverage.');
        }
        $this->command(['HSET', $this->key('state'), 'coverage_emitted', (string) strtotime(Wire::text($frame['emitted_at'])), 'coverage', Wire::text($frame['completeness'])]);
        $this->command(['HDEL', $this->key('state'), 'coverage_pending']);
    }

    /** Called only while the durable buffer guard is locked, including before send. */
    public static function purgeErased(Factory $redis, string $connection, string $prefix, ErasedActors $erased, int $windowSeconds): void
    {
        if ($erased->empty()) {
            return;
        }
        $c = $redis->connection($connection);
        $raw = static fn (array $args): mixed => $c instanceof PhpRedisConnection ? $c->executeRaw($args) : $c->__call('executeRaw', [$args]);
        $attempts = Wire::items($raw(['HGETALL', $prefix.':attempts']));
        for ($i = 0; $i < count($attempts); $i += 2) {
            $id = Wire::text($attempts[$i]);
            $attempt = Attempt::fromJson(Wire::text($attempts[$i + 1]));
            $windowMs = $windowSeconds * 1000;
            if (! $erased->contains(Wire::object(json_decode($attempt->actorJson, true, flags: JSON_THROW_ON_ERROR)), intdiv($attempt->startedMs, $windowMs) * $windowMs)) {
                continue;
            }
            $raw(['EVAL', "local n=redis.call('HDEL',KEYS[1],ARGV[1]); redis.call('HDEL',KEYS[2],ARGV[1]); redis.call('HDEL',KEYS[3],ARGV[1]); redis.call('ZREM',KEYS[4],ARGV[1]); redis.call('HINCRBY',KEYS[5],'erased_attempts',n); return n", '5', $prefix.':attempts', $prefix.':outcomes', $prefix.':order', $prefix.':expires', $prefix.':state', $id]);
        }
        foreach (Wire::items($raw(['LRANGE', $prefix.':sealed', '0', '-1'])) as $value) {
            $json = Wire::text($value);
            $record = Wire::object(Wire::object(json_decode($json, true, flags: JSON_THROW_ON_ERROR))['record']);
            $data = Wire::object($record['data']);
            if ($erased->contains($data, (int) strtotime(Wire::text($data['window_start'])) * 1000)) {
                $raw(['EVAL', "local n=redis.call('LREM',KEYS[1],1,ARGV[1]); redis.call('HINCRBY',KEYS[2],'erased_records',n); return n", '2', $prefix.':sealed', $prefix.':state', $json]);
            }
        }
    }

    private function prune(bool $sealed = true): void
    {
        $this->eval(self::PRUNE, ['attempts', 'outcomes', 'order', 'state', 'sealed', 'expires'], [$sealed ? '1' : '0']);
    }

    private function key(string $suffix): string
    {
        return $this->settings->prefix().':'.$suffix;
    }

    private function checkIdentity(): void
    {
        if ($this->eval("redis.call('HSETNX',KEYS[1],'identity',ARGV[1]); if redis.call('HGET',KEYS[1],'identity')==ARGV[1] then return 1 end return 0", ['state'], [$this->settings->identity()]) !== 1) {
            throw new UnexpectedValueException('MCP source configuration changed; drain with its original configuration and use a new generation.');
        }
    }

    /**
     * @param  list<string>  $suffixes
     * @param  list<string>  $arguments
     */
    private function eval(string $script, array $suffixes, array $arguments): int
    {
        return self::integer($this->command(['EVAL', $script, (string) count($suffixes), ...array_map($this->key(...), $suffixes), ...$arguments]));
    }

    private static function integer(mixed $value): int
    {
        if (! is_int($value) && (! is_string($value) || ! ctype_digit($value))) {
            throw new UnexpectedValueException('Invalid MCP journal response.');
        }

        return (int) $value;
    }

    /** @param list<string> $command */
    private function command(array $command): mixed
    {
        $c = $this->redis->connection($this->settings->source->connection);

        return $c instanceof PhpRedisConnection ? $c->executeRaw($command) : $c->__call('executeRaw', [$command]);
    }

    /** @return array<string,string> */
    private function pairs(mixed $value): array
    {
        $items = Wire::items($value);
        $pairs = [];
        for ($i = 0; $i < count($items); $i += 2) {
            $pairs[Wire::text($items[$i])] = Wire::text($items[$i + 1]);
        }

        return $pairs;
    }
}
