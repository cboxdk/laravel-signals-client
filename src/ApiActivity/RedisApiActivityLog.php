<?php

declare(strict_types=1);

namespace Cbox\Signals\ApiActivity;

use Cbox\Signals\Contracts\ApiActivityLog;
use Cbox\Signals\Contracts\SourceRecords;
use Cbox\Signals\Exceptions\SignalsFailure;
use Cbox\Signals\Support\Wire;
use Illuminate\Contracts\Redis\Factory;
use Illuminate\Redis\Connections\PhpRedisConnection;
use UnexpectedValueException;

/**
 * Counts API calls in Redis (or Valkey), aggregated per window, and sends the
 * closed windows to Signals. The application database is never touched.
 *
 * A call is one script call that adds it to counters, so memory follows the
 * number of actors, operations and references, never the number of requests.
 * Every window is split into `shards` key spaces by actor; all keys of one shard
 * share a hash tag, so a cluster keeps each shard on one node.
 *
 * Each shard counts into a numbered part. Closing seals the part in one atomic
 * step, after which calls go to the next part; so a call is in exactly one part,
 * and a call that arrives after its window closed becomes a later part that
 * Signals adds to the first. A sealed part stays in Redis until Signals has
 * accepted its records; records are rebuilt identically, with the same ids and
 * positions, if sending is retried.
 *
 * Bounds per window and shard: `tracked_actors / shards` actors are counted one
 * by one and every later one per network context (64 networks, then one per
 * enrichment version); 32 operations per actor (later ones are counted as
 * omitted); 256 references per operation and 768 per actor (the smallest kept,
 * the rest counted as overflow); the first 1024 calls per actor for the order.
 */
final class RedisApiActivityLog implements ApiActivityLog
{
    public const int OPERATIONS = 32;

    public const int CALLS = 1024;

    public const int NETWORKS = 64;

    /** Keys expire after a week, so a log nobody closes cannot grow without end. */
    public const int TTL = 604800;

    private const array RETRY = [408, 429, 500, 502, 503, 504];

    private const string RECORD = <<<'LUA'
local part = redis.call('HGET', KEYS[1], 'part') or '1'
local p = ARGV[1] .. ':' .. part .. ':'
local ttl = tonumber(ARGV[17])
local actors, counts = p .. 'a', p .. 'c'
local a = ARGV[2]
local person = true
if redis.call('HEXISTS', actors, a) == 0 then
  if tonumber(redis.call('HGET', counts, '#a') or '0') < tonumber(ARGV[15]) then
    redis.call('HSET', actors, a, ARGV[3])
    redis.call('HINCRBY', counts, '#a', 1)
  else
    person = false
    a = ARGV[4]
    if redis.call('HEXISTS', actors, a) == 0 then
      if tonumber(redis.call('HGET', counts, '#g') or '0') < tonumber(ARGV[16]) then
        redis.call('HSET', actors, a, ARGV[5])
        redis.call('HINCRBY', counts, '#g', 1)
      else
        a = ARGV[6]
        redis.call('HSETNX', actors, a, ARGV[7])
      end
    end
    local h = p .. 'h:' .. a
    if redis.call('PFADD', h, ARGV[2]) == 1 then redis.call('EXPIRE', h, ttl) end
  end
end
if redis.call('HINCRBY', counts, a .. ':t', 1) == 1 then
  redis.call('EXPIRE', actors, ttl)
  redis.call('EXPIRE', counts, ttl)
end
local op = ARGV[8]
local offset = tonumber(ARGV[10])
local f = a .. ':' .. op .. ':'
local first = redis.call('HGET', counts, f .. 'f')
local known = true
if not first then
  if tonumber(redis.call('HGET', counts, a .. ':n') or '0') < 32 then
    redis.call('HINCRBY', counts, a .. ':n', 1)
    redis.call('HSET', counts, f .. 'f', offset)
    if redis.call('HSETNX', p .. 'o', op, ARGV[9]) == 1 then redis.call('EXPIRE', p .. 'o', ttl) end
  else
    known = false
  end
elseif tonumber(first) > offset then
  redis.call('HSET', counts, f .. 'f', offset)
end
if not known then
  redis.call('HINCRBY', counts, a .. ':x', 1)
  op = '*'
else
  redis.call('HINCRBY', counts, f .. ARGV[11], 1)
  if ARGV[12] ~= '' then redis.call('HINCRBY', counts, f .. ARGV[12], 1) end
  local ref = ARGV[13]
  if ref ~= '' then
    if ARGV[14] == '1' then redis.call('HINCRBY', counts, f .. 'x', 1) end
    local rc = p .. 'rc'
    if redis.call('HEXISTS', rc, f .. ref) == 1 then
      redis.call('HINCRBY', rc, f .. ref, 1)
    else
      local refs = p .. 'r:' .. a .. ':' .. op
      local card = redis.call('ZCARD', refs)
      local kept = tonumber(redis.call('HGET', counts, a .. ':R') or '0')
      if card < 256 and kept < 768 then
        redis.call('ZADD', refs, 0, ref)
        redis.call('HSET', rc, f .. ref, 1)
        redis.call('HINCRBY', counts, a .. ':R', 1)
        if card == 0 then
          redis.call('EXPIRE', refs, ttl)
          redis.call('EXPIRE', rc, ttl)
        end
      else
        local top = nil
        if card > 0 then top = redis.call('ZREVRANGEBYLEX', refs, '+', '-', 'LIMIT', 0, 1)[1] end
        if top and ref < top then
          redis.call('ZREM', refs, top)
          redis.call('ZADD', refs, 0, ref)
          local evicted = tonumber(redis.call('HGET', rc, f .. top) or '0')
          redis.call('HDEL', rc, f .. top)
          redis.call('HSET', rc, f .. ref, 1)
          redis.call('HINCRBY', counts, f .. 'v', evicted)
        else
          redis.call('HINCRBY', counts, f .. 'v', 1)
        end
      end
    end
  end
end
if person then
  local q = p .. 'q:' .. a
  redis.call('ZADD', q, offset, string.format('%07d', offset) .. ':' .. ARGV[18] .. ':' .. op)
  local n = redis.call('ZCARD', q)
  if n == 1 then
    redis.call('EXPIRE', q, ttl)
  elseif n > 1024 then
    redis.call('ZPOPMAX', q)
  end
end
return 1
LUA;

    private const string SEAL = <<<'LUA'
local current = tonumber(redis.call('HGET', KEYS[1], 'part') or '1')
local part = tonumber(ARGV[1])
if current == part then redis.call('HSET', KEYS[1], 'part', part + 1) end
redis.call('HSETNX', KEYS[1], 'sealed:' .. part, ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return redis.call('HGET', KEYS[1], 'sealed:' .. part)
LUA;

    private const string UNLOCK = <<<'LUA'
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
LUA;

    private const array CLASSES = ['s' => 'success', 'r' => 'redirect', 'c' => 'client_error', 'e' => 'server_error', 'd' => 'denied', 'n' => 'not_found', 'l' => 'rate_limited'];

    public function __construct(private readonly Factory $redis, private readonly ApiActivitySettings $settings) {}

    public function record(ApiCall $call): void
    {
        $actorJson = self::json($call->actor);
        $actorHash = hash('sha256', $actorJson);
        $network = ['subject' => null, 'organization' => null, 'token' => null, 'ip_reference' => null, 'context' => WindowBuilder::network($call->actor)];
        $networkJson = self::json($network);
        $enrichment = $network['context']['enrichment_version'] ?? 'unenriched';
        $fallback = ['subject' => null, 'organization' => null, 'token' => null, 'ip_reference' => null, 'context' => (new ApiContext(enrichmentVersion: is_string($enrichment) ? $enrichment : 'unenriched'))->toArray()];
        $fallbackJson = self::json($fallback);
        $status = $call->status;
        $class = $status >= 500 ? 'e' : ($status >= 400 ? 'c' : ($status >= 300 ? 'r' : 's'));
        $part = match ($status) {
            401, 403 => 'd',
            404 => 'n',
            429 => 'l',
            default => '',
        };
        $operation = $call->method.' '.$call->route;
        $base = $this->base($call->windowStart, $call->windowSeconds, (int) hexdec(substr($actorHash, 0, 7)) % $this->settings->shards);
        $this->script(self::RECORD, [$base.':m'], [
            $base,
            'a'.substr($actorHash, 0, 19),
            $actorJson,
            'g'.substr(hash('sha256', $networkJson), 0, 19),
            $networkJson,
            'f'.substr(hash('sha256', $fallbackJson), 0, 19),
            $fallbackJson,
            substr(hash('sha256', $operation), 0, 12),
            $operation,
            (string) $call->offsetMs,
            $class,
            $part,
            $status < 400 && $call->reference !== null ? $call->reference : '',
            $call->conflict ? '1' : '0',
            (string) (int) ceil($this->settings->trackedActors / $this->settings->shards),
            (string) self::NETWORKS,
            (string) self::TTL,
            bin2hex(random_bytes(4)),
        ]);
    }

    /**
     * Seal the windows that ended at least the grace period before `$now`, and the
     * later parts of windows inside the lookback, send what is sealed in order, and
     * then send a coverage frame (see `cover`).
     *
     * @return array{parts:int,records:int,coverage:?string} parts finished, records Signals accepted,
     *                                                       and the frame's completeness (null when no frame was sent)
     */
    public function flush(SourceRecords $source, int $now): array
    {
        $this->settings->sendable();
        $began = time();
        $lock = $this->settings->prefix.':lock';
        $token = bin2hex(random_bytes(16));
        if ($this->raw(['SET', $lock, $token, 'NX', 'EX', '300']) === false || $this->raw(['GET', $lock]) !== $token) {
            return ['parts' => 0, 'records' => 0, 'coverage' => null];
        }
        try {
            $this->discover($now);
            $result = $this->sendPending($source, $now);

            return [
                'parts' => $result['parts'],
                'records' => $result['records'],
                'coverage' => $this->cover($source, $now, $now + max(0, time() - $began), $result['refused']),
            ];
        } finally {
            $this->script(self::UNLOCK, [$lock], [$token]);
        }
    }

    /**
     * Send one coverage frame: Signals has every record whose window ends by `through`.
     *
     * An `api.activity` record counts in the window it closes, so `through` stays a
     * second short of the end of every window Signals has not accepted yet: the window
     * still open or in its grace, and any sealed part still waiting to be sent. Once
     * all of a window's parts are accepted, or it closed without calls, `through` moves
     * on to now. It never moves backwards, so a failing send holds it and the source
     * turns stale, as a dead sender does.
     *
     * A complete frame repeats its claim from the start of the lookback, so a window is
     * complete again in the next frame after a late part was accepted. A late part that
     * cannot be sent yet, or a record Signals refused, makes the frame partial from that
     * window on; complete frames never again reach back over a window with a refused record.
     *
     * @param  array<int,int>  $refused  records Signals refused in this run, by window start
     */
    private function cover(SourceRecords $source, int $now, int $emitted, array $refused): ?string
    {
        $key = $this->coverageKey();
        $state = self::pairs($this->raw(['HGETALL', $key]));
        if ($emitted <= (int) ($state['emitted'] ?? 0)) {
            return null; // another run already sent this second's frame
        }
        $seconds = $this->settings->window;
        // Windows ending by $closed are sealed; the one after it may still be counting.
        $closed = intdiv($now - $this->settings->grace, $seconds) * $seconds;
        $candidate = min($now, $closed + $seconds - 1);
        $previous = isset($state['through']) ? (int) $state['through'] : null;
        $suspect = null;
        $late = false;
        $members = $this->raw(['ZRANGE', $this->settings->prefix.':pending', '0', '-1']);
        foreach (is_array($members) ? $members : [] as $member) {
            [$start, $length] = array_map(intval(...), explode(':', self::text($member)));
            $candidate = min($candidate, $start + $length - 1);
            if ($previous !== null && $start + $length <= $previous) {
                $suspect = min($suspect ?? $start, $start); // a late part of a window already claimed
                $late = true;
            }
        }
        $floor = (int) ($state['floor'] ?? 0);
        foreach (array_keys($refused) as $start) {
            $suspect = min($suspect ?? $start, $start);
            $floor = max($floor, $start + $seconds);
        }
        $through = max($previous ?? $candidate, $candidate);
        $origin = (int) ($state['origin'] ?? $through);
        [$commit, $exact] = $this->commit($state);

        $reasons = [];
        if ($late) {
            $reasons[] = 'late_records_pending';
        }
        if ($refused !== []) {
            $reasons[] = 'records_refused';
        }
        if (! $exact) {
            $reasons[] = 'commit_unknown';
        }
        $from = $suspect !== null
            ? max($origin, $suspect)
            : max($origin, $floor, intdiv($now - $this->settings->lookback, $seconds) * $seconds);
        $from = min($from, $through);
        $completeness = $reasons === [] ? 'complete' : 'partial';
        // Claimed before it is sent: a retry never claims less, nor reuses a second.
        $this->raw(['HSET', $key, 'origin', (string) $origin, 'through', (string) $through, 'emitted', (string) $emitted, 'floor', (string) $floor]);
        $this->raw(['EXPIRE', $key, (string) self::TTL]);
        $iso = static fn (int $at): string => gmdate('Y-m-d\TH:i:s\Z', $at);
        $frame = [
            'contract_version' => '2.0.0',
            'kind' => 'coverage',
            'scope' => (array) $this->settings->scope,
            'position' => ['source_id' => $this->settings->sourceId, 'partition' => $this->settings->partition, 'generation' => $this->settings->generation, 'sequence' => (string) $commit],
            'emitted_at' => $iso($emitted),
            'event_time' => [
                'status' => $completeness,
                'from' => $iso($from),
                'through' => $iso($through),
                'expected_cadence_seconds' => 60,
                'lag_seconds' => $emitted - $through,
                'reasons' => $reasons,
            ],
            'commit_through' => (string) $commit,
            'expected_families' => ['api.activity'],
            'observed_families' => ['api.activity'],
            'omitted_records' => array_sum($refused),
            'completeness' => $completeness,
            'reason_codes' => $reasons,
        ];
        try {
            $source->send($this->settings->sourceId, [$frame]);
        } catch (SignalsFailure $failure) {
            if (! in_array($failure->status, self::RETRY, true)) {
                if ($failure->status === 422) {
                    // Perhaps a commit position Signals does not have: claim none until a record is accepted.
                    $this->raw(['HSET', $key, 'commit', '0', 'exact', '0']);
                }
                $failed = $this->settings->prefix.':failed';
                $this->raw(['LPUSH', $failed, self::json(['error_code' => $failure->errorCode, 'record' => $frame])]);
                $this->raw(['LTRIM', $failed, '0', '999']);
            }

            return null;
        }

        return $completeness;
    }

    /**
     * The highest position Signals accepted, and whether it is known exactly. Before the
     * first frame it is the last position reserved, when nothing reserved is still unsent.
     *
     * @param  array<string,string>  $state
     * @return array{0:int,1:bool}
     */
    private function commit(array $state): array
    {
        if (isset($state['commit'])) {
            return [(int) $state['commit'], ($state['exact'] ?? '0') === '1'];
        }
        $members = $this->raw(['ZRANGE', $this->settings->prefix.':pending', '0', '-1']);
        foreach (is_array($members) ? $members : [] as $member) {
            [$start, $seconds, $shard, $part] = array_map(intval(...), explode(':', self::text($member)));
            if ($this->raw(['HEXISTS', $this->base($start, $seconds, $shard).':m', 'seq:'.$part]) === 1) {
                return [0, false];
            }
        }
        $reserved = $this->raw(['GET', $this->positions()]);
        $commit = is_numeric($reserved) ? (int) $reserved : 0;
        $this->raw(['HSET', $this->coverageKey(), 'commit', (string) $commit, 'exact', '1']);

        return [$commit, true];
    }

    /** Note the highest position Signals accepted; it is then known exactly. */
    private function accepted(int $sequence): void
    {
        $state = self::pairs($this->raw(['HGETALL', $this->coverageKey()]));
        if (! isset($state['commit']) || ($state['exact'] ?? '0') !== '1' || (int) $state['commit'] < $sequence) {
            $this->raw(['HSET', $this->coverageKey(), 'commit', (string) $sequence, 'exact', '1']);
        }
    }

    private function coverageKey(): string
    {
        return $this->settings->prefix.':coverage:'.$this->settings->generation.':'.$this->settings->partition;
    }

    private function positions(): string
    {
        return $this->settings->prefix.':positions:'.$this->settings->generation.':'.$this->settings->partition;
    }

    /** Note every window part with calls that is ready to close. */
    private function discover(int $now): void
    {
        $seconds = $this->settings->window;
        $last = intdiv($now - $this->settings->grace, $seconds) * $seconds - $seconds;
        for ($start = intdiv($now - $this->settings->lookback, $seconds) * $seconds; $start <= $last; $start += $seconds) {
            for ($shard = 0; $shard < $this->settings->shards; $shard++) {
                $base = $this->base($start, $seconds, $shard);
                $part = self::integer($this->raw(['HGET', $base.':m', 'part']) ?: 1);
                if (self::integer($this->raw(['EXISTS', $base.':'.$part.':c'])) === 1) {
                    $this->raw(['ZADD', $this->settings->prefix.':pending', 'NX', (string) $start, sprintf('%d:%d:%d:%06d', $start, $seconds, $shard, $part)]);
                }
            }
        }
    }

    /** @return array{parts:int,records:int,refused:array<int,int>} */
    private function sendPending(SourceRecords $source, int $now): array
    {
        $pending = $this->settings->prefix.':pending';
        $result = ['parts' => 0, 'records' => 0, 'refused' => []];
        $members = $this->raw(['ZRANGE', $pending, '0', '-1']);
        foreach (is_array($members) ? $members : [] as $member) {
            [$start, $seconds, $shard, $part] = array_map(intval(...), explode(':', self::text($member)));
            $base = $this->base($start, $seconds, $shard);
            $retry = $this->raw(['HGET', $base.':m', 'retry:'.$part]);
            if (is_string($retry) && (int) explode(':', $retry)[1] > $now) {
                break; // keep positions in order: nothing after a part that must wait
            }
            $sealedAt = self::integer($this->script(self::SEAL, [$base.':m'], [(string) $part, (string) $now, (string) self::TTL]));
            [$builder, $keys] = $this->read($base, $part, $start, $seconds);
            $records = [];
            if (! $builder->empty()) {
                $label = "s{$shard}p{$part}";
                $count = count($builder->windows());
                $records = $builder->records($label, $this->reserve($base, $part, $count), $sealedAt);
            }
            $sent = $this->send($source, $records);
            if ($sent === null) {
                $attempts = is_string($retry) ? (int) explode(':', $retry)[0] + 1 : 1;
                $this->raw(['HSET', $base.':m', 'retry:'.$part, $attempts.':'.($now + min(3600, 5 * 2 ** min($attempts, 10)))]);

                break;
            }
            foreach (array_chunk($keys, 500) as $chunk) {
                $this->raw(['UNLINK', ...$chunk]);
            }
            $this->raw(['HDEL', $base.':m', 'retry:'.$part, 'seq:'.$part, 'sealed:'.$part]);
            $this->raw(['ZREM', $pending, self::text($member)]);
            $result['parts']++;
            $result['records'] += $sent;
            if ($sent < count($records)) {
                $result['refused'][$start] = ($result['refused'][$start] ?? 0) + count($records) - $sent;
            }
        }

        return $result;
    }

    /** The first position of a part's records, reserved once so a retry sends the same positions. */
    private function reserve(string $base, int $part, int $count): int
    {
        $reserved = $this->raw(['HGET', $base.':m', 'seq:'.$part]);
        if (is_string($reserved) || is_int($reserved)) {
            return self::integer($reserved);
        }
        $end = self::integer($this->raw(['INCRBY', $this->positions(), (string) $count]));
        $this->raw(['HSETNX', $base.':m', 'seq:'.$part, (string) ($end - $count + 1)]);

        return self::integer($this->raw(['HGET', $base.':m', 'seq:'.$part]));
    }

    /**
     * Send records in pages. A refused page is resent record by record and a refused
     * record is kept in the `failed` list. Returns the records accepted, or null to retry later.
     * The highest accepted position is noted for the coverage frame.
     *
     * @param  list<array<string,mixed>>  $records
     */
    private function send(SourceRecords $source, array $records): ?int
    {
        $sent = 0;
        foreach ($this->pages($records) as $page) {
            try {
                $source->send($this->settings->sourceId, $page);
                $sent += count($page);
                $this->accepted(self::integer(Wire::object($page[count($page) - 1]['position'])['sequence'] ?? null)); // positions ascend in a page

                continue;
            } catch (SignalsFailure $failure) {
                if (in_array($failure->status, self::RETRY, true)) {
                    return null;
                }
            }
            foreach ($page as $record) {
                try {
                    $source->send($this->settings->sourceId, [$record]);
                    $sent++;
                    $this->accepted(self::integer(Wire::object($record['position'])['sequence'] ?? null));
                } catch (SignalsFailure $failure) {
                    if (in_array($failure->status, self::RETRY, true)) {
                        return null;
                    }
                    $failed = $this->settings->prefix.':failed';
                    $this->raw(['LPUSH', $failed, self::json(['error_code' => $failure->errorCode, 'record' => $record])]);
                    $this->raw(['LTRIM', $failed, '0', '999']);
                }
            }
        }

        return $sent;
    }

    /**
     * @param  list<array<string,mixed>>  $records
     * @return list<list<array<string,mixed>>>
     */
    private function pages(array $records): array
    {
        $pages = [];
        $page = [];
        $bytes = 16;
        foreach ($records as $record) {
            $size = strlen(self::json($record)) + 1;
            if ($page !== [] && (count($page) === 1000 || $bytes + $size > 1048576)) {
                $pages[] = $page;
                [$page, $bytes] = [[], 16];
            }
            $page[] = $record;
            $bytes += $size;
        }

        return $page === [] ? $pages : [...$pages, $page];
    }

    /**
     * Read one sealed part into a builder, and list every key it uses.
     *
     * @return array{0:WindowBuilder,1:list<string>}
     */
    private function read(string $base, int $part, int $start, int $seconds): array
    {
        $p = "{$base}:{$part}:";
        $actors = self::pairs($this->raw(['HGETALL', $p.'a']));
        $counts = self::pairs($this->raw(['HGETALL', $p.'c']));
        $names = self::pairs($this->raw(['HGETALL', $p.'o']));
        $kept = self::pairs($this->raw(['HGETALL', $p.'rc']));
        $keys = [$p.'a', $p.'c', $p.'o', $p.'rc'];
        $builder = new WindowBuilder(new ApiActivitySettings(
            true, $this->settings->referenceKey, $this->settings->referenceKeyId, $this->settings->applicationId, $this->settings->namespace,
            $this->settings->window, $this->settings->grace, $this->settings->actorsPerShard(), $this->settings->scope,
            $this->settings->sourceId, $this->settings->partition, $this->settings->generation,
        ), $start, $seconds);

        $references = [];
        foreach ($kept as $field => $count) {
            [$actor, $operation, $reference] = explode(':', $field, 3);
            $references[$actor][$operation][$reference] = self::integer($count);
        }
        foreach ($actors as $key => $json) {
            $actor = json_decode($json, true, flags: JSON_THROW_ON_ERROR);
            if (! is_array($actor)) {
                throw new UnexpectedValueException('Unreadable actor in Redis.');
            }
            /** @var array<string,mixed> $actor */
            $operations = [];
            foreach ($counts as $field => $first) {
                $parts = explode(':', $field);
                if (count($parts) !== 3 || $parts[0] !== $key || $parts[2] !== 'f') {
                    continue;
                }
                $id = $parts[1];
                $name = $names[$id] ?? throw new UnexpectedValueException('Operation without a name in Redis.');
                [$method, $route] = explode(' ', $name, 2);
                $responses = [];
                foreach (self::CLASSES as $code => $class) {
                    $responses[$class] = self::integer($counts["{$key}:{$id}:{$code}"] ?? 0);
                }
                /** @var array{success:int,redirect:int,client_error:int,server_error:int,denied:int,not_found:int,rate_limited:int} $responses */
                $operations[$name] = OperationTally::restore($method, $route, self::integer($first), $responses, $references[$key][$id] ?? [],
                    self::integer($counts["{$key}:{$id}:v"] ?? 0), self::integer($counts["{$key}:{$id}:x"] ?? 0));
                $keys[] = "{$p}r:{$key}:{$id}";
            }
            $omitted = self::integer($counts["{$key}:x"] ?? 0);
            if ($key[0] === 'a') {
                $calls = [];
                $sequence = $this->raw(['ZRANGE', "{$p}q:{$key}", '0', '-1']);
                foreach (is_array($sequence) ? $sequence : [] as $member) {
                    [$offset, , $id] = explode(':', self::text($member), 3);
                    $calls[] = [$id === '*' ? '*' : ($names[$id] ?? '*'), (int) $offset];
                }
                $keys[] = "{$p}q:{$key}";
                $builder->addTally($key, ActorTally::restore($actor, $operations, $omitted, $calls));
            } else {
                $builder->addFolded(ActorTally::restore($actor, $operations, $omitted, []), self::integer($this->raw(['PFCOUNT', "{$p}h:{$key}"])));
                $keys[] = "{$p}h:{$key}";
            }
        }

        return [$builder, $keys];
    }

    /** All keys of one shard of one window share the braced hash tag. */
    private function base(int $start, int $seconds, int $shard): string
    {
        return $this->settings->prefix.':{'.$start.':'.$seconds.':'.$shard.'}';
    }

    /**
     * One raw command, in the same form for phpredis and Predis.
     *
     * @param  list<string>  $command
     */
    private function raw(array $command): mixed
    {
        $connection = $this->redis->connection($this->settings->connection);

        return $connection instanceof PhpRedisConnection ? $connection->executeRaw($command) : $connection->__call('executeRaw', [$command]);
    }

    /**
     * EVALSHA, falling back to EVAL (which caches the script) the first time.
     *
     * @param  list<string>  $keys
     * @param  list<string>  $arguments
     */
    private function script(string $script, array $keys, array $arguments): mixed
    {
        $tail = [(string) count($keys), ...$keys, ...$arguments];
        $result = $this->raw(['EVALSHA', sha1($script), ...$tail]);
        if ($result === false || is_object($result) || (is_string($result) && str_starts_with($result, 'NOSCRIPT'))) {
            $result = $this->raw(['EVAL', $script, ...$tail]);
        }
        if ($result === false || is_object($result)) {
            throw new UnexpectedValueException('Redis refused the api.activity script.');
        }

        return $result;
    }

    /** @return array<string,string> */
    private static function pairs(mixed $flat): array
    {
        $pairs = [];
        if (! is_array($flat)) {
            return $pairs;
        }
        $flat = array_values($flat);
        for ($i = 0; $i + 1 < count($flat); $i += 2) {
            $pairs[self::text($flat[$i])] = self::text($flat[$i + 1]);
        }

        return $pairs;
    }

    private static function json(mixed $value): string
    {
        return json_encode($value, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION | JSON_UNESCAPED_SLASHES);
    }

    private static function text(mixed $value): string
    {
        return is_string($value) || is_int($value) ? (string) $value : throw new UnexpectedValueException('Expected text from Redis.');
    }

    private static function integer(mixed $value): int
    {
        return is_numeric($value) ? (int) $value : throw new UnexpectedValueException('Expected a number from Redis.');
    }
}
