<?php

declare(strict_types=1);

namespace Cbox\Signals\ApiActivity;

/**
 * The calls of one actor (or of the actors folded into one remainder) in one
 * window, and the `data` of the `api.activity` event they become.
 *
 * @internal
 */
final class ActorTally
{
    public const int OPERATIONS = 32;

    public const int WINDOW_REFERENCES = 768;

    public const int RUNS = 256;

    /** @var array<string,OperationTally> */
    private array $operations = [];

    public int $requests = 0;

    /** @var list<array{0:string,1:int,2:int}> operation key, offset of its first call, consecutive calls */
    private array $runs = [];

    /** Set once the run list is full and another operation was called: the rest of the order is unknown. */
    private bool $stopped = false;

    /** Requests to operations that were not tracked where the calls were counted. */
    private int $omitted = 0;

    /** @param array<string,mixed> $actor */
    public function __construct(public readonly array $actor) {}

    /** Calls must arrive in the order they were made. */
    public function add(ApiCall $call): void
    {
        $key = $call->method.' '.$call->route;
        ($this->operations[$key] ??= new OperationTally($call->method, $call->route, $call->offsetMs))->add($call);
        $this->requests++;
        if ($this->stopped) {
            return;
        }
        $last = array_key_last($this->runs);
        if ($last !== null && $this->runs[$last][0] === $key) {
            $this->runs[$last][2]++;
        } elseif (count($this->runs) < self::RUNS) {
            $this->runs[] = [$key, $call->offsetMs, 1];
        } else {
            $this->stopped = true;
        }
    }

    /**
     * An actor counted elsewhere (in Redis).
     *
     * @param  array<string,mixed>  $actor
     * @param  array<string,OperationTally>  $operations  by "METHOD route"
     * @param  list<array{0:string,1:int}>  $calls  the first calls in order: operation key ("*" for an untracked one) and offset
     */
    public static function restore(array $actor, array $operations, int $omitted, array $calls): self
    {
        $tally = new self($actor);
        $tally->operations = $operations;
        $tally->omitted = $omitted;
        $tally->requests = $omitted + array_sum(array_map(static fn (OperationTally $o): int => $o->requests, $operations));
        foreach ($calls as [$key, $offset]) {
            $last = array_key_last($tally->runs);
            if ($last !== null && $tally->runs[$last][0] === $key) {
                $tally->runs[$last][2]++;
            } elseif (count($tally->runs) < self::RUNS) {
                $tally->runs[] = [$key, $offset, 1];
            } else {
                break;
            }
        }

        return $tally;
    }

    /** Folds another actor in; a remainder has no order of calls. */
    public function merge(self $other): void
    {
        foreach ($other->operations as $key => $operation) {
            if (isset($this->operations[$key])) {
                $this->operations[$key]->merge($operation);
            } else {
                $this->operations[$key] = clone $operation;
            }
        }
        $this->requests += $other->requests;
        $this->omitted += $other->omitted;
    }

    /** At least one dimension that can name an actor window. */
    public function identified(): bool
    {
        $context = is_array($this->actor['context'] ?? null) ? $this->actor['context'] : [];

        return ($this->actor['subject'] ?? null) !== null || ($this->actor['organization'] ?? null) !== null || ($this->actor['token'] ?? null) !== null
            || ($this->actor['ip_reference'] ?? null) !== null || ($context['device_id'] ?? null) !== null;
    }

    /**
     * @param  int|null  $folded  The number of actors in a remainder; null for one actor's window.
     * @return array<string,mixed>
     */
    public function data(ApiActivitySettings $settings, int $start, int $seconds, ?int $folded = null): array
    {
        // The busiest 32 operations are listed, in the order they were first called.
        $ranked = $this->operations;
        uksort($ranked, fn (string $a, string $b): int => [$ranked[$b]->requests, $ranked[$a]->first, $a] <=> [$ranked[$a]->requests, $ranked[$b]->first, $b]);
        $listed = array_slice($ranked, 0, self::OPERATIONS, true);
        $omitted = $this->omitted + array_sum(array_map(static fn (OperationTally $o): int => $o->requests, array_slice($ranked, self::OPERATIONS, null, true)));
        uksort($listed, fn (string $a, string $b): int => [$listed[$a]->first, $a] <=> [$listed[$b]->first, $b]);

        // Share the window's reference budget so no operation starves the others.
        $budget = [];
        $wanting = array_filter($listed, static fn (OperationTally $o): bool => $o->candidates() > 0);
        uasort($wanting, static fn (OperationTally $a, OperationTally $b): int => $a->candidates() <=> $b->candidates());
        $left = self::WINDOW_REFERENCES;
        $remaining = count($wanting);
        foreach ($wanting as $key => $operation) {
            $budget[$key] = min($operation->candidates(), intdiv($left, $remaining));
            $left -= $budget[$key];
            $remaining--;
        }

        $index = array_flip(array_keys($listed));
        $sequence = [];
        if ($folded === null) {
            foreach ($this->runs as [$key, $offset, $repeat]) {
                // A call to an unlisted operation cannot be written; stop rather than join runs it separated.
                if (! isset($index[$key])) {
                    break;
                }
                $sequence[] = ['operation' => $index[$key], 'offset_ms' => $offset, 'repeat' => $repeat];
            }
        }

        return [
            'window_start' => gmdate('Y-m-d\TH:i:s\Z', $start),
            'window_end' => gmdate('Y-m-d\TH:i:s\Z', $start + $seconds),
            'application_id' => $settings->applicationId,
            'grouping' => $folded === null ? 'actor' : 'remainder',
            'actors' => $folded ?? 1,
            'subject' => $this->actor['subject'] ?? null,
            'organization' => $this->actor['organization'] ?? null,
            'token' => $this->actor['token'] ?? null,
            'context' => $this->actor['context'] ?? null,
            'ip_reference' => $this->actor['ip_reference'] ?? null,
            'reference_key_id' => $settings->referenceKeyId,
            'operations' => array_map(static fn (string $key): array => $listed[$key]->toArray($budget[$key] ?? 0), array_keys($listed)),
            'omitted_requests' => $omitted,
            'sequence' => $sequence,
        ];
    }
}
