<?php

declare(strict_types=1);

namespace Cbox\Signals\ApiActivity;

/**
 * Counts for one method and route template inside one window.
 *
 * References are a bottom-k sample: the smallest pseudonyms are kept, so the
 * same object is kept on every server and in every window under one key, and
 * the kept set can estimate how many distinct sets there were. Every answered
 * request whose set is not kept is counted in `overflow`.
 *
 * @internal
 */
final class OperationTally
{
    public const int REFERENCES = 256;

    /** @var array{success:int,redirect:int,client_error:int,server_error:int,denied:int,not_found:int,rate_limited:int} */
    public array $responses = ['success' => 0, 'redirect' => 0, 'client_error' => 0, 'server_error' => 0, 'denied' => 0, 'not_found' => 0, 'rate_limited' => 0];

    public int $requests = 0;

    /** @var array<string,int> requests per kept reference */
    private array $references = [];

    private int $overflow = 0;

    private int $conflicts = 0;

    /** The largest kept reference once the sample is full; a larger one is never kept. */
    private ?string $ceiling = null;

    public function __construct(public readonly string $method, public readonly string $route, public int $first) {}

    /**
     * An operation counted elsewhere (in Redis): its responses, its kept references
     * with the answered requests each, and the answered requests not kept.
     *
     * @param  array{success:int,redirect:int,client_error:int,server_error:int,denied:int,not_found:int,rate_limited:int}  $responses
     * @param  array<string,int>  $references
     */
    public static function restore(string $method, string $route, int $first, array $responses, array $references, int $overflow, int $conflicts): self
    {
        $operation = new self($method, $route, $first);
        $operation->responses = $responses;
        $operation->requests = $responses['success'] + $responses['redirect'] + $responses['client_error'] + $responses['server_error'];
        $operation->overflow = $overflow;
        $operation->conflicts = $conflicts;
        foreach ($references as $reference => $count) {
            $operation->reference((string) $reference, $count);
        }

        return $operation;
    }

    public function add(ApiCall $call): void
    {
        $this->requests++;
        $status = $call->status;
        $this->responses[$status >= 500 ? 'server_error' : ($status >= 400 ? 'client_error' : ($status >= 300 ? 'redirect' : 'success'))]++;
        $part = match ($status) {
            401, 403 => 'denied',
            404 => 'not_found',
            429 => 'rate_limited',
            default => null,
        };
        if ($part !== null) {
            $this->responses[$part]++;
        }
        // Parameter sets describe objects that were actually served: answered requests only.
        if ($status < 400 && $call->reference !== null) {
            $this->conflicts += $call->conflict ? 1 : 0;
            $this->reference($call->reference, 1);
        }
    }

    public function merge(self $other): void
    {
        $this->requests += $other->requests;
        foreach ($other->responses as $class => $count) {
            $this->responses[$class] += $count;
        }
        $this->first = min($this->first, $other->first);
        $this->overflow += $other->overflow;
        $this->conflicts += $other->conflicts;
        foreach ($other->references as $reference => $count) {
            $this->reference((string) $reference, $count);
        }
    }

    /** How many references this operation would send without a window budget. */
    public function candidates(): int
    {
        return count($this->references);
    }

    /** @return array{method:string,route:string,responses:array<string,int>,parameters:array{references:list<string>,overflow:int,conflicts:int}|null} */
    public function toArray(int $references): array
    {
        $sorted = $this->references;
        ksort($sorted, SORT_STRING);
        $kept = array_slice($sorted, 0, $references, true);
        $dropped = array_sum(array_slice($sorted, $references, null, true));

        return [
            'method' => $this->method,
            'route' => $this->route,
            'responses' => $this->responses,
            'parameters' => ApiCall::parameterized($this->route)
                ? ['references' => array_map(strval(...), array_keys($kept)), 'overflow' => $this->overflow + $dropped, 'conflicts' => $this->conflicts]
                : null,
        ];
    }

    private function reference(string $reference, int $count): void
    {
        if (isset($this->references[$reference])) {
            $this->references[$reference] += $count;

            return;
        }
        if ($this->ceiling !== null && strcmp($reference, $this->ceiling) > 0) {
            $this->overflow += $count;

            return;
        }
        $this->references[$reference] = $count;
        if (count($this->references) < self::REFERENCES) {
            return;
        }
        ksort($this->references, SORT_STRING);
        if (count($this->references) > self::REFERENCES) {
            $this->overflow += (int) array_pop($this->references);
        }
        $this->ceiling = (string) array_key_last($this->references);
    }
}
