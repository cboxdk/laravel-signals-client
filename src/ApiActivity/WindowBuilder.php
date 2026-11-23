<?php

declare(strict_types=1);

namespace Cbox\Signals\ApiActivity;

use Cbox\Signals\Support\Wire;
use InvalidArgumentException;

/**
 * Turns the calls of one closed window into `api.activity` records.
 *
 * The busiest `max_actors` actors get a window of their own. Every other actor,
 * and every combination without a user, organization, token, address or
 * device, is folded into a remainder per shared network context.
 */
final class WindowBuilder
{
    /** Remainder records per window: the busiest networks get one each, the rest one per enrichment version. */
    public const int REMAINDERS = 16;

    /** @var array<string,ActorTally> */
    private array $actors = [];

    /** @var list<array{0:ActorTally,1:int}> remainders folded before they reached this builder, with their actor counts */
    private array $folded = [];

    public function __construct(private readonly ApiActivitySettings $settings, public readonly int $start, public readonly int $seconds) {}

    /** Calls must be added in the order they were made. */
    public function add(ApiCall $call): void
    {
        if ($call->windowStart !== $this->start || $call->windowSeconds !== $this->seconds) {
            throw new InvalidArgumentException('The call belongs to another window.');
        }
        ($this->actors[$call->actorKey()] ??= new ActorTally($call->actor))->add($call);
    }

    /** An actor counted elsewhere, under a key that is stable within the window. */
    public function addTally(string $key, ActorTally $tally): void
    {
        $this->actors[$key] = $tally;
    }

    /** Actors folded before they reached this builder; its actor holds only the network context. */
    public function addFolded(ActorTally $tally, int $actors): void
    {
        $this->folded[] = [$tally, max(1, $actors)];
    }

    public function empty(): bool
    {
        return $this->actors === [] && $this->folded === [];
    }

    /**
     * Complete records for Signals. Positions continue from `$sequence`; `$part`
     * names this close of the window, so calls that arrive after it closed
     * become records with their own ids, which Signals adds to the earlier ones.
     *
     * @return list<array<string,mixed>>
     */
    public function records(string $part, int $sequence, int $recordedAt): array
    {
        $this->settings->sendable();
        $scope = (array) $this->settings->scope;
        $records = [];
        foreach ($this->windows() as $key => $data) {
            $records[] = [
                'contract_version' => '2.6.0',
                'id' => sprintf('api-%d-%d-%s-%s', $this->start, $this->seconds, $part, $key),
                'scope' => $scope,
                'position' => ['source_id' => $this->settings->sourceId, 'partition' => $this->settings->partition, 'generation' => $this->settings->generation, 'sequence' => (string) $sequence++],
                'occurred_at' => $data['window_end'],
                'recorded_at' => gmdate('Y-m-d\TH:i:s\Z', max($recordedAt, $this->start + $this->seconds)),
                'kind' => 'event',
                'name' => 'api.activity',
                'data' => $data,
            ];
        }

        return $records;
    }

    /** @return array<string,array<string,mixed>> event data by a stable key within the window */
    public function windows(): array
    {
        $named = array_filter($this->actors, static fn (ActorTally $actor): bool => $actor->identified());
        uksort($named, static fn (string $a, string $b): int => [$named[$b]->requests, $a] <=> [$named[$a]->requests, $b]);
        $kept = array_slice($named, 0, $this->settings->maxActors, true);

        $windows = [];
        foreach ($kept as $key => $actor) {
            $windows['a'.substr($key, 0, 32)] = $actor->data($this->settings, $this->start, $this->seconds);
        }

        /** @var array<string,array{0:ActorTally,1:int}> $groups */
        $groups = [];
        foreach (array_diff_key($this->actors, $kept) as $actor) {
            $network = self::network($actor->actor);
            $group = hash('sha256', json_encode($network, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION));
            $groups[$group] ??= [new ActorTally(['subject' => null, 'organization' => null, 'token' => null, 'ip_reference' => null, 'context' => $network]), 0];
            $groups[$group][0]->merge($actor);
            $groups[$group][1]++;
        }
        foreach ($this->folded as [$tally, $count]) {
            $network = self::network($tally->actor);
            $group = hash('sha256', json_encode($network, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION));
            $groups[$group] ??= [new ActorTally(['subject' => null, 'organization' => null, 'token' => null, 'ip_reference' => null, 'context' => $network]), 0];
            $groups[$group][0]->merge($tally);
            $groups[$group][1] += $count;
        }
        if (count($groups) > self::REMAINDERS) {
            uksort($groups, static fn (string $a, string $b): int => [$groups[$b][0]->requests, $a] <=> [$groups[$a][0]->requests, $b]);
            // The smaller networks share a remainder per enrichment, which keeps only the context they all share.
            $folded = [];
            foreach (array_slice($groups, self::REMAINDERS - 1, null, true) as [$tally, $count]) {
                $context = $tally->actor['context'];
                $enrichment = is_array($context) && is_string($context['enrichment_version'] ?? null) ? $context['enrichment_version'] : '';
                $folded[$enrichment][] = [$tally, $count];
            }
            $groups = array_slice($groups, 0, self::REMAINDERS - 1, true);
            foreach ($folded as $enrichment => $members) {
                $shared = null;
                foreach ($members as [$tally]) {
                    $context = Wire::object($tally->actor['context']);
                    $shared = $shared === null ? $context : self::shared($shared, $context);
                }
                $merged = [new ActorTally(['subject' => null, 'organization' => null, 'token' => null, 'ip_reference' => null, 'context' => $shared]), 0];
                foreach ($members as [$tally, $count]) {
                    $merged[0]->merge($tally);
                    $merged[1] += $count;
                }
                $groups['folded-'.$enrichment] = $merged;
            }
        }
        foreach ($groups as $group => [$tally, $count]) {
            $windows['r'.substr($group, 0, 32)] = $tally->data($this->settings, $this->start, $this->seconds, $count);
        }

        return $windows;
    }

    /**
     * The fields two contexts share; a network class they do not share is unknown, any other field null.
     *
     * @param  array<string,mixed>  $a
     * @param  array<string,mixed>  $b
     * @return array<string,mixed>
     */
    private static function shared(array $a, array $b): array
    {
        foreach ($a as $field => $value) {
            if (($b[$field] ?? null) !== $value) {
                $a[$field] = $field === 'network_class' ? 'unknown' : null;
            }
        }

        return $a;
    }

    /**
     * The context a remainder keeps: only what the folded actors' network shares.
     *
     * @param  array<string,mixed>  $actor
     * @return array<string,mixed>
     */
    public static function network(array $actor): array
    {
        $context = is_array($actor['context'] ?? null) ? $actor['context'] : [];

        return [...(new ApiContext)->toArray(), ...array_intersect_key($context, array_flip(['country', 'asn', 'network_class', 'enrichment_version']))];
    }
}
