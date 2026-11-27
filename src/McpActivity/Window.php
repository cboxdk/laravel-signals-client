<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

use Cbox\Signals\Support\Wire;
use LengthException;

/** A disjoint part of one actor's start-time window, ordered by dispatch ticket. */
class Window
{
    /** @var list<FinalizedAttempt> */
    private array $attempts = [];

    public function __construct(private readonly Settings $settings, public readonly int $start, public readonly string $actorJson) {}

    public function add(FinalizedAttempt $attempt): void
    {
        $this->attempts[] = $attempt;
    }

    /** @return array<string,mixed> */
    public function record(string $id, int $sealedAt): array
    {
        usort($this->attempts, static fn (FinalizedAttempt $a, FinalizedAttempt $b): int => [$a->attempt->startedMs, $a->order] <=> [$b->attempt->startedMs, $b->order]);
        $actor = Wire::object(json_decode($this->actorJson, true, flags: JSON_THROW_ON_ERROR));
        $context = Wire::object($actor['context']);
        $identified = $actor['subject'] !== null || $actor['organization'] !== null || $actor['token'] !== null || $actor['ip_reference'] !== null || $context['device_id'] !== null;
        $operations = [];
        $index = [];
        $sequence = [];
        $refs = $represented = $omitted = 0;
        $stopped = ! $identified;
        foreach ($this->attempts as $value) {
            $op = $value->attempt->operation;
            $key = json_encode([$op->tool, $op->action], JSON_THROW_ON_ERROR);
            if (! isset($index[$key])) {
                if (count($operations) >= 32) {
                    $omitted++;
                    $stopped = true;

                    continue;
                }
                $index[$key] = count($operations);
                $operations[] = new OperationTally($op);
            }
            $i = $index[$key];
            $refs += (int) $operations[$i]->add($value, $refs < 768);
            if ($stopped || $represented >= 1024) {
                continue;
            }
            $last = array_key_last($sequence);
            if ($last !== null && $sequence[$last]['operation'] === $i) {
                $sequence[$last]['repeat']++;
            } else {
                if (count($sequence) >= 256) {
                    $stopped = true;

                    continue;
                }
                $sequence[] = ['operation' => $i, 'offset_ms' => $value->attempt->startedMs - $this->start * 1000, 'repeat' => 1];
            }
            $represented++;
        }
        $end = gmdate('Y-m-d\TH:i:s\Z', $this->start + $this->settings->source->window);
        $record = ['contract_version' => '2.8.0', 'id' => $id, 'scope' => $this->settings->source->scope,
            'position' => ['source_id' => $this->settings->source->sourceId, 'partition' => $this->settings->source->partition, 'generation' => $this->settings->source->generation, 'sequence' => '__mcp_position__'],
            'occurred_at' => $end, 'recorded_at' => gmdate('Y-m-d\TH:i:s\Z', $sealedAt), 'kind' => 'event', 'name' => 'mcp.activity',
            'data' => ['window_start' => gmdate('Y-m-d\TH:i:s\Z', $this->start), 'window_end' => $end, 'application_id' => $this->settings->source->applicationId,
                'catalog_revision' => $this->settings->catalog->revision, 'finalized_at' => gmdate('Y-m-d\TH:i:s\Z', $sealedAt), 'finalization_timeout_ms' => $this->settings->timeoutMs,
                'grouping' => $identified ? 'actor' : 'remainder', 'actors' => 1, ...$actor, 'reference_key_id' => $this->settings->source->referenceKeyId,
                'operations' => array_map(static fn (OperationTally $o): array => $o->toArray(), $operations), 'total_attempts' => count($this->attempts), 'omitted_attempts' => $omitted,
                'sequence' => $sequence, 'sequence_omitted' => count($this->attempts) - $represented]];
        // Reserve 20 bytes for the eventual monotonic source sequence. Dropping
        // retained references must account for every represented attempt.
        if (strlen(json_encode($record, JSON_THROW_ON_ERROR)) > 65516) {
            foreach ($operations as $operation) {
                $operation->references = [];
                $operation->overflow = $operation->captured;
            }
            $record['data']['operations'] = array_map(static fn (OperationTally $o): array => $o->toArray(), $operations);
            $record['data']['sequence'] = [];
            $record['data']['sequence_omitted'] = count($this->attempts);
        }
        if (strlen(json_encode($record, JSON_THROW_ON_ERROR)) > 65516) {
            throw new LengthException('MCP actor window exceeds intake limit.');
        }

        return $record;
    }
}
