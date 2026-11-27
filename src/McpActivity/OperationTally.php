<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

class OperationTally
{
    public int $attempts = 0;

    public int $replayed = 0;

    public int $captured = 0;

    public int $missing = 0;

    public int $overflow = 0;

    /** @var array<string,int> */
    public array $outcomes = [];

    /** @var array<string,true> */
    public array $references = [];

    public function __construct(public readonly Operation $operation)
    {
        foreach (Outcome::cases() as $outcome) {
            $this->outcomes[$outcome->value] = 0;
        }
    }

    public function add(FinalizedAttempt $value, bool $room): bool
    {
        $this->attempts++;
        $this->outcomes[$value->outcome->value]++;
        $this->replayed += (int) $value->replayed;
        if ($this->operation->policy === null) {
            return false;
        }
        $ref = $value->attempt->reference;
        if ($ref === null) {
            $this->missing++;

            return false;
        }
        $this->captured++;
        if (isset($this->references[$ref])) {
            return false;
        }
        if (! $room || count($this->references) >= 256) {
            $this->overflow++;

            return false;
        }
        $this->references[$ref] = true;

        return true;
    }

    /** @return array<string,mixed> */
    public function toArray(): array
    {
        $refs = array_keys($this->references);
        sort($refs, SORT_STRING);

        return ['tool' => $this->operation->tool, 'action' => $this->operation->action, 'attempts' => $this->attempts, 'outcomes' => $this->outcomes, 'replayed' => $this->replayed,
            'objects' => $this->operation->policy === null ? null : ['policy_id' => $this->operation->policy, 'captured' => $this->captured, 'missing' => $this->missing, 'references' => $refs, 'overflow' => $this->overflow]];
    }
}
