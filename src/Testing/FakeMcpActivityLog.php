<?php

declare(strict_types=1);

namespace Cbox\Signals\Testing;

use Cbox\Signals\Contracts\McpActivityLog;
use Cbox\Signals\McpActivity\Attempt;
use Cbox\Signals\McpActivity\FinalizedAttempt;
use Cbox\Signals\McpActivity\Outcome;

/** In-memory observation fake; deadline and concurrency guarantees belong to Redis tests. */
class FakeMcpActivityLog implements McpActivityLog
{
    /** @var array<string,Attempt> */
    public array $attempts = [];

    /** @var array<string,FinalizedAttempt> */
    public array $finalized = [];

    public function start(Attempt $attempt): ?string
    {
        $id = 'dispatch-'.count($this->attempts);
        $this->attempts[$id] = $attempt;

        return $id;
    }

    public function finish(string $id, Outcome $outcome, bool $replayed = false): void
    {
        if (isset($this->attempts[$id])) {
            $this->finalized[$id] ??= new FinalizedAttempt($this->attempts[$id], $outcome, $replayed, count($this->finalized));
        }
    }
}
