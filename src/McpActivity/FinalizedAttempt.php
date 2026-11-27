<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

readonly class FinalizedAttempt
{
    public function __construct(public Attempt $attempt, public Outcome $outcome, public bool $replayed, public int $order) {}
}
