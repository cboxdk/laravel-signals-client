<?php

declare(strict_types=1);

namespace Cbox\Signals\Contracts;

use Cbox\Signals\McpActivity\Attempt;
use Cbox\Signals\McpActivity\Outcome;

interface McpActivityLog
{
    /** Internal dispatch id, not JSON-RPC id. Durable hosts must not dispatch on null or failure. */
    public function start(Attempt $attempt): ?string;

    /** A deadline or terminal outcome wins once; later results do not rewrite it. */
    public function finish(string $id, Outcome $outcome, bool $replayed = false): void;
}
