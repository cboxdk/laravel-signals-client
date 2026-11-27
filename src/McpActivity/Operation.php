<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

use InvalidArgumentException;

/** Server-owned catalog entry. Never construct one from a caller's tool name. */
readonly class Operation
{
    /** @param list<string> $objectFields */
    public function __construct(
        public string $tool,
        public ?string $action = null,
        public ?string $policy = null,
        public array $objectFields = [],
    ) {
        foreach ([$tool, $action, $policy, ...$objectFields] as $name) {
            if ($name !== null && preg_match('/\A[A-Za-z0-9_.-]{1,64}\z/', $name) !== 1) {
                throw new InvalidArgumentException('MCP catalog names are 1–64 ASCII name characters.');
            }
        }
        if (($policy === null) !== ($objectFields === []) || count($objectFields) > 32 || count(array_unique($objectFields)) !== count($objectFields)) {
            throw new InvalidArgumentException('An object policy needs 1–32 unique scalar field names.');
        }
    }
}
