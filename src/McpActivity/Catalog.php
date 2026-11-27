<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

use InvalidArgumentException;

readonly class Catalog
{
    /** @param list<Operation> $operations */
    public function __construct(public string $revision, private array $operations)
    {
        if (preg_match('/\A[A-Za-z0-9_.-]{1,64}\z/', $revision) !== 1) {
            throw new InvalidArgumentException('Invalid MCP catalog revision.');
        }
        $keys = [];
        foreach ($operations as $operation) {
            $key = json_encode([$operation->tool, $operation->action], JSON_THROW_ON_ERROR);
            if ($operation->tool === 'unknown_tool' || $operation->action === 'unknown_action' || isset($keys[$key])) {
                throw new InvalidArgumentException('Duplicate or reserved MCP catalog entry.');
            }
            $keys[$key] = true;
        }
    }

    /** Detect a changed policy even when a host forgets to change its revision. */
    public function fingerprint(): string
    {
        $entries = [];
        foreach ($this->operations as $operation) {
            $fields = $operation->objectFields;
            sort($fields, SORT_STRING);
            $entries[] = json_encode([$operation->tool, $operation->action, $operation->policy, $fields], JSON_THROW_ON_ERROR);
        }
        sort($entries, SORT_STRING);

        return hash('sha256', json_encode([$this->revision, $entries], JSON_THROW_ON_ERROR));
    }

    public function resolve(mixed $tool, mixed $action): Operation
    {
        $known = false;
        foreach ($this->operations as $operation) {
            if ($operation->tool === $tool) {
                $known = true;
                if ($operation->action === $action) {
                    return $operation;
                }
            }
        }

        return new Operation($known && is_string($tool) ? $tool : 'unknown_tool', 'unknown_action');
    }
}
