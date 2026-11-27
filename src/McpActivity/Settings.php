<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

use Cbox\Signals\ApiActivity\ApiActivitySettings;
use InvalidArgumentException;

/** A separate source configuration, never the app's REST collector instance. */
readonly class Settings
{
    public function __construct(
        public ApiActivitySettings $source,
        public Catalog $catalog,
        public int $timeoutMs = 300000,
        public int $capacity = 10000,
        public int $retentionSeconds = 604800,
        // Host assertion: every dispatcher serving this source uses durable admission.
        // A new generation is required when this assertion changes.
        public bool $dispatchProofConfirmed = false,
    ) {
        if ($timeoutMs < 1000 || $timeoutMs > 3600000 || $capacity < 1 || $capacity > 10000) {
            throw new InvalidArgumentException('Invalid MCP deadline or bounded journal capacity.');
        }
        if ($retentionSeconds < 7200 || $retentionSeconds > 604800) {
            throw new InvalidArgumentException('MCP buffer retention is between two hours and seven days.');
        }
    }

    /** @param list<mixed> $message */
    public function reference(array $message): string
    {
        // This domain contains only arrays, strings and null, so JSON's normal
        // encoding with unescaped UTF-8/slashes/line terminators is RFC 8785.
        return substr(hash_hmac('sha256', json_encode($message, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_LINE_TERMINATORS), $this->source->referenceKey), 0, 24);
    }

    /** @return list<string|null> */
    public function domain(): array
    {
        $this->source->sendable();
        $scope = $this->source->scope;
        if ($scope === null) {
            throw new InvalidArgumentException('MCP source scope is required.');
        }

        return [$scope['project_id'], $scope['environment_id'], $scope['audience'], $scope['tenant_id'] ?? null, $this->source->applicationId];
    }

    /** Configuration identity prevents a new key/catalog/source from adopting old buffers. */
    public function identity(): string
    {
        return hash_hmac('sha256', json_encode([
            $this->domain(), $this->source->sourceId, $this->source->partition, $this->source->generation,
            $this->catalog->fingerprint(), $this->source->namespace, $this->source->referenceKeyId, $this->source->window, $this->timeoutMs,
            $this->retentionSeconds,
        ], JSON_THROW_ON_ERROR), $this->source->referenceKey);
    }

    public function prefix(): string
    {
        return $this->source->prefix.':mcp:{'.hash('sha256', json_encode([$this->source->scope, $this->source->sourceId, $this->source->partition, $this->source->generation], JSON_THROW_ON_ERROR)).'}';
    }
}
