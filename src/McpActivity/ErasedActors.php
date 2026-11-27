<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

use Cbox\Signals\Data\Entity;
use Cbox\Signals\Support\Wire;

/** Tombstones retain one-way entity fingerprints, not raw identity values. */
readonly class ErasedActors
{
    /** @param array<string,true> $fingerprints */
    public function __construct(private array $fingerprints = [], private int $anonymousBeforeMs = 0) {}

    public function empty(): bool
    {
        return $this->fingerprints === [] && $this->anonymousBeforeMs === 0;
    }

    public static function fingerprint(Entity $entity): string
    {
        return hash('sha256', json_encode([$entity->namespace, $entity->type, $entity->id], JSON_THROW_ON_ERROR));
    }

    /** @param array<string,mixed> $actor */
    public function contains(array $actor, int $windowStartMs): bool
    {
        $identified = false;
        foreach (['subject', 'organization', 'token'] as $key) {
            if (($actor[$key] ?? null) === null) {
                continue;
            }
            $identified = true;
            $value = Wire::object($actor[$key]);
            $entity = new Entity(Wire::text($value['namespace']), Wire::text($value['type']), Wire::text($value['id']));
            if (isset($this->fingerprints[self::fingerprint($entity)])) {
                return true;
            }
        }

        // Match the engine's conservative rule for remainder/IP/device-only windows.
        return ! $identified && $this->anonymousBeforeMs > 0 && $windowStartMs <= $this->anonymousBeforeMs;
    }
}
