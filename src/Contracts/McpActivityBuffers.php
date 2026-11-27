<?php

declare(strict_types=1);

namespace Cbox\Signals\Contracts;

use Cbox\Signals\Data\Entity;
use Cbox\Signals\McpActivity\ErasedActors;
use Cbox\Signals\McpActivity\Settings;
use Closure;

interface McpActivityBuffers
{
    /** Serialize journal mutation/delivery with erasure, across generations.
     * @template T
     *
     * @param  Closure(ErasedActors):T  $operation
     * @return T
     */
    public function protect(Settings $settings, Closure $operation): mixed;

    /** Persist the fence before cleanup. Failure must be retried, never swallowed. */
    public function erase(Entity $entity): void;
}
