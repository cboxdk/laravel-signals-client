<?php

declare(strict_types=1);

namespace Cbox\Signals\Data;

use DateTimeImmutable;

readonly class SourceReceipt
{
    public function __construct(public int $accepted, public int $duplicates, public DateTimeImmutable $acceptedAt) {}
}
