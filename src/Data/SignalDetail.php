<?php

declare(strict_types=1);

namespace Cbox\Signals\Data;

use Cbox\Signals\Support\Wire;

readonly class SignalDetail
{
    /** @param list<SignalRevision> $revisions
     * @param list<SignalDelivery> $deliveries */
    public function __construct(public SignalRecord $signal, public array $revisions, public array $deliveries, public int $historyLimit) {}

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(SignalRecord::fromArray($data),
            array_map(static fn (mixed $item): SignalRevision => SignalRevision::fromArray(Wire::object($item)), Wire::items($data['revisions'] ?? null)),
            array_map(static fn (mixed $item): SignalDelivery => SignalDelivery::fromArray(Wire::object($item)), Wire::items($data['deliveries'] ?? null)),
            Wire::integer($data['history_limit'] ?? null, 1));
    }
}
