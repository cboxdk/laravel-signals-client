<?php

declare(strict_types=1);

namespace Cbox\Signals\Contracts;

use Cbox\Signals\Data\SourceReceipt;
use Cbox\Signals\Exceptions\SignalsFailure;

/** Canonical v2 records for one source: `POST /v2/sources/{source}/records:batch`. */
interface SourceRecords
{
    /**
     * One page of at most 1000 records and 1 MiB. The page commits whole or not at all;
     * an identical retry counts as duplicates.
     *
     * @param  list<array<string,mixed>>  $records
     *
     * @throws SignalsFailure
     */
    public function send(string $source, array $records): SourceReceipt;
}
