<?php

declare(strict_types=1);

namespace Cbox\Signals;

use Cbox\Signals\Contracts\SourceRecords;
use Cbox\Signals\Data\SourceReceipt;
use Cbox\Signals\Support\JsonTransport;
use Cbox\Signals\Support\Wire;
use InvalidArgumentException;

class HttpSourceRecords implements SourceRecords
{
    public function __construct(private readonly JsonTransport $transport, private readonly string $token) {}

    public function send(string $source, array $records): SourceReceipt
    {
        if ($records === [] || count($records) > 1000) {
            throw new InvalidArgumentException('A page holds 1 to 1000 records.');
        }
        $data = $this->transport->send('POST', '/v2/sources/'.rawurlencode($source).'/records:batch', $this->token, ['records' => $records]);

        return new SourceReceipt(Wire::integer($data['accepted'] ?? null), Wire::integer($data['duplicates'] ?? null), Wire::date($data['accepted_at'] ?? null));
    }
}
