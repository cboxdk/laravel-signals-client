<?php

declare(strict_types=1);

namespace Cbox\Signals\Contracts;

use Cbox\Signals\ApiActivity\ApiCall;

/** Where the middleware counts API calls. Nothing here may call Signals. */
interface ApiActivityLog
{
    public function record(ApiCall $call): void;
}
