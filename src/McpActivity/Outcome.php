<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

enum Outcome: string
{
    case Success = 'success';
    case Invalid = 'invalid';
    case Denied = 'denied';
    case NotFound = 'not_found';
    case RateLimited = 'rate_limited';
    case ToolError = 'tool_error';
    case InternalError = 'internal_error';
    case Cancelled = 'cancelled';
    case Unknown = 'unknown';
}
