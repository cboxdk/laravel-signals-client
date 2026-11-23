<?php

declare(strict_types=1);

namespace Cbox\Signals\Contracts;

use Cbox\Signals\ApiActivity\ApiActor;
use Illuminate\Http\Request;

/**
 * Decides which user, organization and token an API request belongs to. Called
 * after the response has been sent, so the authenticated user is known.
 */
interface ApiActorResolver
{
    public function resolve(Request $request): ApiActor;
}
