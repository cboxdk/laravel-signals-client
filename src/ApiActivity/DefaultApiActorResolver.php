<?php

declare(strict_types=1);

namespace Cbox\Signals\ApiActivity;

use Cbox\Signals\Contracts\ApiActorResolver;
use Illuminate\Contracts\Auth\Authenticatable;
use Illuminate\Http\Request;

/**
 * A standard Laravel API: the authenticated user, the database id of the
 * Sanctum or Passport token it used, and the client address. No organization;
 * bind your own resolver for that.
 */
final class DefaultApiActorResolver implements ApiActorResolver
{
    public function resolve(Request $request): ApiActor
    {
        $user = $request->user();
        if (! $user instanceof Authenticatable) {
            return new ApiActor(ip: $request->ip());
        }
        $token = null;
        // Sanctum's currentAccessToken() and Passport's token() are models; their key is the internal id, never the secret.
        foreach (['currentAccessToken', 'token'] as $method) {
            $model = method_exists($user, $method) ? $user->{$method}() : null;
            if (is_object($model) && method_exists($model, 'getKey')) {
                $token = self::id($model->getKey());
                break;
            }
        }

        return new ApiActor(user: self::id($user->getAuthIdentifier()), token: $token, ip: $request->ip());
    }

    private static function id(mixed $value): ?string
    {
        return (is_string($value) || is_int($value)) && (string) $value !== '' ? (string) $value : null;
    }
}
