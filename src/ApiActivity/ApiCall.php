<?php

declare(strict_types=1);

namespace Cbox\Signals\ApiActivity;

use InvalidArgumentException;

/**
 * One answered API call as it is counted: already pseudonymized, so nothing in
 * it is a raw address, a path or a parameter value.
 */
final readonly class ApiCall
{
    private const array METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'];

    /**
     * @param  array{subject:array{namespace:string,type:string,id:string}|null,organization:array{namespace:string,type:string,id:string}|null,token:array{namespace:string,type:string,id:string}|null,ip_reference:string|null,context:array<string,mixed>}  $actor
     */
    public function __construct(
        public int $windowStart,
        public int $windowSeconds,
        public int $offsetMs,
        public array $actor,
        public string $method,
        public string $route,
        public int $status,
        public ?string $reference = null,
        public bool $conflict = false,
    ) {
        if ($offsetMs < 0 || $offsetMs >= $windowSeconds * 1000 || $windowStart % $windowSeconds !== 0) {
            throw new InvalidArgumentException('A call lies inside a window that starts on a multiple of its length.');
        }
        if (! in_array($method, [...self::METHODS, 'OTHER'], true) || strlen($route) > 256 || preg_match('/^\/[^?#\s]*$/D', $route) !== 1) {
            throw new InvalidArgumentException('A call names a listed method and a route template.');
        }
        if ($status < 100 || $status > 599 || ($reference !== null && preg_match('/^[0-9a-f]{24}$/D', $reference) !== 1)) {
            throw new InvalidArgumentException('A call has an HTTP status and at most a 24-character reference.');
        }
    }

    /**
     * @param  array<string>  $parameters  The route's path-parameter values as requested.
     */
    public static function of(ApiActivitySettings $settings, ApiActor $actor, int $startedAtMs, string $method, string $route, int $status, array $parameters, bool $conflict): self
    {
        $start = intdiv(intdiv($startedAtMs, 1000), $settings->window) * $settings->window;
        $entity = static fn (?string $id, string $type): ?array => $id === null ? null : ['namespace' => $settings->namespace, 'type' => $type, 'id' => $id];
        $ip = $actor->ip !== null && ($packed = @inet_pton($actor->ip)) !== false ? (string) inet_ntop($packed) : null;
        ksort($parameters, SORT_STRING);
        $method = strtoupper($method);

        return new self(
            $start,
            $settings->window,
            $startedAtMs - $start * 1000,
            [
                'subject' => $entity($actor->user, 'user'),
                'organization' => $entity($actor->organization, $actor->organizationType),
                'token' => $entity($actor->token, 'token'),
                'ip_reference' => $ip === null ? null : $settings->pseudonym('ip', $ip),
                'context' => $actor->context->toArray(),
            ],
            in_array($method, self::METHODS, true) ? $method : 'OTHER',
            $route,
            $status,
            self::parameterized($route) ? $settings->pseudonym('parameters', json_encode($parameters, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)) : null,
            $conflict,
        );
    }

    public static function parameterized(string $route): bool
    {
        return str_contains($route, '{');
    }

    /** The combination of dimensions this call belongs to. */
    public function actorKey(): string
    {
        return hash('sha256', json_encode($this->actor, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION));
    }
}
