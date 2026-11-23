<?php

declare(strict_types=1);

namespace Cbox\Signals\ApiActivity;

use Cbox\Signals\Contracts\ApiActivityLog;
use Cbox\Signals\Contracts\ApiActorResolver;
use Closure;
use Illuminate\Http\Request;
use Illuminate\Routing\Route;
use Illuminate\Support\Carbon;
use Symfony\Component\HttpFoundation\Response;
use Throwable;

/**
 * Counts each API call for Cbox Signals. The request itself only notes its start
 * time; the call is counted in `terminate`, after the response has been sent,
 * and nothing here talks to Signals. A failure to count is reported, never
 * thrown into the application.
 */
final class CountApiActivity
{
    private const string STARTED = 'signals.api_activity.started_ms';

    private static int $reportedAt = 0;

    public function __construct(private readonly ApiActivityLog $log, private readonly ApiActorResolver $actors, private readonly ApiActivitySettings $settings) {}

    public function handle(Request $request, Closure $next): mixed
    {
        $request->attributes->set(self::STARTED, Carbon::now()->getTimestampMs());

        return $next($request);
    }

    public function terminate(Request $request, Response $response): void
    {
        try {
            $call = $this->call($request, $response);
            if ($call !== null) {
                $this->log->record($call);
            }
        } catch (Throwable $failure) {
            // Under load a Redis outage fails every request; report it once a minute per process.
            if (self::$reportedAt < time() - 60) {
                self::$reportedAt = time();
                report($failure);
            }
        }
    }

    private function call(Request $request, Response $response): ?ApiCall
    {
        $route = $request->route();
        $started = $request->attributes->get(self::STARTED);
        // Only calls that matched a route have a template; a raw path never leaves the application.
        if (! $this->settings->counting() || ! $route instanceof Route || ! is_int($started)) {
            return null;
        }
        $parameters = [];
        $conflict = false;
        foreach ($route->originalParameters() as $name => $value) {
            if (! is_string($value)) {
                continue;
            }
            $parameters[$name] = $value;
            // The same parameter given again in the query or body with another value.
            if ($request->has($name)) {
                $given = $request->input($name);
                $conflict = $conflict || ! is_scalar($given) || (string) $given !== $value;
            }
        }

        return ApiCall::of($this->settings, $this->actors->resolve($request), $started, $request->getMethod(), '/'.ltrim($route->uri(), '/'), $response->getStatusCode(), $parameters, $conflict);
    }
}
