<?php

declare(strict_types=1);

namespace Cbox\Signals\Testing;

use Cbox\Signals\ApiActivity\ApiActivitySettings;
use Cbox\Signals\ApiActivity\ApiCall;
use Cbox\Signals\ApiActivity\WindowBuilder;
use Cbox\Signals\Contracts\ApiActivityLog;
use Cbox\Signals\Support\Wire;
use Illuminate\Contracts\Container\Container;
use PHPUnit\Framework\Assert;

/**
 * Keeps counted API calls in memory and shows the `api.activity` records they
 * would become, every window closed at once. Install it with `swap()`.
 */
class FakeApiActivity implements ApiActivityLog
{
    /** @var list<ApiCall> */
    public array $calls = [];

    public readonly ApiActivitySettings $settings;

    public function __construct(?ApiActivitySettings $settings = null)
    {
        $this->settings = $settings ?? ApiActivitySettings::testing();
    }

    /** Bind a fake log, and settings that count, in the application container. */
    public static function swap(Container $app, ?ApiActivitySettings $settings = null): self
    {
        $fake = new self($settings ?? ApiActivitySettings::testing());
        $app->instance(ApiActivityLog::class, $fake);
        $app->instance(ApiActivitySettings::class, $fake->settings);

        return $fake;
    }

    public function record(ApiCall $call): void
    {
        $this->calls[] = $call;
    }

    /**
     * The records Signals would receive, in position order from 1.
     *
     * @return list<array<string,mixed>>
     */
    public function records(): array
    {
        $calls = $this->calls;
        usort($calls, static fn (ApiCall $a, ApiCall $b): int => [$a->windowStart, $a->windowSeconds, $a->offsetMs] <=> [$b->windowStart, $b->windowSeconds, $b->offsetMs]);
        $builders = [];
        foreach ($calls as $call) {
            ($builders[$call->windowStart.':'.$call->windowSeconds] ??= new WindowBuilder($this->settings, $call->windowStart, $call->windowSeconds))->add($call);
        }
        $records = [];
        foreach ($builders as $builder) {
            array_push($records, ...$builder->records('1', count($records) + 1, $builder->start + $builder->seconds));
        }

        return $records;
    }

    /**
     * The `data` of every record that passes the filter.
     *
     * @param  (callable(array<string,mixed>):bool)|null  $filter
     * @return list<array<string,mixed>>
     */
    public function windows(?callable $filter = null): array
    {
        $windows = [];
        foreach ($this->records() as $record) {
            $data = Wire::object($record['data']);
            if ($filter === null || $filter($data)) {
                $windows[] = $data;
            }
        }

        return $windows;
    }

    /** @param (callable(array<string,mixed>):bool)|null $filter */
    public function assertSent(?callable $filter = null, ?int $times = null): void
    {
        $count = count($this->windows($filter));
        if ($times === null) {
            Assert::assertGreaterThan(0, $count, 'No matching api.activity window would have been sent.');
        } else {
            Assert::assertSame($times, $count, 'Unexpected number of matching api.activity windows.');
        }
    }

    public function assertNothingSent(): void
    {
        Assert::assertSame([], $this->calls, 'API calls were counted.');
    }
}
