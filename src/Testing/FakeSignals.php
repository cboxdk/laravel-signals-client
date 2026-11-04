<?php

declare(strict_types=1);

namespace Cbox\Signals\Testing;

use Cbox\Signals\Contracts\Signals;
use Cbox\Signals\Data\Evaluation;
use Cbox\Signals\Data\EvaluationRequest;
use Cbox\Signals\Data\EventBatch;
use Cbox\Signals\Data\MetricBatch;
use Cbox\Signals\Data\Receipt;
use Cbox\Signals\Data\ReplayRequest;
use InvalidArgumentException;
use LogicException;
use Throwable;

/** Explicit test responses only. The production provider never binds this fake. */
class FakeSignals implements Signals
{
    /** @var list<array{batch: EventBatch, key: string}> */
    public array $eventCalls = [];

    /** @var list<array{batch: MetricBatch, key: string}> */
    public array $metricCalls = [];

    /** @var list<array{request: ReplayRequest, key: string}> */
    public array $replayCalls = [];

    /** @var list<EvaluationRequest> */
    public array $evaluationCalls = [];

    /** @var array<string,list<Receipt|Throwable>> */
    private array $receipts = [];

    /** @var list<Evaluation|Throwable> */
    private array $evaluations = [];

    public function queueReceipt(string $method, Receipt|Throwable $result): self
    {
        if (! in_array($method, ['events', 'metrics', 'replay'], true)) {
            throw new InvalidArgumentException('Receipt method must be events, metrics or replay.');
        }
        $this->receipts[$method][] = $result;

        return $this;
    }

    public function queueEvaluation(Evaluation|Throwable $result): self
    {
        $this->evaluations[] = $result;

        return $this;
    }

    public function events(EventBatch $batch, string $idempotencyKey): Receipt
    {
        $this->eventCalls[] = ['batch' => $batch, 'key' => $idempotencyKey];

        return $this->receipt('events');
    }

    public function metrics(MetricBatch $batch, string $idempotencyKey): Receipt
    {
        $this->metricCalls[] = ['batch' => $batch, 'key' => $idempotencyKey];

        return $this->receipt('metrics');
    }

    public function replay(ReplayRequest $request, string $idempotencyKey): Receipt
    {
        $this->replayCalls[] = ['request' => $request, 'key' => $idempotencyKey];

        return $this->receipt('replay');
    }

    public function evaluate(EvaluationRequest $request): Evaluation
    {
        $this->evaluationCalls[] = $request;
        $result = array_shift($this->evaluations);
        if ($result instanceof Throwable) {
            throw $result;
        }
        if ($result === null) {
            throw new LogicException('No evaluation response was configured for this test.');
        }

        return $result;
    }

    private function receipt(string $method): Receipt
    {
        $this->receipts[$method] ??= [];
        $result = array_shift($this->receipts[$method]);
        if ($result instanceof Throwable) {
            throw $result;
        }
        if ($result === null) {
            throw new LogicException('No '.$method.' response was configured for this test.');
        }

        return $result;
    }
}
