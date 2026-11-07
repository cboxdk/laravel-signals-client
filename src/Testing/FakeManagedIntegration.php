<?php

declare(strict_types=1);

namespace Cbox\Signals\Testing;

use Cbox\Signals\Contracts\ManagedIntegration;
use Cbox\Signals\Data\EnrollmentStatus;
use Cbox\Signals\Data\ManagedEnrollment;
use Cbox\Signals\Data\ManagedEnvironment;
use DateTimeImmutable;
use Throwable;

/**
 * In-memory enrolment store with the production contract's idempotency and
 * conflict rules, so an application can test its lifecycle hooks and the
 * scheduled reconcile without a Signals instance.
 */
class FakeManagedIntegration implements ManagedIntegration
{
    /** @var array<string,ManagedEnrollment> */
    public array $enrollments = [];

    /** @var list<array{provider:string,owner_id:string,secret_length:int}> */
    public array $enrollCalls = [];

    /** @var list<array{provider:string,owner_id:string}> */
    public array $revokeCalls = [];

    private ?Throwable $failure = null;

    public function failWith(?Throwable $failure): self
    {
        $this->failure = $failure;

        return $this;
    }

    public function enroll(string $provider, ManagedEnvironment $environment, string $secret): ManagedEnrollment
    {
        $this->enrollCalls[] = ['provider' => $provider, 'owner_id' => $environment->ownerId, 'secret_length' => strlen($secret)];
        $this->raise();
        $key = $provider.'/'.$environment->ownerId;
        $now = new DateTimeImmutable;
        $current = $this->enrollments[$key] ?? null;
        $enrollment = new ManagedEnrollment($provider, $environment->ownerId, 'managed-'.str_replace('cbox_', '', $provider).'-'.$environment->ownerId, $environment->url, 1, EnrollmentStatus::Enrolled, $environment->importFrom, $current === null ? $now : $current->enrolledAt, $now, null, null, null);
        $this->enrollments[$key] = $enrollment;

        return $enrollment;
    }

    public function enrollment(string $provider, string $ownerId): ?ManagedEnrollment
    {
        $this->raise();

        return $this->enrollments[$provider.'/'.$ownerId] ?? null;
    }

    public function revoke(string $provider, string $ownerId): void
    {
        $this->revokeCalls[] = ['provider' => $provider, 'owner_id' => $ownerId];
        $this->raise();
        $key = $provider.'/'.$ownerId;
        $current = $this->enrollments[$key] ?? null;
        if ($current !== null) {
            $now = new DateTimeImmutable;
            $this->enrollments[$key] = new ManagedEnrollment($provider, $ownerId, $current->source, $current->url, $current->catalogVersion, EnrollmentStatus::Revoked, $current->importFrom, $current->enrolledAt, $now, $now, $current->pipelinesGeneration, $current->errorCode);
        }
    }

    private function raise(): void
    {
        if ($this->failure !== null) {
            throw $this->failure;
        }
    }
}
