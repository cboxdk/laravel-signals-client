<?php

declare(strict_types=1);

namespace Cbox\Signals\ApiActivity;

use Cbox\Signals\Exceptions\SignalsFailure;
use InvalidArgumentException;

/** The `signals.api_activity` configuration, checked once. */
final readonly class ApiActivitySettings
{
    /** @param array{project_id:string,environment_id:string,audience:string,tenant_id?:string}|null $scope */
    public function __construct(
        public bool $enabled,
        public string $referenceKey,
        public string $referenceKeyId,
        public string $applicationId,
        public string $namespace,
        public int $window = 600,
        public int $grace = 60,
        public int $maxActors = 500,
        public ?array $scope = null,
        public string $sourceId = '',
        public string $partition = '0',
        public string $generation = '',
        public string $connection = 'default',
        public string $prefix = 'signals:api',
        public int $shards = 8,
        public int $trackedActors = 2000,
        public int $lookback = 3600,
    ) {
        if ($window < 60 || $window > 3600 || $grace < 0 || $grace > 3600 || $maxActors < 1 || $maxActors > 10000) {
            throw new InvalidArgumentException('A window is 60 to 3600 seconds, the grace 0 to 3600 seconds and the actor limit 1 to 10000.');
        }
        if ($shards < 1 || $shards > 256 || $trackedActors < $maxActors || $trackedActors > 100000 || $lookback < $window + $grace || $lookback > 86400 || $prefix === '' || preg_match('/[{}\s]/', $prefix) === 1) {
            throw new InvalidArgumentException('Shards are 1 to 256, tracked actors at least the actor limit, the lookback at least one window and its grace, and the prefix has no braces or spaces.');
        }
        foreach ([$referenceKeyId, $applicationId, $namespace, $sourceId, $partition, $generation] as $text) {
            if (strlen($text) > 256) {
                throw new InvalidArgumentException('Identifiers are at most 256 characters.');
            }
        }
    }

    public static function fromConfig(mixed $config): self
    {
        $config = is_array($config) ? $config : [];
        $text = static fn (mixed $value): string => is_string($value) ? trim($value) : (is_int($value) ? (string) $value : '');
        $number = static fn (mixed $value, int $default): int => is_numeric($value) ? (int) $value : $default;
        $scope = null;
        $project = $text($config['project'] ?? null);
        $environment = $text($config['environment'] ?? null);
        if ($project !== '' && $environment !== '') {
            $tenant = $text($config['tenant'] ?? null);
            $scope = $tenant === ''
                ? ['project_id' => $project, 'environment_id' => $environment, 'audience' => 'environment']
                : ['project_id' => $project, 'environment_id' => $environment, 'audience' => 'tenant', 'tenant_id' => $tenant];
        }
        $connection = $text($config['connection'] ?? null);
        $settings = new self(
            enabled: filter_var($config['enabled'] ?? false, FILTER_VALIDATE_BOOL),
            referenceKey: $text($config['reference_key'] ?? null),
            referenceKeyId: $text($config['reference_key_id'] ?? null),
            applicationId: $text($config['application_id'] ?? null),
            namespace: $text($config['namespace'] ?? null),
            window: $number($config['window'] ?? null, 600),
            grace: $number($config['grace'] ?? null, 60),
            maxActors: $number($config['max_actors'] ?? null, 500),
            scope: $scope,
            sourceId: $text($config['source'] ?? null),
            partition: $text($config['partition'] ?? null) !== '' ? $text($config['partition'] ?? null) : '0',
            generation: $text($config['generation'] ?? null),
            connection: $connection !== '' ? $connection : 'default',
            prefix: $text($config['prefix'] ?? null) !== '' ? $text($config['prefix'] ?? null) : 'signals:api',
            shards: $number($config['shards'] ?? null, 8),
            trackedActors: $number($config['tracked_actors'] ?? null, 2000),
            lookback: $number($config['lookback'] ?? null, 3600),
        );

        return $settings;
    }

    /** Settings for the testing fake: every identifier set, a throwaway key. */
    public static function testing(int $window = 600, int $maxActors = 500): self
    {
        return new self(true, 'testing-reference-key', 'testing-key', 'testing-application', 'testing', $window, 0, $maxActors,
            ['project_id' => 'testing', 'environment_id' => 'testing', 'audience' => 'environment'], 'testing-source', '0', 'testing-generation');
    }

    /** Actor windows per shard: the actor limit is shared by the shards a window is counted in. */
    public function actorsPerShard(): int
    {
        return (int) ceil($this->maxActors / $this->shards);
    }

    /** Count calls only with a key to pseudonymize with and the identifiers every window needs. */
    public function counting(): bool
    {
        return $this->enabled && $this->referenceKey !== '' && $this->referenceKeyId !== '' && $this->applicationId !== '' && $this->namespace !== '';
    }

    /** The scope and source position every record carries; refused before anything is stored as a window. */
    public function sendable(): void
    {
        if (! $this->counting() || $this->scope === null || $this->sourceId === '' || $this->generation === '') {
            throw new SignalsFailure('not_configured', 503);
        }
    }

    /** A keyed pseudonym of 24 hexadecimal characters; the purpose keeps an address and a parameter set apart. */
    public function pseudonym(string $purpose, string $value): string
    {
        return substr(hash_hmac('sha256', $purpose."\0".$value, $this->referenceKey), 0, 24);
    }
}
