<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

use Cbox\Signals\ApiActivity\ApiActor;
use Cbox\Signals\Support\Wire;

/** Sanitized journal entry. Raw arguments, results and IP never enter the journal. */
readonly class Attempt
{
    public function __construct(
        public int $startedMs,
        public Operation $operation,
        public string $actorJson,
        public ?string $reference,
    ) {}

    /** $input is transient; only the catalog's exact scalar object-field set may be hashed.
     * @param  array<array-key,mixed>  $input
     */
    public static function capture(Settings $settings, ApiActor $actor, int $startedMs, mixed $tool, mixed $action, array $input): self
    {
        $operation = $settings->catalog->resolve($tool, $action);
        $pairs = [];
        $fields = $operation->objectFields;
        sort($fields, SORT_STRING);
        foreach ($fields as $field) {
            $value = $input[$field] ?? null;
            if (! is_string($value) || $value === '' || ! mb_check_encoding($value, 'UTF-8')) {
                $pairs = [];
                break;
            }
            $pairs[] = [$field, $value];
        }
        $ref = $pairs === [] ? null : $settings->reference(['mcp.object.v1', ...$settings->domain(), $settings->catalog->revision, $operation->tool, $operation->action, $operation->policy, $pairs]);
        $entity = static fn (?string $id, string $type): ?array => $id === null ? null : ['namespace' => $settings->source->namespace, 'type' => $type, 'id' => $id];
        $ip = null;
        if ($actor->ip !== null && filter_var($actor->ip, FILTER_VALIDATE_IP) !== false) {
            $packed = inet_pton($actor->ip);
            if ($packed !== false) {
                $ip = $settings->reference(['mcp.ip.v1', ...$settings->domain(), inet_ntop($packed)]);
            }
        }
        $wire = ['subject' => $entity($actor->user, 'user'), 'organization' => $entity($actor->organization, $actor->organizationType), 'token' => $entity($actor->token, 'token'), 'context' => $actor->context->toArray(), 'ip_reference' => $ip];

        return new self($startedMs, $operation, json_encode($wire, JSON_THROW_ON_ERROR), $ref);
    }

    public function json(): string
    {
        return json_encode(['started' => $this->startedMs, 'tool' => $this->operation->tool, 'action' => $this->operation->action, 'policy' => $this->operation->policy, 'fields' => $this->operation->objectFields, 'actor' => $this->actorJson, 'reference' => $this->reference], JSON_THROW_ON_ERROR);
    }

    public static function fromJson(string $json): self
    {
        $v = Wire::object(json_decode($json, true, flags: JSON_THROW_ON_ERROR));
        $policy = $v['policy'] === null ? null : Wire::text($v['policy']);

        return new self(Wire::integer($v['started']), new Operation(Wire::text($v['tool']), $v['action'] === null ? null : Wire::text($v['action']), $policy, Wire::strings($v['fields'])), Wire::text($v['actor']), $v['reference'] === null ? null : Wire::text($v['reference']));
    }
}
