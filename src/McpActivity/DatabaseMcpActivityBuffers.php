<?php

declare(strict_types=1);

namespace Cbox\Signals\McpActivity;

use Cbox\Signals\Contracts\McpActivityBuffers;
use Cbox\Signals\Data\Entity;
use Closure;
use Illuminate\Contracts\Redis\Factory;
use Illuminate\Database\ConnectionInterface;
use RuntimeException;
use UnexpectedValueException;

/** Durable fences and a database lock, not a Redis lease that can expire mid-send. */
class DatabaseMcpActivityBuffers implements McpActivityBuffers
{
    public function __construct(private readonly ConnectionInterface $db, private readonly Factory $redis) {}

    public function protect(Settings $settings, Closure $operation): mixed
    {
        $prefix = $settings->prefix();
        $connection = $settings->source->connection;
        // Register durably before touching Redis. A killed first dispatcher must
        // not leave a buffer that erasure cannot discover.
        $this->db->transaction(function () use ($settings, $prefix, $connection): void {
            $this->lock();
            $id = hash('sha256', json_encode([$connection, $prefix], JSON_THROW_ON_ERROR));
            $window = $this->db->table('signals_mcp_buffers')->where('id', $id)->value('window_seconds');
            if ($window === null) {
                if ($this->db->table('signals_mcp_buffers')->count() >= 128) {
                    throw new RuntimeException('MCP buffer registry is full; retire drained generations before adding another.');
                }
                $this->db->table('signals_mcp_buffers')->insert(['id' => $id, 'prefix' => $prefix, 'connection' => $connection, 'window_seconds' => $settings->source->window]);
            } elseif (! is_numeric($window) || (int) $window !== $settings->source->window) {
                // Do not apply a changed window's anonymous-erasure boundary
                // before Redis's configuration-identity check can reject it.
                throw new UnexpectedValueException('MCP buffer window changed; use a new generation.');
            }
        });

        return $this->db->transaction(function () use ($settings, $operation, $prefix, $connection): mixed {
            $this->lock();
            $erased = $this->actors();
            // Recheck restored data too. A revision cannot prove Redis was not restored.
            RedisMcpActivityLog::purgeErased($this->redis, $connection, $prefix, $erased, $settings->source->window);

            return $operation($erased);
        });
    }

    public function erase(Entity $entity): void
    {
        // This commit survives Redis outages. Every future protected operation
        // observes the fence, even when the collector is disabled or reconfigured.
        $this->db->transaction(function () use ($entity): void {
            $this->lock();
            $inserted = $this->db->table('signals_mcp_erasures')->insertOrIgnore(['fingerprint' => ErasedActors::fingerprint($entity)]);
            if ($inserted !== 0) {
                $this->db->table('signals_mcp_guard')->where('id', 1)->update(['anonymous_before_ms' => (int) floor(microtime(true) * 1000)]);
            }
            // SQL payload removal and proof withdrawal commit with the fence,
            // independently of the legacy Redis cleanup below.
            DatabaseMcpActivityLog::purgeErased($this->db, $this->actors());
        });
        $this->db->transaction(function (): void {
            $this->lock();
            $erased = $this->actors();
            foreach ($this->db->table('signals_mcp_buffers')->get() as $buffer) {
                if (! is_string($buffer->connection) || ! is_string($buffer->prefix) || ! is_numeric($buffer->window_seconds)) {
                    throw new RuntimeException('Invalid MCP buffer registry.');
                }
                RedisMcpActivityLog::purgeErased($this->redis, $buffer->connection, $buffer->prefix, $erased, (int) $buffer->window_seconds);
            }
        });
    }

    private function lock(): void
    {
        if ($this->db->table('signals_mcp_guard')->where('id', 1)->lockForUpdate()->first() === null) {
            throw new RuntimeException('MCP buffer privacy migrations are required.');
        }
    }

    private function actors(): ErasedActors
    {
        $fingerprints = [];
        foreach ($this->db->table('signals_mcp_erasures')->pluck('fingerprint') as $fingerprint) {
            if (! is_string($fingerprint)) {
                throw new RuntimeException('Invalid MCP erasure fingerprint.');
            }
            $fingerprints[$fingerprint] = true;
        }
        $before = $this->db->table('signals_mcp_guard')->where('id', 1)->value('anonymous_before_ms');
        if (! is_numeric($before)) {
            throw new RuntimeException('Invalid MCP anonymous erasure fence.');
        }

        return new ErasedActors($fingerprints, (int) $before);
    }
}
