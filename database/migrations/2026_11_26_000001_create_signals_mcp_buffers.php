<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('signals_mcp_guard', function (Blueprint $table): void {
            $table->unsignedInteger('id')->primary();
            $table->unsignedBigInteger('anonymous_before_ms')->default(0);
        });
        DB::table('signals_mcp_guard')->insert(['id' => 1]);
        Schema::create('signals_mcp_erasures', function (Blueprint $table): void {
            $table->string('fingerprint', 64)->primary();
        });
        Schema::create('signals_mcp_buffers', function (Blueprint $table): void {
            $table->string('id', 64)->primary();
            $table->string('prefix', 512);
            $table->string('connection', 128);
            $table->unsignedInteger('window_seconds');
        });
        Schema::create('signals_mcp_sources', function (Blueprint $table): void {
            $table->string('id', 64)->primary();
            $table->string('identity', 64);
            $table->unsignedInteger('window_ms');
            $table->unsignedBigInteger('origin_ms');
            $table->unsignedBigInteger('materialized_ms');
            $table->unsignedBigInteger('clock_ms');
            $table->unsignedBigInteger('ticket')->default(0);
            $table->unsignedBigInteger('sequence')->default(0);
            $table->unsignedBigInteger('committed')->default(0);
            $table->boolean('commit_unknown')->default(false);
            $table->unsignedBigInteger('emitted')->default(0);
            $table->text('frame')->nullable();
            $table->text('acknowledged_frame')->nullable();
        });
        Schema::create('signals_mcp_windows', function (Blueprint $table): void {
            $table->string('source', 64);
            $table->unsignedBigInteger('start_ms');
            $table->unsignedBigInteger('admitted')->default(0);
            $table->unsignedBigInteger('sealed')->default(0);
            $table->unsignedBigInteger('records')->default(0);
            $table->string('reason', 64)->nullable();
            $table->boolean('reported')->default(false);
            $table->primary(['source', 'start_ms']);
        });
        Schema::create('signals_mcp_attempts', function (Blueprint $table): void {
            $table->string('id', 32)->primary();
            $table->string('source', 64)->index();
            $table->unsignedBigInteger('start_ms');
            $table->unsignedBigInteger('window_ms');
            $table->unsignedBigInteger('ticket');
            $table->text('payload');
            $table->string('outcome', 32)->nullable();
            $table->boolean('replayed')->default(false);
            foreach (['subject', 'organization', 'token'] as $entity) {
                $table->string($entity.'_hash', 64)->nullable()->index();
            }
        });
        Schema::create('signals_mcp_parts', function (Blueprint $table): void {
            $table->string('id', 64)->primary();
            $table->string('source', 64);
            $table->unsignedBigInteger('start_ms');
            $table->unsignedBigInteger('sequence');
            $table->unsignedBigInteger('attempts');
            $table->text('payload');
            $table->string('payload_hash', 64);
            $table->boolean('acknowledged')->default(false);
            foreach (['subject', 'organization', 'token'] as $entity) {
                $table->string($entity.'_hash', 64)->nullable()->index();
            }
            $table->unique(['source', 'sequence']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('signals_mcp_parts');
        Schema::dropIfExists('signals_mcp_attempts');
        Schema::dropIfExists('signals_mcp_windows');
        Schema::dropIfExists('signals_mcp_sources');
        Schema::dropIfExists('signals_mcp_buffers');
        Schema::dropIfExists('signals_mcp_erasures');
        Schema::dropIfExists('signals_mcp_guard');
    }
};
