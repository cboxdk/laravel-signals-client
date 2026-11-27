<?php

use Cbox\Signals\ApiActivity\ApiActivitySettings;
use Cbox\Signals\ApiActivity\ApiActor;
use Cbox\Signals\McpActivity\Attempt;
use Cbox\Signals\McpActivity\Catalog;
use Cbox\Signals\McpActivity\FinalizedAttempt;
use Cbox\Signals\McpActivity\Operation;
use Cbox\Signals\McpActivity\Outcome;
use Cbox\Signals\McpActivity\Settings;
use Cbox\Signals\McpActivity\Window;

it('captures only catalogued operation and complete object sets and separates keys and scopes', function () {
    $settings = mcpSettings();
    $actor = new ApiActor(user: 'u1', ip: '192.0.2.42');
    $capture = fn ($input, $tool = 'pages', $action = 'getPage') => Attempt::capture($settings, $actor, WINDOW_START * 1000, $tool, $action, $input);
    $a = $capture(['page_id' => 'page-one', 'password' => 'do-not-store']);
    expect($a->reference)->toHaveLength(24)->toBe($capture(['page_id' => 'page-one'])->reference)
        ->not->toBe($capture(['page_id' => 'page-two'])->reference)
        ->and($capture(['page_id' => 123])->reference)->toBeNull()
        ->and($capture(['page_id' => ['x']])->reference)->toBeNull()
        ->and($capture(['page_id' => 'page-one'], 'user-input-name')->operation->tool)->toBe('unknown_tool')
        ->and($capture([], 'pages', 'invented')->operation->action)->toBe('unknown_action')
        ->and($a->json())->not->toContain('password', 'do-not-store', '192.0.2.42', 'page-one')
        ->and(Attempt::fromJson($a->json())->json())->toBe($a->json());
    // Independently specified canonical message, including a null tenant.
    $message = '["mcp.object.v1","testing","testing","environment",null,"testing-application","catalog-1","pages","getPage","page-v1",[["page_id","page-one"]]]';
    expect($a->reference)->toBe(substr(hash_hmac('sha256', $message, 'testing-reference-key'), 0, 24));
    expect(fn () => new Catalog('v1', [new Operation('unknown_tool', null)]))->toThrow(InvalidArgumentException::class);
});

it('bounds object coverage and keeps a start-order prefix with explicit omissions', function () {
    $s = mcpSettings();
    $actor = new ApiActor(user: 'u1');
    $first = Attempt::capture($s, $actor, WINDOW_START * 1000, 'pages', 'getPage', ['page_id' => 'object-0']);
    $window = new Window($s, WINDOW_START, $first->actorJson);
    for ($i = 0; $i < 1030; $i++) {
        $attempt = Attempt::capture($s, $actor, WINDOW_START * 1000 + $i, 'pages', 'getPage', $i === 1029 ? [] : ['page_id' => 'object-'.($i % 257)]);
        $window->add(new FinalizedAttempt($attempt, Outcome::Denied, false, $i));
    }
    $record = $window->record('synthetic', WINDOW_START + 600);
    $op = $record['data']['operations'][0];
    expect($op['attempts'])->toBe(1030)->and($op['outcomes']['denied'])->toBe(1030)
        ->and($op['objects']['references'])->toHaveCount(256)
        ->and($op['objects']['overflow'])->toBe(4)
        ->and($op['objects']['captured'])->toBe(1029)->and($op['objects']['missing'])->toBe(1)
        ->and($record['data']['sequence'])->toBe([['operation' => 0, 'offset_ms' => 0, 'repeat' => 1024]])
        ->and($record['data']['sequence_omitted'])->toBe(6)
        ->and(strlen(json_encode($record)))->toBeLessThan(65536);
});

it('does not invent sequence adjacency after an omitted operation', function () {
    $operations = array_map(fn ($i) => new Operation('tool'.$i), range(0, 32));
    $s = new Settings(ApiActivitySettings::testing(), new Catalog('v1', $operations));
    $actor = new ApiActor(user: 'u1');
    $window = null;
    foreach ([...range(0, 32), 0] as $i => $tool) {
        $attempt = Attempt::capture($s, $actor, WINDOW_START * 1000 + $i, 'tool'.$tool, null, []);
        $window ??= new Window($s, WINDOW_START, $attempt->actorJson);
        $window->add(new FinalizedAttempt($attempt, Outcome::Success, false, $i));
    }
    $data = $window->record('synthetic', WINDOW_START + 600)['data'];
    expect($data['total_attempts'])->toBe(34)->and($data['operations'])->toHaveCount(32)
        ->and($data['omitted_attempts'])->toBe(1)->and($data['sequence'])->toHaveCount(32)->and($data['sequence_omitted'])->toBe(2);
});
