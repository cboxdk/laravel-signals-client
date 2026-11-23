<?php

use Cbox\Signals\ApiActivity\ApiActivitySettings;
use Cbox\Signals\ApiActivity\ApiActor;
use Cbox\Signals\ApiActivity\ApiContext;
use Cbox\Signals\ApiActivity\CountApiActivity;
use Cbox\Signals\ApiActivity\WindowBuilder;
use Cbox\Signals\Contracts\ApiActivityLog;
use Cbox\Signals\Contracts\ApiActorResolver;
use Cbox\Signals\SignalsServiceProvider;
use Cbox\Signals\Testing\FakeApiActivity;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Foundation\Auth\User;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;
use Orchestra\Testbench\TestCase;

uses(TestCase::class);

beforeEach(function (): void {
    $this->app->register(SignalsServiceProvider::class);
});

function ownRecords(WindowBuilder $builder): array
{
    $records = $builder->records('1', 1, WINDOW_START + 700);
    foreach ($records as $record) {
        expect(apiActivityErrors($record))->toBe([], $record['id']);
        expect(strlen(json_encode($record, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES)))->toBeLessThanOrEqual(65536);
    }

    return $records;
}

it('counts a request through the middleware by route template, token id and pseudonyms only', function (): void {
    $fake = FakeApiActivity::swap($this->app);
    $this->app->instance(ApiActorResolver::class, new class implements ApiActorResolver
    {
        public function resolve(Request $request): ApiActor
        {
            return new ApiActor(user: 'user-7', organization: 'org-3', token: 'token-db-id-9', ip: $request->ip());
        }
    });
    Route::middleware(CountApiActivity::class)->get('/v1/incidents/{incident}', fn (string $incident) => $incident === 'missing' ? abort(404) : ['id' => $incident]);
    Route::middleware(CountApiActivity::class)->get('/v1/components', fn () => []);
    Route::get('/health', fn () => 'ok');

    $this->getJson('/v1/components', ['Authorization' => 'Bearer plain-secret-token'])->assertOk();
    $this->getJson('/v1/incidents/inc-42')->assertOk();
    $this->getJson('/v1/incidents/inc-42')->assertOk();
    $this->getJson('/v1/incidents/inc-43?incident=inc-99')->assertOk();
    $this->getJson('/v1/incidents/missing')->assertNotFound();
    $this->get('/health')->assertOk();
    $this->getJson('/no-such-route')->assertNotFound();

    expect($fake->calls)->toHaveCount(5);
    $records = $fake->records();
    expect($records)->toHaveCount(1);
    expect(apiActivityErrors($records[0]))->toBe([]);
    $wire = json_encode($records, JSON_THROW_ON_ERROR);
    expect($wire)->not->toContain('inc-42')->not->toContain('127.0.0.1')->not->toContain('plain-secret-token')->not->toContain('no-such-route');
    $data = $records[0]['data'];
    expect($data['grouping'])->toBe('actor')
        ->and($data['token'])->toBe(['namespace' => 'testing', 'type' => 'token', 'id' => 'token-db-id-9'])
        ->and($data['ip_reference'])->toBe($fake->settings->pseudonym('ip', '127.0.0.1'))
        ->and(array_column($data['operations'], 'route'))->toBe(['/v1/components', '/v1/incidents/{incident}']);
    $incident = $data['operations'][1];
    expect($incident['responses'])->toMatchArray(['success' => 3, 'client_error' => 1, 'not_found' => 1])
        ->and($incident['parameters'])->toMatchArray(['overflow' => 0, 'conflicts' => 1])
        ->and($incident['parameters']['references'])->toHaveCount(2)
        ->and($data['sequence'])->toHaveCount(2)
        ->and($data['sequence'][1]['repeat'])->toBe(4);
    $fake->assertSent(fn (array $w): bool => $w['subject']['id'] === 'user-7', 1);
});

it('never counts without a reference key and never breaks the request when counting fails', function (): void {
    $this->app->instance(ApiActivitySettings::class, ApiActivitySettings::fromConfig(['enabled' => true, 'reference_key_id' => 'k', 'application_id' => 'a', 'namespace' => 'n']));
    $fake = new FakeApiActivity;
    $this->app->instance(ApiActivityLog::class, $fake);
    Route::middleware(CountApiActivity::class)->get('/v1/a', fn () => []);
    $this->getJson('/v1/a')->assertOk();
    $fake->assertNothingSent();

    FakeApiActivity::swap($this->app);
    $this->app->instance(ApiActorResolver::class, new class implements ApiActorResolver
    {
        public function resolve(Request $request): ApiActor
        {
            throw new RuntimeException('resolver down');
        }
    });
    $this->getJson('/v1/a')->assertOk();
});

it('uses the authenticated user and the database id of its token by default', function (): void {
    $fake = FakeApiActivity::swap($this->app);
    $token = (new class extends Model
    {
        protected $guarded = [];
    })->forceFill(['id' => 77, 'token' => 'hashed-secret']);
    $user = new class($token) extends User
    {
        public function __construct(private ?object $model = null)
        {
            parent::__construct();
            $this->forceFill(['id' => 41]);
        }

        public function currentAccessToken(): ?object
        {
            return $this->model;
        }
    };
    Route::middleware(CountApiActivity::class)->get('/v1/me', fn () => []);
    $this->actingAs($user)->getJson('/v1/me')->assertOk();
    $data = $fake->windows()[0];
    expect($data['subject']['id'])->toBe('41')->and($data['token']['id'])->toBe('77')->and($data['organization'])->toBeNull();
});

it('keeps every window within the limits and counts what did not fit', function (): void {
    $s = settings();
    $builder = new WindowBuilder($s, WINDOW_START, 600);
    $offset = 0;
    // 40 operations: the 8 least used fall into omitted_requests.
    for ($i = 0; $i < 40; $i++) {
        for ($n = 0; $n < ($i < 32 ? 3 : 1); $n++) {
            $builder->add(call($s, $offset++, "/v1/things{$i}/{thing}", 200, ['thing' => "x{$i}-{$n}"]));
        }
    }
    // 300 distinct objects on each of four routes, called in turn: 1200 runs, 1200 candidate references.
    for ($i = 0; $i < 300; $i++) {
        foreach (range(0, 3) as $route) {
            $builder->add(call($s, $offset++, "/v1/things{$route}/{thing}", 200, ['thing' => "object-{$i}"]));
        }
    }
    $records = ownRecords($builder);
    expect($records)->toHaveCount(1);
    $data = $records[0]['data'];
    $operations = $data['operations'];
    expect($operations)->toHaveCount(32)
        ->and($data['omitted_requests'])->toBe(8)
        ->and(array_sum(array_map(fn ($o) => count($o['parameters']['references']), $operations)))->toBe(768)
        // The order is known up to the first call to an operation that is not listed.
        ->and($data['sequence'])->toHaveCount(32)
        ->and(array_sum(array_column($data['sequence'], 'repeat')))->toBe(96);
    foreach ($operations as $o) {
        $answered = $o['responses']['success'];
        // Every answered request is either a kept reference's request or counted in overflow.
        expect(count($o['parameters']['references']) + $o['parameters']['overflow'])->toBeLessThanOrEqual($answered);
    }
    $total = array_sum(array_map(fn ($o) => array_sum(array_slice($o['responses'], 0, 4)), $operations));
    expect($total + $data['omitted_requests'])->toBe(32 * 3 + 8 + 1200);
    // 28 routes keep their 3 references; the four busy ones share the remaining 684.
    expect(array_map(fn ($o) => count($o['parameters']['references']), array_slice($operations, 0, 4)))->toBe([171, 171, 171, 171]);

    $alternating = new WindowBuilder($s, WINDOW_START, 600);
    for ($i = 0; $i < 600; $i++) {
        $alternating->add(call($s, $i, '/v1/'.($i % 2 ? 'a' : 'b')));
    }
    $sequence = ownRecords($alternating)[0]['data']['sequence'];
    expect($sequence)->toHaveCount(256)->and(array_sum(array_column($sequence, 'repeat')))->toBe(256);
});

it('folds actors beyond the limit, and actors with no identity, into remainders that name no one', function (): void {
    $s = settings(maxActors: 2);
    $builder = new WindowBuilder($s, WINDOW_START, 600);
    $offset = 0;
    foreach (range(1, 5) as $i) {
        $actor = new ApiActor(ip: "198.51.100.{$i}", context: new ApiContext(country: 'DK', asn: 64500, networkClass: 'hosting', enrichmentVersion: 'e1'));
        foreach (range(1, 6 - $i) as $n) {
            $builder->add(call($s, $offset++, '/v1/incidents/{incident}', 404, ['incident' => "i{$n}"], $actor));
        }
    }
    $builder->add(call($s, $offset++, '/v1/public', 200, [], new ApiActor));
    $records = ownRecords($builder);
    $windows = array_column($records, 'data');
    $actors = array_values(array_filter($windows, fn ($w) => $w['grouping'] === 'actor'));
    $remainders = array_values(array_filter($windows, fn ($w) => $w['grouping'] === 'remainder'));
    expect($actors)->toHaveCount(2)
        ->and(array_map(fn ($w) => $w['operations'][0]['responses']['not_found'], $actors))->toBe([5, 4])
        ->and($remainders)->toHaveCount(2)
        ->and(array_sum(array_column($remainders, 'actors')))->toBe(4);
    $folded = array_values(array_filter($remainders, fn ($w) => $w['context']['asn'] === 64500))[0];
    expect($folded['actors'])->toBe(3)->and($folded['operations'][0]['responses']['not_found'])->toBe(6)
        ->and($folded['ip_reference'])->toBeNull()->and($folded['sequence'])->toBe([]);
    // Twenty networks: the 15 busiest keep a remainder each, the rest share one per enrichment version
    // that keeps only the context its networks share.
    $many = new WindowBuilder($s, WINDOW_START, 600);
    $offset = 0;
    foreach (range(0, 19) as $i) {
        $context = new ApiContext(country: 'DK', asn: 65000 + $i, networkClass: $i === 18 ? 'residential' : 'hosting', enrichmentVersion: $i < 19 ? 'geo-1' : 'geo-2');
        foreach (range(0, 25 - $i) as $n) {
            $many->add(call($s, $offset++, '/v1/public', 200, [], new ApiActor(ip: "203.0.113.{$i}", context: $context)));
        }
    }
    // The two busiest have a window of their own (max_actors 2).
    $remainders = array_values(array_filter(array_column(ownRecords($many), 'data'), fn ($w) => $w['grouping'] === 'remainder'));
    expect($remainders)->toHaveCount(17)->and(array_sum(array_column($remainders, 'actors')))->toBe(18);
    $byEnrichment = [];
    foreach (array_filter($remainders, fn ($w) => $w['context']['asn'] === null || $w['context']['enrichment_version'] === 'geo-2') as $w) {
        $byEnrichment[$w['context']['enrichment_version']] = [$w['actors'], $w['context']['country'], $w['context']['network_class']];
    }
    ksort($byEnrichment);
    expect($byEnrichment)->toBe(['geo-1' => [2, 'DK', 'unknown'], 'geo-2' => [1, 'DK', 'hosting']]);
});

it('registers the middleware dependencies and the scheduled command', function (): void {
    config()->set('signals.api_activity', ['enabled' => true, 'reference_key' => 'k', 'reference_key_id' => 'k1', 'application_id' => 'a', 'namespace' => 'n']);
    $this->app->register(SignalsServiceProvider::class, true);
    expect($this->app->make(CountApiActivity::class))->toBeInstanceOf(CountApiActivity::class);
    $this->artisan('signals:api-activity')->assertFailed();
});
