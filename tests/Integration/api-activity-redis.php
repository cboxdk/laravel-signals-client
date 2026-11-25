<?php

declare(strict_types=1);

// A separate process for tests/ApiActivityRedisTest.php: counts API calls or
// closes windows against the Redis in SIGNALS_TEST_REDIS, as an application
// server or a scheduler would.

require dirname(__DIR__, 2).'/vendor/autoload.php';

use Cbox\Signals\ApiActivity\ApiActivitySettings;
use Cbox\Signals\ApiActivity\ApiActor;
use Cbox\Signals\ApiActivity\ApiCall;
use Cbox\Signals\ApiActivity\RedisApiActivityLog;
use Cbox\Signals\Contracts\SourceRecords;
use Cbox\Signals\Data\SourceReceipt;
use Illuminate\Container\Container;
use Illuminate\Redis\RedisManager;

$url = parse_url((string) getenv('SIGNALS_TEST_REDIS'));
$redis = new RedisManager(new Container, 'phpredis', ['default' => ['host' => $url['host'] ?? '127.0.0.1', 'port' => $url['port'] ?? 6379, 'database' => (int) ltrim($url['path'] ?? '/15', '/')]]);
$settings = new ApiActivitySettings(true, 'secret-reference-key', 'key-1', 'status-api', 'cbox.id', 600, 0, 500,
    ['project_id' => 'project', 'environment_id' => 'production', 'audience' => 'environment'], 'status-api-activity', '0', 'g1',
    shards: 4, trackedActors: 2000);
$log = new RedisApiActivityLog($redis, $settings);
$start = 1793872800;

switch ($argv[1] ?? '') {
    case 'write': // write <worker> <calls>: calls spread over three windows, twenty users and 300 objects
        mt_srand((int) $argv[2]);
        $times = [];
        for ($i = 0; $i < (int) $argv[3]; $i++) {
            $window = $start + 600 * mt_rand(0, 2);
            $call = ApiCall::of($settings, new ApiActor(user: 'u'.mt_rand(0, 19), ip: '198.51.100.'.mt_rand(1, 50)), $window * 1000 + mt_rand(0, 599999), 'GET', '/v1/things/{thing}', [200, 200, 200, 404][mt_rand(0, 3)], ['thing' => (string) mt_rand(0, 299)], false);
            $t = hrtime(true);
            $log->record($call);
            $times[] = hrtime(true) - $t;
        }
        sort($times);
        $at = static fn (float $q): float => $times[(int) floor($q * (count($times) - 1))] / 1e6;
        echo json_encode(['calls' => count($times), 'p50_ms' => round($at(0.5), 3), 'p99_ms' => round($at(0.99), 3), 'max_ms' => round($at(1.0), 3)]), "\n";
        break;
    case 'flush': // flush <rounds> <file>: close and "send" again and again, appending accepted records to the file
        $source = new class($argv[3]) implements SourceRecords
        {
            public function __construct(private string $file) {}

            public function send(string $source, array $records): SourceReceipt
            {
                if ($records[0]['kind'] === 'coverage') {
                    return new SourceReceipt(1, 0, new DateTimeImmutable);
                }
                $lines = implode('', array_map(static fn (array $r): string => json_encode($r, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES)."\n", $records));
                file_put_contents($this->file, $lines, FILE_APPEND | LOCK_EX);

                return new SourceReceipt(count($records), 0, new DateTimeImmutable);
            }
        };
        $records = 0;
        for ($round = 0; $round < (int) $argv[2]; $round++) {
            $records += $log->flush($source, $start + 3 * 600 + 1)['records'];
            usleep(3000);
        }
        echo "records {$records}\n";
        break;
    default:
        fwrite(STDERR, "usage: write <worker> <calls> | flush <rounds> <file>\n");
        exit(2);
}
