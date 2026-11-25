<?php

declare(strict_types=1);

namespace Cbox\Signals\Console;

use Cbox\Signals\ApiActivity\ApiActivitySettings;
use Cbox\Signals\ApiActivity\RedisApiActivityLog;
use Cbox\Signals\Contracts\SourceRecords;
use Cbox\Signals\Exceptions\SignalsFailure;
use Illuminate\Console\Command;

class SendApiActivity extends Command
{
    protected $signature = 'signals:api-activity';

    protected $description = 'Close finished API activity windows in Redis, send them to Cbox Signals, and send a coverage frame';

    public function handle(RedisApiActivityLog $log, SourceRecords $source, ApiActivitySettings $settings): int
    {
        try {
            $settings->sendable();
        } catch (SignalsFailure) {
            $this->error('Set the reference key and its id, application, namespace, project, environment, source and generation under signals.api_activity.');

            return self::FAILURE;
        }
        $result = $log->flush($source, time());
        $coverage = $result['coverage'] === null ? 'no coverage frame was accepted' : "the coverage frame is {$result['coverage']}";
        $this->info("Closed {$result['parts']} window parts; Signals accepted {$result['records']} api.activity records, and {$coverage}. Refused records are kept in the Redis list {$settings->prefix}:failed.");

        return self::SUCCESS;
    }
}
