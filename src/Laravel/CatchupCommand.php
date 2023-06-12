<?php

namespace ZiffMedia\Ksql\Laravel;

use Illuminate\Console\Command;
use ZiffMedia\Ksql\PullQuery;

class CatchupCommand extends Command
{
    protected $signature = 'ksql:catchup
                            {resourceName? : Optional specific resource to catch up}';

    protected $description = 'Consume KSQL tables based on deltas in updated_at and emit events';

    public function handle()
    {
        $client = app(Client::class);

        $resourceManager = app(ResourceManager::class);
        if ($resourceName = $this->argument('resourceName')) {
            $resources = [$resourceManager->$resourceName];
        } else {
            $resources = $resourceManager;
        }

        /** @var KsqlResource $resource */
        foreach ($resources as $resource) {
            $query = new PullQuery($resource->getKsqlCatchupQuery());
            $client->queryAndEmit($query, $resource->getEventName());
        }
    }
}
