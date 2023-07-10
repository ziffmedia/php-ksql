<?php

namespace ZiffMedia\Ksql\Laravel;

use Illuminate\Console\Command;
use ZiffMedia\Ksql\PullQuery;

class FetchCommand extends Command
{
    protected $signature = 'ksql:fetch
                            {resourceName : Specific resource to fill}
                            {resourceIds* : Specific resource ids to fill}';

    protected $description = 'Consume KSQL entire tables and emit events';

    public function handle()
    {
        $client = app(Client::class);

        $resourceManager = app(ResourceManager::class);
        $resource = $resourceManager[$this->argument('resourceName')];
        $query = new PullQuery($resource->getKsqlFetchQuery($this->argument('resourceIds')));
        $client->queryAndEmit($query, $resource->getEventName());
    }
}
