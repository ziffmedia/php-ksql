<?php

namespace ZiffMedia\Ksql\Laravel;

use Illuminate\Console\Command;
use ZiffMedia\Ksql\Offset;
use ZiffMedia\Ksql\PullQuery;

class StreamCommand extends Command
{
    protected $signature = 'ksql:stream
                            {resourceName :  Specific resource to begin consuming}
                            {offset? : Override the specific offset}';

    protected $description = 'Consume KSQL streams and emit events';

    public function handle()
    {
        $client = app(Client::class);

        $resourceManager = app(ResourceManager::class);
        $resource = $resourceManager[$this->argument('resourceName')];

        $overrideOffset = $this->input->getArgument('offset');
        if ($overrideOffset) {
            $resource->offset = Offset::from($overrideOffset);
        }

        $query = new PushQuery($resource->getKeyName(), $resource->getKsqlStreamQuery(), fn () => null, $resource->offset);
        $query->event = $resource->getEventName();
        $query->tombstoneEvent = $resource->getTombstoneEventName();

        $client->streamAndEmit($query);
    }
}
