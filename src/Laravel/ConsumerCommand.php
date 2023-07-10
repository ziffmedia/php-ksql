<?php

namespace ZiffMedia\Ksql\Laravel;

use Illuminate\Console\Command;
use ZiffMedia\Ksql\Offset;
use ZiffMedia\Ksql\PullQuery;

class ConsumerCommand extends Command
{
    protected $signature = 'ksql:consume
                            {resourceName? : Optional specific resource to begin consuming}
                            {offset? : Override the specific offset}';

    protected $description = 'Consume KSQL streams and emit events';

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
            if ($resource->shouldConsume && $resource->catchUpBeforeConsume) {
                $query = new PullQuery($resource->getKsqlCatchupQuery());
                $client->queryAndEmit($query, $resource->getEventName());
            }
        }

        $streamQueries = [];
        /** @var KsqlResource $resource */
        foreach ($resources as $resource) {
            if ($resource->shouldConsume) {
                $overrideOffset = $this->input->getArgument('offset');
                if ($overrideOffset) {
                    $resource->offset = Offset::from($overrideOffset);
                }

                $query = new PushQuery($resource->getKeyName(), $resource->getKsqlStreamQuery(), fn () => null, $resource->offset);
                $query->event = $resource->getEventName();
                $query->tombstoneEvent = $resource->getTombstoneEventName();
                $streamQueries[] = $query;
            }
        }

        $client->streamAndEmit($streamQueries);
    }
}
