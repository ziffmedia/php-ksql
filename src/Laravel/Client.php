<?php

namespace ZiffMedia\Ksql\Laravel;

use ZiffMedia\Ksql\Client as KsqlClient;
use ZiffMedia\Ksql\PullQuery;
use ZiffMedia\Ksql\ResultRow;
use ZiffMedia\Ksql\TombstoneRow;

class Client extends KsqlClient
{
    /**
     * @param  PushQuery[]|PushQuery  $query
     */
    public function streamAndEmit(array|PushQuery $query): void
    {
        if (! is_array($query)) {
            $query = [$query];
        }

        $handler = function (ResultRow $row) {
            if ($row instanceof TombstoneRow) {
                event($row->query->tombstoneEvent, $row);
            } else {
                event($row->query->event, $row);
            }
        };

        foreach ($query as $q) {
            $q->handler = $handler;
        }
        $this->stream($query);
    }

    public function queryAndEmit(string|PullQuery $query, $event): void
    {
        $results = $this->query($query);
        foreach ($results as $result) {
            event($event, $result);
        }
    }
}
