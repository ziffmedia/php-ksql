<?php

namespace ZiffMedia\Ksql\Laravel;

use ZiffMedia\Ksql\Client as KsqlClient;
use ZiffMedia\Ksql\PullQuery;
use ZiffMedia\Ksql\ResultRow;
use ZiffMedia\Ksql\TombstoneRow;

class Client extends KsqlClient
{
    public function streamAndEmit(PushQuery $query): void
    {
        $query->handler = function (ResultRow $row) {
            dd($row);
            if ($row instanceof TombstoneRow) {
                event($row->query->tombstoneEvent, $row);
            } else {
                event($row->query->event, $row);
            }
        };

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
