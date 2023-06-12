<?php

namespace ZiffMedia\Ksql\Laravel;

use ZiffMedia\Ksql\PushQuery as KsqlPushQuery;

class PushQuery extends KsqlPushQuery
{
    public string $event;

    public string $tombstoneEvent;
}
