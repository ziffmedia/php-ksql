<?php

namespace ZiffMedia\Ksql;

class PullQuery extends Query
{
    public function __construct(public string $query)
    {
    }
}
