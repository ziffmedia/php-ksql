<?php
namespace ZiffMedia\Ksql;

class QueryResultRow extends \ArrayObject
{
    public function __get($key)
    {
        return $this[$key];
    }
}