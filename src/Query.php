<?php
namespace ZiffMedia\Ksql;
abstract class Query
{
    public string $queryId;
    public array $schema;
    public string $query;
}