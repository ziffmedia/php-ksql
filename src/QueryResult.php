<?php
namespace ZiffMedia\Ksql;

use Exception;
use Traversable;

class QueryResult implements \IteratorAggregate, \Countable, \ArrayAccess
{
    public function __construct(
        public string $query,
        public string $queryId,
        public array $schema,
        public array $data,
    ) {}

    public function getIterator(): Traversable
    {
        return new \ArrayIterator($this->data);
    }

    public function count(): int
    {
        return count($this->data);
    }

    public function offsetExists(mixed $offset): bool
    {
        return isset($this->data);
    }

    public function offsetGet(mixed $offset): mixed
    {
        return $this->data[$offset];
    }

    public function offsetSet(mixed $offset, mixed $value): void
    {
        return; // read only
    }

    public function offsetUnset(mixed $offset): void
    {
        return; //read only
    }
}