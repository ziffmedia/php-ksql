<?php

namespace ZiffMedia\Ksql\Laravel;

use Carbon\Carbon;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Collection;
use Illuminate\Support\Str;
use ZiffMedia\Ksql\Offset;
use ZiffMedia\Ksql\ResultRow;
use ZiffMedia\Ksql\TombstoneRow;

class KsqlResource
{
    public string $ksqlTable;

    public string $ksqlUpdatedField = 'updated_at';

    public string $ksqlIdField = 'id';

    public string $model;

    public Offset $offset = Offset::LATEST;

    public bool $catchupOnEarliest = false;

    /** @var int seconds to look back for catchup */
    public int $lookback = 300;

    public function handle(ResultRow $data): void
    {
    }

    public function handleTombstone(TombstoneRow $data): void
    {
    }

    public function getKsqlStreamQuery(): string
    {
        return sprintf('SELECT * FROM %s EMIT CHANGES;', $this->ksqlTable);
    }

    public function getKsqlFetchQuery($resourceIds): string
    {
        if (! ($resourceIds instanceof Collection)) {
            $resourceIds = collect($resourceIds);
        }

        return sprintf("SELECT * FROM %s WHERE %s IN ('%s');", $this->ksqlTable, $this->ksqlIdField, $resourceIds->implode("','"));
    }

    public function getKeyName(): string
    {
        return Str::snake(last(explode('\\', get_class($this))));
    }

    public function getEventName(): string
    {
        return 'ksql.'.$this->getKeyName();
    }

    public function getTombstoneEventName(): string
    {
        return $this->getEventName().'.tombstone';
    }

    private function getLatestModel(): Model
    {
        /** @var Model $stubModel */
        $stubModel = new $this->model;
        $updatedAtColumn = $stubModel->getUpdatedAtColumn();

        return $this->model::orderBy($updatedAtColumn, 'desc')->limit(1)->first();
    }
}
