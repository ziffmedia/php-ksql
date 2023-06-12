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

    public bool $catchUpBeforeConsume = false;

    public bool $shouldConsume = true;

    public string $model;

    public Offset $offset = Offset::LATEST;

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

    public function getKsqlFillQuery($resourceIds = null): string
    {
        if ($resourceIds) {
            if (! ($resourceIds instanceof Collection)) {
                $resourceIds = collect($resourceIds);
            }

            return sprintf("SELECT * FROM %s WHERE %s IN ('%s');", $this->ksqlTable, $this->ksqlIdField, $resourceIds->implode("','"));
        } else {
            return sprintf('SELECT * FROM %s;', $this->ksqlTable);
        }
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

    public function getKsqlCatchupQuery(): string
    {
        $latestModel = $this->getLatestModel();
        $dateTime = new Carbon($latestModel->{$latestModel->getUpdatedAtColumn()});
        $dateTime->modify("-$this->lookback seconds");
        $isoString = $dateTime->toIso8601String();

        return sprintf("SELECT * FROM %s WHERE %s >= '%s'", $this->ksqlTable, $this->ksqlUpdatedField, $isoString);
    }

    private function getLatestModel(): Model
    {
        /** @var Model $stubModel */
        $stubModel = new $this->model;
        $updatedAtColumn = $stubModel->getUpdatedAtColumn();

        return $this->model::orderBy($updatedAtColumn, 'desc')->limit(1)->first();
    }
}
