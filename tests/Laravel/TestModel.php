<?php

class TestModel extends \Illuminate\Database\Eloquent\Model
{
    use \Sushi\Sushi;

    protected array $rows = [
        ['id' => 1, 'updated_at' => '2023-01-01 12:00:00'],
    ];
}
