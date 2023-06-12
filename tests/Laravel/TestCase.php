<?php

namespace Laravel;

use Orchestra\Testbench\TestCase as Orchestra;
use ZiffMedia\Ksql\Laravel\KsqlServiceProvider;

class TestCase extends Orchestra
{
    protected function getPackageProviders($app)
    {
        return [
            KsqlServiceProvider::class,
        ];
    }
}
