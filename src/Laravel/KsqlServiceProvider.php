<?php

namespace ZiffMedia\Ksql\Laravel;

use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use SplFileInfo;
use Symfony\Component\Finder\Finder;
use ZiffMedia\Ksql\ContentType;

class KsqlServiceProvider extends ServiceProvider
{
    public function register()
    {
        $this->app->singleton(ResourceManager::class, fn () => new ResourceManager());

        if ($this->app->runningInConsole()) {
            $this->commands([
                ConsumerCommand::class,
                FillCommand::class,
            ]);
            if (config('ksql.discover_resources') == DiscoverResources::CONSOLE) {
                $this->discoverResources();
            }
        }

        if (config('ksql.discover_resources') == DiscoverResources::ALWAYS) {
            $this->discoverResources();
        }

        $this->app->bind(Client::class, function () {
            $client = new Client(
                config('ksql.endpoint'),
                config('ksql.auth.username'),
                config('ksql.auth.password')
            );
            $client->setAcceptContentType(config('ksql.client_content_type', ContentType::V1_DELIMITTED));
            if (config('ksql.logging.client')) {
                $client->setLogger(logger()->getLogger());
            }

            return $client;
        });
    }

    public function boot()
    {
        $this->publishes([
            __DIR__.'/../config/ksql.php' => config_path('ksql.php'),
        ], 'config');
    }

    private function discoverResources()
    {
        $ksqlResourcesPath = app_path('Ksql');
        $finder = new Finder();
        collect($finder->in($ksqlResourcesPath)->files()->name('*.php'))->each(function (SplFileInfo $file) {
            $namespace = 'App\Ksql';
            $strippedBaseName = str_replace('.php', '', $file->getBasename());
            $className = $namespace.'\\'.$strippedBaseName;
            /** @var KsqlResource $instance */
            $instance = new $className;
            app(ResourceManager::class)->{$instance->getKeyName()} = $instance;
            Event::listen($instance->getEventName(), [$instance, 'handle']);
            Event::listen($instance->getTombstoneEventName(), [$instance, 'handleTombstone']);
        });
    }
}
