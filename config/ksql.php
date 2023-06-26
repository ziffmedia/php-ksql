<?php

use ZiffMedia\Ksql\ContentType;
use ZiffMedia\Ksql\Laravel\DiscoverResources;

return [
    'endpoint' => env('KSQL_ENDPOINT'),
    'auth' => [
        'username' => env('KSQL_USERNAME'),
        'password' => env('KSQL_PASSWORD'),
    ],
    'discover_resources' => DiscoverResources::CONSOLE,
    'register_commands' => env('KSQL_REGISTER_COMMANDS', false),
    'client_content_type' => ContentType::V1_JSON,
    'logging' => [
        'client' => env('KSQL_CLIENT_LOGGING', false),
    ],
];
