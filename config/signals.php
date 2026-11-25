<?php

return [
    'url' => env('SIGNALS_URL'),
    'token' => env('SIGNALS_TOKEN'),
    'timeout' => env('SIGNALS_TIMEOUT', 2),
    'webhook_secret' => env('SIGNALS_WEBHOOK_SECRET'),
    // Bundled managed integration: the credential that identifies this
    // application towards the enrolment API. Applications usually bind a Cbox
    // ID machine-token implementation of ManagedCredential instead and leave
    // this empty; when set it falls back to `token` when empty.
    'managed' => ['token' => env('SIGNALS_MANAGED_TOKEN')],
    // API activity windows (contract 2.6.0, `api.activity`). The CountApiActivity
    // middleware counts only when enabled and the reference key, its id, the
    // application id and the entity namespace are set. `signals:api-activity`
    // also needs the project, environment, source and generation.
    'api_activity' => [
        'enabled' => env('SIGNALS_API_ACTIVITY', false),
        // Pseudonymizes IP addresses and path parameters. Keep it secret; give a
        // new id when you rotate it, because references under two ids never match.
        'reference_key' => env('SIGNALS_REFERENCE_KEY'),
        'reference_key_id' => env('SIGNALS_REFERENCE_KEY_ID'),
        'application_id' => env('SIGNALS_API_APPLICATION_ID'),
        // Namespace of the user, organization and token entities, as in the source grant.
        'namespace' => env('SIGNALS_API_NAMESPACE'),
        'project' => env('SIGNALS_PROJECT'),
        'environment' => env('SIGNALS_ENVIRONMENT'),
        'tenant' => env('SIGNALS_TENANT'),
        'source' => env('SIGNALS_API_SOURCE'),
        'partition' => env('SIGNALS_API_PARTITION', '0'),
        'generation' => env('SIGNALS_API_GENERATION'),
        // Window length in seconds (60 to 3600) and how long after its end calls may still arrive.
        // The coverage frame cannot pass a window's end before the grace is over, and Signals
        // reads the source as stale once that is 120 s old: keep the grace at 60 or less.
        'window' => env('SIGNALS_API_WINDOW', 600),
        'grace' => env('SIGNALS_API_GRACE', 60),
        // Actors with a window of their own; the rest are folded into remainder windows.
        'max_actors' => env('SIGNALS_API_MAX_ACTORS', 500),
        // Redis (or Valkey) connection every application server shares. Calls are counted there,
        // never in the database: one script call per request, after the response has been sent.
        // Use a dedicated connection with a short read_timeout.
        'connection' => env('SIGNALS_API_REDIS', 'default'),
        'prefix' => env('SIGNALS_API_PREFIX', 'signals:api'),
        // Each window is counted in this many Redis keys spaces (one cluster slot each), by actor.
        'shards' => env('SIGNALS_API_SHARDS', 8),
        // Actors counted one by one per window (all shards); later ones are counted per network.
        'tracked_actors' => env('SIGNALS_API_TRACKED_ACTORS', 2000),
        // How far back closed windows are checked for calls that arrived after they closed.
        'lookback' => env('SIGNALS_API_LOOKBACK', 3600),
    ],
];
