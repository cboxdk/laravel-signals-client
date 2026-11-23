<?php

declare(strict_types=1);

namespace Cbox\Signals\ApiActivity;

use InvalidArgumentException;

/**
 * Who made an API call, as the application decides it. User, organization and
 * token are the application's own opaque ids (for a token its database id,
 * never the secret). `ip` is the raw client address; the middleware turns it
 * into a keyed pseudonym before anything is stored or sent.
 */
final readonly class ApiActor
{
    public function __construct(
        public ?string $user = null,
        public ?string $organization = null,
        public ?string $token = null,
        public ?string $ip = null,
        public ApiContext $context = new ApiContext,
        public string $organizationType = 'organization',
    ) {
        foreach ([$user, $organization, $token] as $id) {
            if ($id !== null && ($id === '' || strlen($id) > 256)) {
                throw new InvalidArgumentException('Actor ids are 1 to 256 characters.');
            }
        }
        if (! in_array($organizationType, ['organization', 'customer'], true)) {
            throw new InvalidArgumentException('An organization is an "organization" or a "customer".');
        }
    }
}
