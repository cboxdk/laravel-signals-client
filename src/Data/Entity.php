<?php

declare(strict_types=1);

namespace Cbox\Signals\Data;

readonly class Entity
{
    public function __construct(public string $namespace, public string $type, public string $id) {}

    /** @return array{namespace:string,type:string,id:string} */
    public function toArray(): array
    {
        return ['namespace' => $this->namespace, 'type' => $this->type, 'id' => $this->id];
    }
}
