<?php

declare(strict_types=1);

namespace Cbox\Signals\ApiActivity;

use InvalidArgumentException;

/**
 * Network and client context of an actor, as the application knows it. The raw
 * IP address is never part of it: the middleware sends only `ip_reference`.
 */
final readonly class ApiContext
{
    private const array EVIDENCE = ['first_party_cookie', 'authenticated_device', 'coarse_fingerprint', 'missing'];

    private const array NETWORKS = ['residential', 'mobile', 'corporate', 'hosting', 'vpn', 'tor', 'unknown'];

    public function __construct(
        public ?string $deviceId = null,
        public string $deviceEvidence = 'missing',
        public ?string $country = null,
        public ?int $asn = null,
        public ?string $browserFamily = null,
        public ?string $browserMajor = null,
        public ?string $osFamily = null,
        public string $networkClass = 'unknown',
        public ?float $geoAccuracyKm = null,
        public ?float $latitude = null,
        public ?float $longitude = null,
        public string $enrichmentVersion = 'unenriched',
    ) {
        foreach ([$deviceId, $browserFamily, $browserMajor, $osFamily, $enrichmentVersion] as $text) {
            if ($text !== null && ($text === '' || strlen($text) > 256)) {
                throw new InvalidArgumentException('Context text is 1 to 256 characters.');
            }
        }
        if (! in_array($deviceEvidence, self::EVIDENCE, true) || ($deviceId === null) !== ($deviceEvidence === 'missing')) {
            throw new InvalidArgumentException('A device id needs its evidence, and only a missing device has evidence "missing".');
        }
        if (! in_array($networkClass, self::NETWORKS, true) || ($country !== null && preg_match('/^[A-Z]{2}$/D', $country) !== 1) || ($asn !== null && ($asn < 0 || $asn > 4294967295))) {
            throw new InvalidArgumentException('Unknown network class, country or ASN.');
        }
        if (($latitude === null) !== ($longitude === null) || ($latitude !== null && ($geoAccuracyKm === null || abs($latitude) > 90 || abs((float) $longitude) > 180)) || ($geoAccuracyKm !== null && $geoAccuracyKm < 0)) {
            throw new InvalidArgumentException('Coordinates come as a pair with an accuracy.');
        }
    }

    /** The network context shared by every actor folded into a remainder window; nothing that names a device. */
    public function network(): self
    {
        return new self(country: $this->country, asn: $this->asn, networkClass: $this->networkClass, enrichmentVersion: $this->enrichmentVersion);
    }

    /** @return array<string,mixed> */
    public function toArray(): array
    {
        return [
            'device_id' => $this->deviceId,
            'device_evidence' => $this->deviceEvidence,
            'ip' => null,
            'country' => $this->country,
            'asn' => $this->asn,
            'browser_family' => $this->browserFamily,
            'browser_major' => $this->browserMajor,
            'os_family' => $this->osFamily,
            'network_class' => $this->networkClass,
            'geo_accuracy_km' => $this->geoAccuracyKm,
            'latitude' => $this->latitude,
            'longitude' => $this->longitude,
            'enrichment_version' => $this->enrichmentVersion,
        ];
    }
}
