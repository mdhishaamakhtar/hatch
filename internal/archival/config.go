package archival

import "time"

// Config is loaded once at boot via pkg/config.Load[Config]. The archival cron
// needs Postgres (to list/check/detach/export/drop partitions) and a directory
// to write the cold-storage CSV exports to.
//
// Interval defaults to the production monthly (~720h) cadence from the Build
// Plan; the dev cluster overrides it to a few seconds via the Helm chart so
// `make verify` and demos see a sweep promptly.
type Config struct {
	DatabaseURL string `env:"DATABASE_URL,required"`

	OTLPEndpoint string `env:"OTLP_ENDPOINT"`

	AdminPort int `env:"ARCHIVAL_ADMIN_PORT" envDefault:"9026"`

	// Interval between archival sweeps.
	Interval time.Duration `env:"ARCHIVAL_INTERVAL" envDefault:"720h"`

	// RunOnStart runs one sweep immediately at boot (before the first tick) so the
	// last-run gauge and active-partitions gauge appear without waiting a full
	// interval.
	RunOnStart bool `env:"ARCHIVAL_RUN_ON_START" envDefault:"true"`

	// ArchiveDir is where exported partitions are written as <name>.csv.gz. In the
	// dev cluster this is an emptyDir; in production it would be a PVC or an S3/GCS
	// sync target.
	ArchiveDir string `env:"ARCHIVE_DIR" envDefault:"/archive"`

	ShutdownTimeoutMS int `env:"ARCHIVAL_SHUTDOWN_MS" envDefault:"10000"`
}
