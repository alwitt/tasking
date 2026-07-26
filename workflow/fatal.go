package workflow

import (
	"time"

	"github.com/alwitt/tasking/models"
	"github.com/apex/log"
)

// defaultOnFatal is the OnFatal callback used when the caller supplies none. It preserves the
// pre-callback behavior: log the fault and terminate the process via log.Fatal.
func defaultOnFatal(reporter string, err error, timestamp time.Time) {
	log.
		WithError(err).
		WithField("reporter", reporter).
		WithField("timestamp", timestamp).
		Fatalf("Fatal error in %s:\n%+v", reporter, err)
}

// ensure the default satisfies the shared callback signature.
var _ models.OnFatalCB = defaultOnFatal
