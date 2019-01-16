package v1alpha1

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestIsInScheduledPausePeriod(t *testing.T) {
	r := require.New(t)

	_, err := validateClock("06:00:00")
	r.Error(err)

	_, err = validateClock("06:00:00+08:00")
	r.NoError(err)

	shanghaiLoc, err := time.LoadLocation("Asia/Shanghai")
	r.NoError(err)

	newYorkLoc, err := time.LoadLocation("America/New_York")
	r.NoError(err)

	var cases = []struct{
		name string
		PeriodSpec  ScheduledPausePeriod
		CurrentTime time.Time
		expected    bool
	}{
		{
			name: "case 1",
			PeriodSpec:  ScheduledPausePeriod{StartClock: "06:00:00+08:00", EndClock: "10:00:00+08:00"},
			CurrentTime: time.Date(2019, 1, 16, 5, 0, 0, 0, shanghaiLoc),
			expected:    false,
		},
		{
			name: "case 2",
			PeriodSpec:  ScheduledPausePeriod{StartClock: "06:00:00+08:00", EndClock: "10:00:00+08:00"},
			CurrentTime: time.Date(2019, 1, 16, 7, 0, 0, 0, shanghaiLoc),
			expected:    true,
		},
		{
			name: "case 3",
			PeriodSpec:  ScheduledPausePeriod{StartClock: "06:00:00+08:00", EndClock: "10:00:00+08:00"},
			CurrentTime: time.Date(2019, 1, 15, 7, 0, 0, 0, shanghaiLoc),
			expected:    true,
		},
		// The same as above, just use a different timezone
		// 06:00:00+08:00 --> 17:00:00-05:00
		// 10:00:00+08:00 --> 21:00:00-05:00
		{
			name: "case 4",
			PeriodSpec:  ScheduledPausePeriod{StartClock: "06:00:00+08:00", EndClock: "10:00:00+08:00"},
			CurrentTime: time.Date(2019, 1, 16, 16, 0, 0, 0, newYorkLoc),
			expected:    false,
		},
		{
			name: "case 5",
			PeriodSpec:  ScheduledPausePeriod{StartClock: "06:00:00+08:00", EndClock: "10:00:00+08:00"},
			CurrentTime: time.Date(2019, 1, 16, 18, 0, 0, 0, newYorkLoc),
			expected:    true,
		},
		{
			name: "case 6",
			PeriodSpec:  ScheduledPausePeriod{StartClock: "06:00:00+08:00", EndClock: "10:00:00+08:00"},
			CurrentTime: time.Date(2019, 1, 15, 18, 0, 0, 0, newYorkLoc),
			expected:    true,
		},

	}

	for _, c := range cases {
		r.Equal(c.expected, IsInScheduledPausePeriod(c.PeriodSpec, c.CurrentTime), c.name)
	}
}
